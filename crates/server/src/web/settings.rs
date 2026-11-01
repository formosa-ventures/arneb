//! Runtime settings on `/api/v2/settings` (runtime-config D9): read the
//! slow-query threshold, and change it at runtime as a manage action.
//!
//! A change is persisted in the state store (`settings.slow_query_threshold_ms`)
//! before it is applied through [`arneb_common::metrics::set_slow_query_threshold`],
//! and a stored value wins over `[observability]` at the next start
//! (`config::resolve_slow_query_threshold`). Admission limits stay at
//! `/api/v2/admission`.

use std::sync::Arc;

use arneb_state::StateStore;
use axum::body::Bytes;
use axum::extract::rejection::{BytesRejection, QueryRejection};
use axum::extract::{Query, State};
use axum::response::Json;
use axum::routing::get;
use axum::{Extension, Router};
use serde::{Deserialize, Serialize};

use super::auth::Identity;
use super::error::{ApiError, ErrorCode};
use super::manage::{
    config_change_lock, run_manage, Applied, ChangeLock, DryRun, ManageAction, ManageOutcome,
    Preview,
};
use super::privilege::Layer;
use super::v2::{bad_body, bad_query, parse_body};
use super::WebState;
use crate::config::slow_query_threshold_from_ms;

/// The runtime settings.
#[derive(Debug, Serialize)]
pub(super) struct SettingsView {
    /// Statements running at least this long are logged on
    /// `arneb::slow_query`; `0` means the log is off.
    slow_query_threshold_ms: u64,
    /// `config` (`[observability]`) or `state` (set at runtime and stored
    /// in the state store).
    source: &'static str,
}

/// The `PATCH /api/v2/settings` body. Kept untyped so that a negative,
/// fractional, non-numeric or `null` value gets the range hint.
#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub(super) struct SettingsUpdate {
    #[serde(default, deserialize_with = "present")]
    pub slow_query_threshold_ms: Option<serde_json::Value>,
}

/// A field that is present is `Some`, also when it is `null`.
fn present<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<serde_json::Value>, D::Error> {
    serde_json::Value::deserialize(d).map(Some)
}

/// The largest accepted threshold, as in the config (a TOML integer is an
/// i64).
const MAX_THRESHOLD_MS: u64 = i64::MAX as u64;

/// The effective slow-query threshold in ms (`0` = off).
pub(super) fn current_threshold_ms() -> u64 {
    arneb_common::metrics::slow_query_threshold().map_or(0, |d| d.as_millis() as u64)
}

fn source(store: Option<&StateStore>) -> &'static str {
    match store.and_then(|s| s.slow_query_threshold_ms()) {
        Some(_) => "state",
        None => "config",
    }
}

fn view(store: Option<&StateStore>) -> SettingsView {
    SettingsView {
        slow_query_threshold_ms: current_threshold_ms(),
        source: source(store),
    }
}

/// The runtime settings. Needs observe. Shared with the MCP `get_settings`
/// tool.
pub(super) async fn get_settings_for(
    state: &WebState,
    who: &Identity,
) -> Result<SettingsView, ApiError> {
    state.authorizer.check(who, Layer::Observe)?;
    Ok(view(state.state.as_deref()))
}

/// `slow_query_threshold_ms` of a `PATCH` body, validated like the config:
/// a non-negative integer.
pub(super) fn threshold_arg(value: Option<serde_json::Value>) -> Result<u64, ApiError> {
    let Some(value) = value else {
        return Err(
            ApiError::new(ErrorCode::InvalidArgument, "nothing to change")
                .with_hint("send slow_query_threshold_ms (0 turns the slow-query log off)"),
        );
    };
    value
        .as_u64()
        .filter(|v| *v <= MAX_THRESHOLD_MS)
        .ok_or_else(|| invalid_threshold(&value.to_string()))
}

fn invalid_threshold(value: &str) -> ApiError {
    ApiError::new(
        ErrorCode::InvalidArgument,
        format!("slow_query_threshold_ms {value} is not valid"),
    )
    .with_hint(format!(
        "slow_query_threshold_ms must be a non-negative integer of at most \
         {MAX_THRESHOLD_MS} (0 turns the slow-query log off)"
    ))
}

/// Sets the slow-query threshold to `slow_query_threshold_ms` (`0` = log
/// off) for `who`, who needs manage (also for a dry run). Shared with the
/// MCP `update_settings` tool.
pub(super) async fn update_settings_for(
    state: &WebState,
    who: &Identity,
    dry_run: bool,
    slow_query_threshold_ms: u64,
) -> Result<ManageOutcome<SettingsView>, ApiError> {
    let grant = state.authorizer.check(who, Layer::Manage)?;
    if slow_query_threshold_ms > MAX_THRESHOLD_MS {
        return Err(invalid_threshold(&slow_query_threshold_ms.to_string()));
    }
    let store = state.state.clone().ok_or_else(|| {
        ApiError::new(ErrorCode::Internal, "runtime settings need the state store")
    })?;
    // runtime-config D10: held from reading `from` until the audit entry is
    // appended.
    let serialized = config_change_lock(state, who).await?;
    let action = UpdateSettings {
        from: current_threshold_ms(),
        to: slow_query_threshold_ms,
        store,
        _serialized: Arc::clone(&serialized),
    };
    let outcome = run_manage(state, who, grant, dry_run, action).await;
    drop(serialized);
    outcome
}

struct UpdateSettings {
    /// The effective threshold when the action was built, under
    /// [`WebState::config_changes`].
    from: u64,
    to: u64,
    store: Arc<StateStore>,
    _serialized: ChangeLock,
}

fn describe(ms: u64) -> String {
    if ms == 0 {
        "off".to_owned()
    } else {
        format!("{ms} ms")
    }
}

impl UpdateSettings {
    fn plan(&self) -> Preview {
        let now = current_threshold_ms();
        if now == self.to {
            return Preview {
                summary: format!(
                    "slow_query_threshold_ms is already {} ({}); nothing to change",
                    self.to,
                    describe(self.to)
                ),
                changed: false,
                warning: None,
            };
        }
        let summary = if self.to == 0 {
            format!("sets slow_query_threshold_ms {now} → 0: turns the slow-query log off")
        } else {
            format!(
                "sets slow_query_threshold_ms {now} → {}: statements finishing from now on \
                 are logged on arneb::slow_query when they ran at least {}",
                self.to,
                describe(self.to)
            )
        };
        Preview {
            summary,
            changed: true,
            warning: None,
        }
    }
}

fn persist_failed(e: arneb_state::StateError) -> ApiError {
    tracing::error!(error = %e, "cannot persist the runtime settings");
    ApiError::new(
        ErrorCode::Internal,
        "the settings could not be saved to the state store; nothing changed",
    )
}

impl ManageAction for UpdateSettings {
    type Output = SettingsView;

    fn action(&self) -> &'static str {
        "settings.update"
    }

    fn target(&self) -> String {
        "settings".to_owned()
    }

    fn parameters(&self) -> serde_json::Value {
        serde_json::json!({
            "slow_query_threshold_ms": {"from": self.from, "to": self.to},
        })
    }

    fn persisted(&self) -> bool {
        true
    }

    fn preview(&self, _state: &WebState) -> Result<Preview, ApiError> {
        Ok(self.plan())
    }

    fn change_lock(&self) -> Option<ChangeLock> {
        Some(Arc::clone(&self._serialized))
    }

    /// Persists first, then applies, so a failed write changes nothing.
    fn apply(self, _state: &WebState) -> Result<Applied<SettingsView>, ApiError> {
        let preview = self.plan();
        if preview.changed {
            self.store
                .set_slow_query_threshold_ms(self.to)
                .map_err(persist_failed)?;
            arneb_common::metrics::set_slow_query_threshold(slow_query_threshold_from_ms(self.to));
            tracing::info!(
                target: "arneb::config",
                slow_query_threshold_ms = self.to,
                "slow-query threshold changed at runtime"
            );
        }
        Ok(Applied {
            preview,
            result: view(Some(&self.store)),
        })
    }
}

async fn get_settings(
    State(state): State<WebState>,
    Extension(who): Extension<Identity>,
) -> Result<Json<SettingsView>, ApiError> {
    Ok(Json(get_settings_for(&state, &who).await?))
}

async fn patch_settings(
    State(state): State<WebState>,
    Extension(who): Extension<Identity>,
    params: Result<Query<DryRun>, QueryRejection>,
    body: Result<Bytes, BytesRejection>,
) -> Result<Json<ManageOutcome<SettingsView>>, ApiError> {
    let Query(DryRun { dry_run }) = params.map_err(bad_query)?;
    // Authorization before validation, so a non-manager learns nothing
    // about the body.
    state.authorizer.check(&who, Layer::Manage)?;
    let req: SettingsUpdate = parse_body(&body.map_err(bad_body)?, true)?;
    let ms = threshold_arg(req.slow_query_threshold_ms)?;
    Ok(Json(update_settings_for(&state, &who, dry_run, ms).await?))
}

/// The settings routes, merged into `/api/v2`.
pub(super) fn routes() -> Router<WebState> {
    Router::new().route("/settings", get(get_settings).patch(patch_settings))
}

/// Serializes the tests that set or read the process-wide slow-query
/// threshold.
#[cfg(test)]
pub(super) static THRESHOLD_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[cfg(test)]
mod tests {
    use axum::http::{Method, StatusCode};
    use serde_json::{json, Value};

    use super::*;
    use crate::web::v2::tests::{basic, call, web_state, with_authz};

    fn node(path: Option<&std::path::Path>) -> WebState {
        with_authz(web_state(true, path), None, &["root"])
    }

    async fn patch(state: &WebState, path: &str, auth: &str, body: Value) -> (StatusCode, Value) {
        call(state, Method::PATCH, path, Some(auth), Some(body)).await
    }

    fn audit_entries(state: &WebState) -> Vec<arneb_state::AuditRecord> {
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
            .map(|e| e.record)
            .collect()
    }

    #[tokio::test]
    async fn settings_update_applies_and_persists() {
        let _g = THRESHOLD_TEST_LOCK.lock().await;
        arneb_common::metrics::set_slow_query_threshold(slow_query_threshold_from_ms(10_000));
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("state.json");
        let state = node(Some(&path));
        let root = basic("root", "rootpw");

        // A dry run changes nothing.
        let (status, dry) = patch(
            &state,
            "/api/v2/settings?dry_run=true",
            &root,
            json!({"slow_query_threshold_ms": 250}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{dry}");
        assert_eq!(dry["dry_run"], true);
        assert_eq!(dry["changed"], true);
        assert_eq!(current_threshold_ms(), 10_000);
        assert!(audit_entries(&state).is_empty());

        let (status, body) = patch(
            &state,
            "/api/v2/settings",
            &root,
            json!({"slow_query_threshold_ms": 250}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["action"], "settings.update");
        assert_eq!(
            body["result"],
            json!({"slow_query_threshold_ms": 250, "source": "state"})
        );
        assert_eq!(
            arneb_common::metrics::slow_query_threshold(),
            Some(std::time::Duration::from_millis(250))
        );
        let entries = audit_entries(&state);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].action, "settings.update");
        assert_eq!(
            entries[0].parameters,
            json!({
                "slow_query_threshold_ms": {"from": 10_000, "to": 250},
                "effect": "applied",
            })
        );

        // 0 turns the log off.
        let (status, body) = patch(
            &state,
            "/api/v2/settings",
            &root,
            json!({"slow_query_threshold_ms": 0}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(arneb_common::metrics::slow_query_threshold(), None);
        drop(state);

        let store = StateStore::open(Some(&path)).unwrap();
        assert_eq!(store.slow_query_threshold_ms(), Some(0));
        let resolved = crate::config::resolve_slow_query_threshold(10_000, Some(0));
        assert_eq!(
            (resolved.ms, resolved.source, resolved.diverging),
            (0, "state", Some((10_000, 0)))
        );
        arneb_common::metrics::set_slow_query_threshold(slow_query_threshold_from_ms(10_000));
    }

    #[tokio::test]
    async fn settings_get_is_observe() {
        let _g = THRESHOLD_TEST_LOCK.lock().await;
        arneb_common::metrics::set_slow_query_threshold(slow_query_threshold_from_ms(10_000));
        // alice observes (no rules: everyone observes) but does not manage.
        let state = node(None);
        let (status, body) = call(
            &state,
            Method::GET,
            "/api/v2/settings",
            Some(&basic("alice", "secret")),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(
            body,
            json!({"slow_query_threshold_ms": 10_000, "source": "config"})
        );
        let (status, body) = patch(
            &state,
            "/api/v2/settings",
            &basic("alice", "secret"),
            json!({"slow_query_threshold_ms": 1}),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
        // A user without observe is refused the GET.
        let rules = r#"{"system_information":[{"user":"root","allow":["read","write"]}]}"#;
        let state = with_authz(web_state(true, None), Some(rules), &[]);
        let (status, body) = call(
            &state,
            Method::GET,
            "/api/v2/settings",
            Some(&basic("alice", "secret")),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
        assert_eq!(current_threshold_ms(), 10_000);
    }

    #[tokio::test]
    async fn settings_patch_validation() {
        let state = node(None);
        let root = basic("root", "rootpw");
        let (status, body) = patch(&state, "/api/v2/settings", &root, json!({})).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(body["error"]["message"], "nothing to change");
        let too_big = json!(i64::MAX as u64 + 1);
        for bad in [json!(-1), json!(1.5), json!("x"), Value::Null, too_big] {
            let (status, body) = patch(
                &state,
                "/api/v2/settings",
                &root,
                json!({"slow_query_threshold_ms": bad}),
            )
            .await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
            assert_eq!(body["error"]["code"], "invalid_argument");
        }
        let (status, body) = patch(&state, "/api/v2/settings", &root, json!({"bogus": 1})).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert!(audit_entries(&state).is_empty());
    }
}
