//! Admission limits on `/api/v2` (manage-api D9): read the limits and
//! counters, and change the limits at runtime as a manage action.
//!
//! A change is persisted in the state store (`settings.admission`) before it
//! is applied to the controller, and a stored value wins over the config
//! seed at the next start (`config::resolve_admission_limits`).

use std::sync::Arc;

use arneb_scheduler::{AdmissionController, AdmissionSnapshot};
use arneb_state::{AdmissionLimits, StateStore};
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
    run_manage, Applied, ChangeLock, DryRun, ManageAction, ManageOutcome, Preview,
};
use super::privilege::Layer;
use super::v2::{bad_body, bad_query, parse_body};
use super::WebState;

/// The admission limits and counters.
#[derive(Debug, Serialize)]
pub(super) struct AdmissionView {
    /// `0` means unlimited.
    max_running_queries: usize,
    max_queued_queries: usize,
    running_queries: usize,
    queued_queries: usize,
    /// `config` (the `[admission]` seed) or `state` (set at runtime and
    /// stored in the state store).
    source: &'static str,
}

/// The `PUT /api/v2/admission` body, or the `set_admission_limits` tool's
/// arguments. The values are kept untyped so that a negative, fractional,
/// non-numeric or `null` value gets the range hint.
#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub(super) struct AdmissionUpdate {
    #[serde(default, deserialize_with = "present")]
    pub max_running_queries: Option<serde_json::Value>,
    #[serde(default, deserialize_with = "present")]
    pub max_queued_queries: Option<serde_json::Value>,
}

/// A field that is present is `Some`, also when it is `null`, so that
/// `null` is refused instead of read as "keep the value".
fn present<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<serde_json::Value>, D::Error> {
    serde_json::Value::deserialize(d).map(Some)
}

/// The largest accepted limit, as in the config (a TOML integer is an i64).
const MAX_LIMIT: u64 = i64::MAX as u64;

fn controller(state: &WebState) -> Result<&AdmissionController, ApiError> {
    state.admission.as_ref().ok_or_else(|| {
        ApiError::new(
            ErrorCode::NotFound,
            "this node runs no admission control (it serves no SQL)",
        )
    })
}

fn source(store: Option<&StateStore>) -> &'static str {
    match store.and_then(|s| s.admission_limits()) {
        Some(_) => "state",
        None => "config",
    }
}

fn view(snapshot: AdmissionSnapshot, source: &'static str) -> AdmissionView {
    AdmissionView {
        max_running_queries: snapshot.max_running,
        max_queued_queries: snapshot.max_queued,
        running_queries: snapshot.running,
        queued_queries: snapshot.queued,
        source,
    }
}

/// The admission limits and counters. Needs observe. Shared with the MCP
/// `get_admission` tool.
pub(super) fn get_admission_for(
    state: &WebState,
    who: &Identity,
) -> Result<AdmissionView, ApiError> {
    state.authorizer.check(who, Layer::Observe)?;
    let controller = controller(state)?;
    Ok(view(controller.snapshot(), source(state.state.as_deref())))
}

/// One knob of an [`AdmissionUpdate`], validated like the config: a
/// non-negative integer.
fn knob(name: &str, value: Option<serde_json::Value>) -> Result<Option<usize>, ApiError> {
    let Some(value) = value else {
        return Ok(None);
    };
    match value
        .as_u64()
        .filter(|v| *v <= MAX_LIMIT)
        .and_then(|v| usize::try_from(v).ok())
    {
        Some(v) => Ok(Some(v)),
        None => Err(ApiError::new(
            ErrorCode::InvalidArgument,
            format!("{name} {value} is not valid"),
        )
        .with_hint(format!(
            "{name} must be a non-negative integer of at most {MAX_LIMIT} \
             (0 = unlimited for max_running_queries)"
        ))),
    }
}

/// Changes the admission limits for `who`, who needs manage (also for a dry
/// run). A knob left out keeps its value. Shared with the MCP
/// `set_admission_limits` tool.
pub(super) async fn set_admission_for(
    state: &WebState,
    who: &Identity,
    req: AdmissionUpdate,
    dry_run: bool,
) -> Result<ManageOutcome<AdmissionView>, ApiError> {
    let grant = state.authorizer.check(who, Layer::Manage)?;
    let max_running = knob("max_running_queries", req.max_running_queries)?;
    let max_queued = knob("max_queued_queries", req.max_queued_queries)?;
    if max_running.is_none() && max_queued.is_none() {
        return Err(
            ApiError::new(ErrorCode::InvalidArgument, "nothing to change")
                .with_hint("send max_running_queries and/or max_queued_queries"),
        );
    }
    let controller = controller(state)?.clone();
    let store = state.state.clone().ok_or_else(|| {
        ApiError::new(ErrorCode::Internal, "admission limits need the state store")
    })?;
    // Held from reading `from` until the change (and its audit entry) is
    // done, so preview, audit and apply see one state. The action keeps a
    // share, so a request dropped mid-call still serializes its apply.
    let serialized = Arc::new(Arc::clone(&state.admission_changes).lock_owned().await);
    let action = SetAdmission {
        max_running,
        max_queued,
        from: controller.snapshot(),
        controller,
        store,
        _serialized: Arc::clone(&serialized),
    };
    let outcome = run_manage(state, who, grant, dry_run, action).await;
    drop(serialized);
    outcome
}

struct SetAdmission {
    max_running: Option<usize>,
    max_queued: Option<usize>,
    /// The controller's limits when the action was built, under
    /// [`WebState::admission_changes`]; the audit `parameters` name them as
    /// `from`.
    from: AdmissionSnapshot,
    controller: AdmissionController,
    store: Arc<StateStore>,
    _serialized: ChangeLock,
}

/// `n` with the singular or plural noun.
fn count(n: usize, one: &str, many: &str) -> String {
    format!("{n} {}", if n == 1 { one } else { many })
}

impl SetAdmission {
    /// The limits after the change, given the current ones.
    fn target_limits(&self, now: AdmissionSnapshot) -> AdmissionLimits {
        AdmissionLimits {
            max_running_queries: self.max_running.unwrap_or(now.max_running),
            max_queued_queries: self.max_queued.unwrap_or(now.max_queued),
        }
    }

    /// What applying the change now does.
    fn plan(&self) -> (Preview, AdmissionLimits) {
        let now = self.controller.snapshot();
        let to = self.target_limits(now);
        let mut changes = Vec::new();
        if to.max_running_queries != now.max_running {
            changes.push(format!(
                "max_running_queries {} → {}",
                now.max_running, to.max_running_queries
            ));
        }
        if to.max_queued_queries != now.max_queued {
            changes.push(format!(
                "max_queued_queries {} → {}",
                now.max_queued, to.max_queued_queries
            ));
        }
        let counters = format!(
            "{} running, {} queued",
            count(now.running, "query", "queries"),
            now.queued
        );
        if changes.is_empty() {
            return (
                Preview {
                    summary: format!(
                        "admission limits are already max_running_queries {}, \
                         max_queued_queries {}; nothing to change ({counters})",
                        now.max_running, now.max_queued
                    ),
                    changed: false,
                    warning: None,
                },
                to,
            );
        }
        let mut summary = format!("sets {} ({counters})", changes.join(", "));
        let cap = to.max_running_queries;
        if cap != 0 && now.running >= cap {
            summary.push_str(&format!(
                ": running queries finish; new queries queue until fewer than {cap} run"
            ));
        } else if now.queued > 0 && (cap == 0 || cap > now.max_running) {
            summary.push_str(": queued queries are admitted up to the new cap");
        }
        (
            Preview {
                summary,
                changed: true,
                warning: None,
            },
            to,
        )
    }
}

fn persist_failed(e: arneb_state::StateError) -> ApiError {
    tracing::error!(error = %e, "cannot persist the admission limits");
    ApiError::new(
        ErrorCode::Internal,
        "the admission limits could not be saved to the state store; nothing changed",
    )
}

impl ManageAction for SetAdmission {
    type Output = AdmissionView;

    fn action(&self) -> &'static str {
        "admission.set"
    }

    fn target(&self) -> String {
        "admission".to_owned()
    }

    fn parameters(&self) -> serde_json::Value {
        let mut map = serde_json::Map::new();
        if let Some(to) = self.max_running {
            map.insert(
                "max_running_queries".into(),
                serde_json::json!({"from": self.from.max_running, "to": to}),
            );
        }
        if let Some(to) = self.max_queued {
            map.insert(
                "max_queued_queries".into(),
                serde_json::json!({"from": self.from.max_queued, "to": to}),
            );
        }
        serde_json::Value::Object(map)
    }

    fn persisted(&self) -> bool {
        true
    }

    fn preview(&self, _state: &WebState) -> Result<Preview, ApiError> {
        Ok(self.plan().0)
    }

    fn change_lock(&self) -> Option<ChangeLock> {
        Some(Arc::clone(&self._serialized))
    }

    /// Persists first, then updates the controller, so a failed write
    /// changes nothing. Runs under [`WebState::admission_changes`], so
    /// concurrent calls cannot leave the store and the controller
    /// disagreeing.
    fn apply(self, _state: &WebState) -> Result<Applied<AdmissionView>, ApiError> {
        let (preview, to) = self.plan();
        let snapshot = if preview.changed {
            self.store
                .set_admission_limits(to)
                .map_err(persist_failed)?;
            let snapshot = self
                .controller
                .set_limits(to.max_running_queries, to.max_queued_queries);
            tracing::info!(
                target: "arneb::admission",
                max_running_queries = to.max_running_queries,
                max_queued_queries = to.max_queued_queries,
                running = snapshot.running,
                queued = snapshot.queued,
                "admission limits changed at runtime"
            );
            snapshot
        } else {
            self.controller.snapshot()
        };
        let result = view(snapshot, source(Some(&self.store)));
        Ok(Applied { preview, result })
    }
}

async fn get_admission(
    State(state): State<WebState>,
    Extension(who): Extension<Identity>,
) -> Result<Json<AdmissionView>, ApiError> {
    Ok(Json(get_admission_for(&state, &who)?))
}

async fn put_admission(
    State(state): State<WebState>,
    Extension(who): Extension<Identity>,
    params: Result<Query<DryRun>, QueryRejection>,
    body: Result<Bytes, BytesRejection>,
) -> Result<Json<ManageOutcome<AdmissionView>>, ApiError> {
    let Query(DryRun { dry_run }) = params.map_err(bad_query)?;
    let req: AdmissionUpdate = parse_body(&body.map_err(bad_body)?, true)?;
    Ok(Json(set_admission_for(&state, &who, req, dry_run).await?))
}

/// The admission routes, merged into `/api/v2`.
pub(super) fn routes() -> Router<WebState> {
    Router::new().route("/admission", get(get_admission).put(put_admission))
}

#[cfg(test)]
mod tests {
    use axum::http::{Method, StatusCode};
    use serde_json::{json, Value};

    use super::*;
    use crate::config::{resolve_admission_limits, AdmissionConfig};
    use crate::web::v2::tests::{basic, call, web_state, with_authz};

    /// Password auth, `root` the one manager, a controller at `(8, 256)`.
    fn node(path: Option<&std::path::Path>) -> WebState {
        let mut state = with_authz(web_state(true, path), None, &["root"]);
        state.admission = Some(AdmissionController::new(8, 256));
        state
    }

    fn root() -> String {
        basic("root", "rootpw")
    }

    fn alice() -> String {
        basic("alice", "secret")
    }

    async fn put(state: &WebState, path: &str, auth: &str, body: Value) -> (StatusCode, Value) {
        call(state, Method::PUT, path, Some(auth), Some(body)).await
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

    fn snapshot(state: &WebState) -> AdmissionSnapshot {
        state.admission.as_ref().unwrap().snapshot()
    }

    #[tokio::test]
    async fn get_reports_limits_and_counters() {
        let state = node(None);
        let _running = state.admission.as_ref().unwrap().admit("q").unwrap();
        let (status, body) = call(
            &state,
            Method::GET,
            "/api/v2/admission",
            Some(&alice()),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(
            body,
            json!({
                "max_running_queries": 8,
                "max_queued_queries": 256,
                "running_queries": 1,
                "queued_queries": 0,
                "source": "config",
            })
        );
    }

    #[tokio::test]
    async fn put_is_partial_persisted_and_audited() {
        let state = node(None);
        let (status, body) = put(
            &state,
            "/api/v2/admission",
            &root(),
            json!({"max_running_queries": 2}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["action"], "admission.set");
        assert_eq!(body["changed"], true);
        assert_eq!(body["result"]["max_running_queries"], 2);
        assert_eq!(body["result"]["max_queued_queries"], 256);
        assert_eq!(body["result"]["source"], "state");
        // The state store is in memory here.
        assert!(
            body["warning"]
                .as_str()
                .unwrap()
                .contains("lost on restart"),
            "{body}"
        );
        assert_eq!(
            state.state.as_ref().unwrap().admission_limits(),
            Some(AdmissionLimits {
                max_running_queries: 2,
                max_queued_queries: 256,
            })
        );
        let snap = snapshot(&state);
        assert_eq!((snap.max_running, snap.max_queued), (2, 256));
        let entries = audit_entries(&state);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].action, "admission.set");
        assert_eq!(entries[0].user, "root");
        assert_eq!(
            entries[0].parameters,
            json!({
                "max_running_queries": {"from": 8, "to": 2},
                "effect": "applied",
            })
        );

        // The same value again is a no-op.
        let (status, again) = put(
            &state,
            "/api/v2/admission",
            &root(),
            json!({"max_running_queries": 2}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{again}");
        assert_eq!(again["changed"], false, "{again}");
        assert!(
            again["summary"]
                .as_str()
                .unwrap()
                .contains("nothing to change"),
            "{again}"
        );
    }

    #[tokio::test]
    async fn put_validation() {
        let state = node(None);
        let (status, body) = put(&state, "/api/v2/admission", &root(), json!({})).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(body["error"]["code"], "invalid_argument");
        assert_eq!(
            body["error"]["hint"],
            "send max_running_queries and/or max_queued_queries"
        );

        let too_big = json!(i64::MAX as u64 + 1);
        for bad in [json!(-1), json!(1.5), json!("x"), Value::Null, too_big] {
            let (status, body) = put(
                &state,
                "/api/v2/admission",
                &root(),
                json!({"max_running_queries": bad}),
            )
            .await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
            assert_eq!(body["error"]["code"], "invalid_argument");
            assert!(
                body["error"]["hint"]
                    .as_str()
                    .unwrap()
                    .contains("max_running_queries must be a non-negative integer"),
                "{body}"
            );
        }

        let (status, body) = put(&state, "/api/v2/admission", &root(), json!({"bogus": 1})).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(body["error"]["code"], "invalid_argument");
        assert!(
            body["error"]["message"].as_str().unwrap().contains("bogus"),
            "{body}"
        );

        let snap = snapshot(&state);
        assert_eq!((snap.max_running, snap.max_queued), (8, 256));
        assert!(audit_entries(&state).is_empty());
    }

    #[tokio::test]
    async fn non_manager_and_dry_run() {
        let state = node(None);
        let body = json!({"max_running_queries": 2});
        let (status, real) = put(&state, "/api/v2/admission", &alice(), body.clone()).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{real}");
        assert_eq!(real["error"]["code"], "forbidden");
        let (status, dry) = put(
            &state,
            "/api/v2/admission?dry_run=true",
            &alice(),
            body.clone(),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert_eq!(dry, real);

        let _running = state.admission.as_ref().unwrap().admit("q").unwrap();
        let (status, dry) = put(&state, "/api/v2/admission?dry_run=true", &root(), body).await;
        assert_eq!(status, StatusCode::OK, "{dry}");
        assert_eq!(dry["dry_run"], true);
        assert_eq!(dry["changed"], true);
        assert!(dry.get("result").is_none(), "{dry}");
        let summary = dry["summary"].as_str().unwrap();
        assert!(
            summary.starts_with("sets max_running_queries 8 → 2"),
            "{summary}"
        );
        assert!(summary.contains("1 query running"), "{summary}");

        let snap = snapshot(&state);
        assert_eq!((snap.max_running, snap.max_queued), (8, 256));
        assert_eq!(state.state.as_ref().unwrap().admission_limits(), None);
        assert!(audit_entries(&state).is_empty());
    }

    fn audited_running(state: &WebState) -> Vec<Value> {
        audit_entries(state)
            .into_iter()
            .rev()
            .map(|e| e.parameters["max_running_queries"].clone())
            .collect()
    }

    /// A request dropped while its change runs still holds the change lock
    /// until the audit entry is appended.
    #[tokio::test]
    async fn dropped_request_holds_the_lock_until_audited() {
        let state = node(None);
        let (inside, release) =
            crate::web::manage::test_hooks::stop_after_apply(&state.admission_changes);
        let call = tokio::spawn({
            let state = state.clone();
            async move {
                put(
                    &state,
                    "/api/v2/admission",
                    &root(),
                    json!({"max_running_queries": 3}),
                )
                .await
            }
        });
        tokio::task::spawn_blocking(move || inside.recv().unwrap())
            .await
            .unwrap();
        call.abort();
        assert!(call.await.unwrap_err().is_cancelled());
        assert!(
            state.admission_changes.try_lock().is_err(),
            "the lock is held until the append"
        );
        assert!(audited_running(&state).is_empty());
        release.send(()).unwrap();
        let _next = state.admission_changes.lock().await;
        assert_eq!(audited_running(&state).len(), 1);
    }

    #[tokio::test]
    async fn sequential_puts_audit_a_chained_from() {
        let state = node(None);
        for to in [2, 5] {
            let (status, body) = put(
                &state,
                "/api/v2/admission",
                &root(),
                json!({"max_running_queries": to}),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{body}");
        }
        assert_eq!(
            audited_running(&state),
            vec![json!({"from": 8, "to": 2}), json!({"from": 2, "to": 5})]
        );
    }

    /// `from` is read only once the change holds the lock: a change that
    /// lands while it waits is what it audits as `from`.
    #[tokio::test]
    async fn from_is_read_under_the_change_lock() {
        let state = node(None);
        let held = state.admission_changes.clone().lock_owned().await;
        let call = tokio::spawn({
            let state = state.clone();
            async move {
                put(
                    &state,
                    "/api/v2/admission",
                    &root(),
                    json!({"max_running_queries": 2}),
                )
                .await
            }
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(!call.is_finished(), "the PUT waits for the lock");
        state.admission.as_ref().unwrap().set_limits(5, 256);
        drop(held);
        let (status, body) = call.await.unwrap();
        assert_eq!(status, StatusCode::OK, "{body}");
        assert!(
            body["summary"]
                .as_str()
                .unwrap()
                .starts_with("sets max_running_queries 5 → 2"),
            "{body}"
        );
        assert_eq!(audited_running(&state), vec![json!({"from": 5, "to": 2})]);
    }

    #[tokio::test]
    async fn restart_restores_runtime_limits() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("state.json");
        let state = node(Some(&path));
        let (status, body) = put(
            &state,
            "/api/v2/admission",
            &root(),
            json!({"max_running_queries": 2}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["warning"], Value::Null, "{body}");
        drop(state);

        let store = StateStore::open(Some(&path)).unwrap();
        let config = AdmissionConfig::default();
        assert_eq!(
            (config.max_running_queries, config.max_queued_queries),
            (8, 256)
        );
        let resolved = resolve_admission_limits(&config, store.admission_limits());
        assert_eq!(
            resolved.limits,
            AdmissionLimits {
                max_running_queries: 2,
                max_queued_queries: 256,
            }
        );
        assert_eq!(resolved.source, "state");
        assert_eq!(resolved.diverging, vec![("max_running_queries", 8, 2)]);
    }
}
