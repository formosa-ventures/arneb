//! Workers on `/api/v2` (manage-api D8): the bounded worker listing, one
//! worker, and cordon / drain / uncordon as manage actions.
//!
//! A cordon is coordinator-side scheduler state, separate from the
//! lifecycle `state` a worker reports for its SIGTERM drain-then-exit. It is
//! persisted in the state store before it is applied to the node registry,
//! and restored at startup by [`restore_worker_cordons`].

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use arneb_scheduler::{CordonMode, NodeRegistry, WorkerInfo};
use arneb_state::{now_ms, CordonKind, CordonRecord, StateStore};
use axum::extract::rejection::QueryRejection;
use axum::extract::{Path, Query, State};
use axum::response::Json;
use axum::routing::{get, post};
use axum::{Extension, Router};
use serde::{Deserialize, Serialize};

use super::auth::Identity;
use super::error::{ApiError, ErrorCode};
use super::manage::{
    run_manage, Applied, ChangeLock, DryRun, ManageAction, ManageOutcome, Preview,
};
use super::privilege::Layer;
use super::v2::bad_query;
use super::WebState;

/// Workers listed when the request names no `limit`.
pub(super) const DEFAULT_WORKER_LIMIT: usize = 100;
/// The largest accepted `limit`.
pub(super) const MAX_WORKER_LIMIT: usize = 1000;

/// A worker scheduling change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum WorkerOp {
    Cordon,
    Drain,
    Uncordon,
}

impl WorkerOp {
    fn action(self) -> &'static str {
        match self {
            WorkerOp::Cordon => "worker.cordon",
            WorkerOp::Drain => "worker.drain",
            WorkerOp::Uncordon => "worker.uncordon",
        }
    }

    /// The cordon this op sets; `None` for uncordon.
    fn mode(self) -> Option<CordonMode> {
        match self {
            WorkerOp::Cordon => Some(CordonMode::Cordon),
            WorkerOp::Drain => Some(CordonMode::Drain),
            WorkerOp::Uncordon => None,
        }
    }
}

/// One worker: registered, or only known by a stored cordon.
#[derive(Debug, Serialize)]
pub(super) struct WorkerView {
    worker_id: String,
    /// The last advertised Flight address; `None` when not registered.
    address: Option<String>,
    /// Whether the registry knows the id (alive or not).
    registered: bool,
    alive: bool,
    version: Option<String>,
    /// Lifecycle, reported by the worker: `active` or `draining` (its
    /// SIGTERM drain-then-exit); `unknown` when not registered.
    state: &'static str,
    /// Coordinator-side: `schedulable`, `cordoned`, `draining` or `drained`.
    scheduling: &'static str,
    /// Distributed queries the coordinator runs on it.
    running_queries: usize,
    cordoned_since_ms: Option<u64>,
    cordoned_by: Option<String>,
    last_heartbeat_secs_ago: Option<u64>,
}

/// A bounded page of workers, sorted by id.
#[derive(Debug, Serialize)]
pub(super) struct WorkerList {
    workers: Vec<WorkerView>,
    truncated: bool,
}

fn kind_of(mode: CordonMode) -> CordonKind {
    match mode {
        CordonMode::Cordon => CordonKind::Cordon,
        CordonMode::Drain => CordonKind::Drain,
    }
}

fn mode_of(kind: CordonKind) -> CordonMode {
    match kind {
        CordonKind::Cordon => CordonMode::Cordon,
        CordonKind::Drain => CordonMode::Drain,
    }
}

/// Restores `store`'s cordons into `registry` (at startup, before the Flight
/// server accepts heartbeats); returns how many there are.
pub fn restore_worker_cordons(store: &StateStore, registry: &NodeRegistry) -> usize {
    let cordons = store.cordons();
    let count = cordons.len();
    registry.restore_cordons(cordons.into_iter().map(|c| (c.worker_id, mode_of(c.kind))));
    count
}

/// The stored cordons by worker id (none without a state store).
fn stored_cordons(state: &WebState) -> HashMap<String, CordonRecord> {
    state
        .state
        .as_ref()
        .map(|s| s.cordons())
        .unwrap_or_default()
        .into_iter()
        .map(|c| (c.worker_id.clone(), c))
        .collect()
}

/// The stored cordon on `id`, if any.
fn stored_cordon(state: &WebState, id: &str) -> Option<CordonRecord> {
    state.state.as_ref().and_then(|s| s.cordon(id))
}

fn view(
    registry: &NodeRegistry,
    id: &str,
    info: Option<&WorkerInfo>,
    record: Option<&CordonRecord>,
) -> WorkerView {
    WorkerView {
        worker_id: id.to_owned(),
        address: info.map(|w| w.address.clone()),
        registered: info.is_some(),
        alive: info.is_some_and(|w| w.alive),
        version: info.map(|w| w.version.clone()),
        state: match info {
            Some(w) if w.draining => "draining",
            Some(_) => "active",
            None => "unknown",
        },
        scheduling: registry.scheduling_state(id).as_str(),
        running_queries: registry.in_flight(id),
        cordoned_since_ms: record.map(|r| r.since_ms),
        cordoned_by: record.map(|r| r.by.clone()),
        last_heartbeat_secs_ago: info.map(|w| w.last_heartbeat.elapsed().as_secs()),
    }
}

/// Worker `id` as listed, or `None` when it is neither registered nor
/// cordoned.
fn find_worker(state: &WebState, id: &str) -> Option<WorkerView> {
    let registry = &state.node_registry;
    let info = registry.worker(id);
    let record = stored_cordon(state, id);
    if info.is_none() && record.is_none() && registry.cordon_of(id).is_none() {
        return None;
    }
    Some(view(registry, id, info.as_ref(), record.as_ref()))
}

fn no_such_worker(id: &str) -> ApiError {
    ApiError::new(ErrorCode::NotFound, format!("no worker {id:?} is known"))
        .with_hint("GET /api/v2/workers lists the registered and cordoned workers")
}

/// Every registered or cordoned worker, sorted by id, at most `limit`
/// (default [`DEFAULT_WORKER_LIMIT`], at most [`MAX_WORKER_LIMIT`]). Needs
/// observe. Shared with the MCP `list_workers` tool.
pub(super) fn list_workers_for(
    state: &WebState,
    who: &Identity,
    limit: Option<usize>,
) -> Result<WorkerList, ApiError> {
    state.authorizer.check(who, Layer::Observe)?;
    let limit = limit.unwrap_or(DEFAULT_WORKER_LIMIT);
    if limit == 0 || limit > MAX_WORKER_LIMIT {
        return Err(ApiError::new(
            ErrorCode::InvalidArgument,
            format!("limit {limit} is not valid"),
        )
        .with_hint(format!("use a limit between 1 and {MAX_WORKER_LIMIT}")));
    }
    let registry = &state.node_registry;
    let mut infos: HashMap<String, WorkerInfo> = registry
        .all_workers()
        .into_iter()
        .map(|w| (w.worker_id.clone(), w))
        .collect();
    let records = stored_cordons(state);
    let ids: BTreeSet<String> = infos
        .keys()
        .cloned()
        .chain(registry.cordoned_ids())
        .chain(records.keys().cloned())
        .collect();
    let truncated = ids.len() > limit;
    let workers = ids
        .into_iter()
        .take(limit)
        .map(|id| {
            let info = infos.remove(&id);
            view(registry, &id, info.as_ref(), records.get(&id))
        })
        .collect();
    Ok(WorkerList { workers, truncated })
}

/// Worker `id`, registered or cordoned; else `not_found`. Needs observe.
/// Shared with the MCP `get_worker` tool.
pub(super) fn get_worker_for(
    state: &WebState,
    who: &Identity,
    id: &str,
) -> Result<WorkerView, ApiError> {
    state.authorizer.check(who, Layer::Observe)?;
    find_worker(state, id).ok_or_else(|| no_such_worker(id))
}

/// Cordons, drains or uncordons worker `id` for `who`, who needs manage
/// (also for a dry run). Cordon and drain need a registered id; uncordon
/// takes any id with a cordon. Shared with the MCP worker tools.
pub(super) async fn worker_action_for(
    state: &WebState,
    who: &Identity,
    id: &str,
    op: WorkerOp,
    dry_run: bool,
) -> Result<ManageOutcome<WorkerView>, ApiError> {
    let grant = state.authorizer.check(who, Layer::Manage)?;
    let store = state
        .state
        .clone()
        .ok_or_else(|| ApiError::new(ErrorCode::Internal, "worker cordons need the state store"))?;
    let registry = &state.node_registry;
    let known = match op {
        WorkerOp::Cordon | WorkerOp::Drain => registry.is_known(id),
        WorkerOp::Uncordon => registry.is_known(id) || has_cordon(registry, &store, id),
    };
    if !known {
        return Err(no_such_worker(id));
    }
    // Held across the whole change, audit append included, so the audit
    // order is the apply order (as for admission changes). The action keeps
    // a share, so a request dropped mid-call still serializes its apply.
    let serialized = Arc::new(Arc::clone(&state.cordon_changes).lock_owned().await);
    let action = WorkerAction {
        id: id.to_owned(),
        op,
        by: who.user.clone(),
        store,
        _serialized: Arc::clone(&serialized),
    };
    let outcome = run_manage(state, who, grant, dry_run, action).await;
    drop(serialized);
    outcome
}

fn has_cordon(registry: &NodeRegistry, store: &StateStore, id: &str) -> bool {
    registry.cordon_of(id).is_some() || store.cordon(id).is_some()
}

struct WorkerAction {
    id: String,
    op: WorkerOp,
    by: String,
    store: Arc<StateStore>,
    /// A share of [`WebState::cordon_changes`], taken in
    /// [`worker_action_for`].
    _serialized: ChangeLock,
}

/// `n` with the singular or plural noun.
fn count(n: usize, one: &str, many: &str) -> String {
    format!("{n} {}", if n == 1 { one } else { many })
}

const LAST_WORKER_WARNING: &str = "no schedulable worker is left: new distributed queries fail \
     until a worker is uncordoned with POST /api/v2/workers/{id}/uncordon";

impl WorkerAction {
    /// What running the op now does.
    fn plan(&self, state: &WebState) -> Preview {
        let registry = &state.node_registry;
        let id = &self.id;
        let current = registry.cordon_of(id);
        let noop = |what: &str| Preview {
            summary: format!("worker {id} is {what}; nothing to change"),
            changed: false,
            warning: None,
        };
        let left = || {
            registry
                .active_workers()
                .iter()
                .filter(|w| &w.worker_id != id)
                .count()
        };
        let warning = |left: usize| (left == 0).then(|| LAST_WORKER_WARNING.to_owned());
        match self.op {
            WorkerOp::Cordon if current == Some(CordonMode::Cordon) => noop("already cordoned"),
            WorkerOp::Drain if current == Some(CordonMode::Drain) => noop("already draining"),
            WorkerOp::Uncordon if !has_cordon(registry, &self.store, id) => noop("not cordoned"),
            WorkerOp::Cordon => {
                let left = left();
                Preview {
                    summary: format!(
                        "cordons worker {id}: it gets no new queries ({} left)",
                        count(left, "schedulable worker", "schedulable workers")
                    ),
                    changed: true,
                    warning: warning(left),
                }
            }
            WorkerOp::Drain => Preview {
                summary: format!(
                    "drains worker {id}: no new queries; {} on it",
                    count(
                        registry.in_flight(id),
                        "running query finishes",
                        "running queries finish"
                    )
                ),
                changed: true,
                warning: warning(left()),
            },
            WorkerOp::Uncordon => Preview {
                summary: format!("uncordons worker {id}: it gets new queries again"),
                changed: true,
                warning: None,
            },
        }
    }
}

fn persist_failed(e: arneb_state::StateError) -> ApiError {
    tracing::error!(error = %e, "cannot persist a worker cordon");
    ApiError::new(
        ErrorCode::Internal,
        "the cordon could not be saved to the state store; nothing changed",
    )
}

impl ManageAction for WorkerAction {
    type Output = WorkerView;

    fn action(&self) -> &'static str {
        self.op.action()
    }

    fn target(&self) -> String {
        self.id.clone()
    }

    fn parameters(&self) -> serde_json::Value {
        serde_json::json!({})
    }

    fn persisted(&self) -> bool {
        true
    }

    fn preview(&self, state: &WebState) -> Result<Preview, ApiError> {
        Ok(self.plan(state))
    }

    fn change_lock(&self) -> Option<ChangeLock> {
        Some(Arc::clone(&self._serialized))
    }

    /// Persists first, then updates the registry, so a failed write
    /// changes nothing. Runs under [`WebState::cordon_changes`], so
    /// concurrent calls cannot leave the store and the registry
    /// disagreeing.
    fn apply(self, state: &WebState) -> Result<Applied<WorkerView>, ApiError> {
        let preview = self.plan(state);
        if preview.changed {
            match self.op.mode() {
                Some(mode) => self
                    .store
                    .set_cordon(CordonRecord {
                        worker_id: self.id.clone(),
                        kind: kind_of(mode),
                        since_ms: now_ms(),
                        by: self.by.clone(),
                    })
                    .map_err(persist_failed)?,
                None => {
                    self.store.clear_cordon(&self.id).map_err(persist_failed)?;
                }
            }
            #[cfg(test)]
            tests::pause_after_persist(&self.id, self.op);
            state.node_registry.set_cordon(&self.id, self.op.mode());
        }
        let result = find_worker(state, &self.id).unwrap_or_else(|| {
            // An uncordoned id that never registered: no longer listed.
            view(&state.node_registry, &self.id, None, None)
        });
        Ok(Applied { preview, result })
    }
}

/// `GET /api/v2/workers` query parameters.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ListParams {
    limit: Option<usize>,
}

async fn list(
    State(state): State<WebState>,
    Extension(who): Extension<Identity>,
    params: Result<Query<ListParams>, QueryRejection>,
) -> Result<Json<WorkerList>, ApiError> {
    let Query(params) = params.map_err(bad_query)?;
    Ok(Json(list_workers_for(&state, &who, params.limit)?))
}

async fn get_one(
    State(state): State<WebState>,
    Extension(who): Extension<Identity>,
    Path(id): Path<String>,
) -> Result<Json<WorkerView>, ApiError> {
    Ok(Json(get_worker_for(&state, &who, &id)?))
}

async fn act(
    state: WebState,
    who: Identity,
    id: String,
    params: Result<Query<DryRun>, QueryRejection>,
    op: WorkerOp,
) -> Result<Json<ManageOutcome<WorkerView>>, ApiError> {
    let Query(DryRun { dry_run }) = params.map_err(bad_query)?;
    Ok(Json(
        worker_action_for(&state, &who, &id, op, dry_run).await?,
    ))
}

async fn cordon(
    State(state): State<WebState>,
    Extension(who): Extension<Identity>,
    Path(id): Path<String>,
    params: Result<Query<DryRun>, QueryRejection>,
) -> Result<Json<ManageOutcome<WorkerView>>, ApiError> {
    act(state, who, id, params, WorkerOp::Cordon).await
}

async fn drain(
    State(state): State<WebState>,
    Extension(who): Extension<Identity>,
    Path(id): Path<String>,
    params: Result<Query<DryRun>, QueryRejection>,
) -> Result<Json<ManageOutcome<WorkerView>>, ApiError> {
    act(state, who, id, params, WorkerOp::Drain).await
}

async fn uncordon(
    State(state): State<WebState>,
    Extension(who): Extension<Identity>,
    Path(id): Path<String>,
    params: Result<Query<DryRun>, QueryRejection>,
) -> Result<Json<ManageOutcome<WorkerView>>, ApiError> {
    act(state, who, id, params, WorkerOp::Uncordon).await
}

/// The worker routes, merged into `/api/v2`.
pub(super) fn routes() -> Router<WebState> {
    Router::new()
        .route("/workers", get(list))
        .route("/workers/{id}", get(get_one))
        .route("/workers/{id}/cordon", post(cordon))
        .route("/workers/{id}/drain", post(drain))
        .route("/workers/{id}/uncordon", post(uncordon))
}

#[cfg(test)]
mod tests {
    use axum::http::{Method, StatusCode};
    use serde_json::{json, Value};

    use super::*;
    use crate::web::v2::tests::{basic, call, web_state, with_authz};

    /// Password auth, `root` the one manager, w1 and w2 heartbeating.
    fn cluster(path: Option<&std::path::Path>) -> WebState {
        let state = with_authz(web_state(true, path), None, &["root"]);
        state
            .node_registry
            .heartbeat("w1".into(), "http://127.0.0.1:1".into(), 256);
        state
            .node_registry
            .heartbeat("w2".into(), "http://127.0.0.1:2".into(), 256);
        state
    }

    fn root() -> String {
        basic("root", "rootpw")
    }

    fn alice() -> String {
        basic("alice", "secret")
    }

    async fn post(state: &WebState, path: &str, auth: Option<&str>) -> (StatusCode, Value) {
        call(state, Method::POST, path, auth, None).await
    }

    async fn get(state: &WebState, path: &str) -> (StatusCode, Value) {
        call(state, Method::GET, path, Some(&root()), None).await
    }

    fn audit_actions(state: &WebState) -> Vec<(String, String, String)> {
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
            .map(|e| (e.record.action, e.record.user, e.record.target))
            .collect()
    }

    fn store(state: &WebState) -> &StateStore {
        state.state.as_deref().unwrap()
    }

    #[tokio::test]
    async fn manager_cordons_and_uncordons() {
        let state = cluster(None);
        let (status, body) = post(&state, "/api/v2/workers/w1/cordon", Some(&root())).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["dry_run"], false);
        assert_eq!(body["action"], "worker.cordon");
        assert_eq!(body["target"], "w1");
        assert_eq!(body["changed"], true);
        assert_eq!(
            body["summary"],
            "cordons worker w1: it gets no new queries (1 schedulable worker left)"
        );
        assert_eq!(body["result"]["scheduling"], "cordoned");
        assert_eq!(body["result"]["state"], "active");
        assert_eq!(body["result"]["cordoned_by"], "root");

        let (status, one) = get(&state, "/api/v2/workers/w1").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(one["scheduling"], "cordoned");
        assert_eq!(one["registered"], true);
        let stored = store(&state).cordons();
        assert_eq!(stored.len(), 1);
        assert_eq!(
            (stored[0].worker_id.as_str(), stored[0].kind),
            ("w1", CordonKind::Cordon)
        );
        assert_eq!(
            audit_actions(&state),
            vec![("worker.cordon".into(), "root".into(), "w1".into())]
        );

        let (_, again) = post(&state, "/api/v2/workers/w1/cordon", Some(&root())).await;
        assert_eq!(again["changed"], false, "{again}");
        assert!(
            again["summary"]
                .as_str()
                .unwrap()
                .contains("already cordoned"),
            "{again}"
        );

        let (status, body) = post(&state, "/api/v2/workers/w1/uncordon", Some(&root())).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["changed"], true);
        assert_eq!(
            body["summary"],
            "uncordons worker w1: it gets new queries again"
        );
        assert_eq!(body["result"]["scheduling"], "schedulable");
        assert!(store(&state).cordons().is_empty());
        let (_, again) = post(&state, "/api/v2/workers/w1/uncordon", Some(&root())).await;
        assert_eq!(again["changed"], false);
        assert!(again["summary"].as_str().unwrap().contains("not cordoned"));
        let actions: Vec<String> = audit_actions(&state).into_iter().map(|a| a.0).collect();
        assert_eq!(
            actions,
            [
                "worker.uncordon",
                "worker.uncordon",
                "worker.cordon",
                "worker.cordon"
            ]
        );
    }

    #[tokio::test]
    async fn drain_reports_draining_then_drained() {
        let state = cluster(None);
        let (_, guard) = state.node_registry.schedule_workers();
        let (status, body) = post(&state, "/api/v2/workers/w1/drain", Some(&root())).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(
            body["summary"],
            "drains worker w1: no new queries; 1 running query finishes on it"
        );
        assert_eq!(body["result"]["scheduling"], "draining");
        assert_eq!(body["result"]["running_queries"], 1);
        assert_eq!(store(&state).cordons()[0].kind, CordonKind::Drain);
        drop(guard);
        let (_, one) = get(&state, "/api/v2/workers/w1").await;
        assert_eq!(one["scheduling"], "drained");
        assert_eq!(one["running_queries"], 0);
        let (_, again) = post(&state, "/api/v2/workers/w1/drain", Some(&root())).await;
        assert_eq!(again["changed"], false);
        assert!(again["summary"]
            .as_str()
            .unwrap()
            .contains("already draining"));
    }

    #[tokio::test]
    async fn non_manager_is_refused_including_dry_run() {
        let state = cluster(None);
        let (status, real) = post(&state, "/api/v2/workers/w1/cordon", Some(&alice())).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert_eq!(real["error"]["code"], "forbidden");
        assert!(
            real["error"]["hint"]
                .as_str()
                .unwrap()
                .contains("admin_users"),
            "{real}"
        );
        let (status, dry) = post(
            &state,
            "/api/v2/workers/w1/cordon?dry_run=true",
            Some(&alice()),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert_eq!(dry, real);
        assert_eq!(
            state.node_registry.scheduling_state("w1").as_str(),
            "schedulable"
        );
        assert!(store(&state).cordons().is_empty());
        assert!(audit_actions(&state).is_empty());

        let open = web_state(false, None);
        open.node_registry
            .heartbeat("w1".into(), "http://127.0.0.1:1".into(), 256);
        for path in [
            "/api/v2/workers/w1/cordon",
            "/api/v2/workers/w1/cordon?dry_run=true",
        ] {
            let (status, body) = post(&open, path, None).await;
            assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
            assert_eq!(body["error"]["code"], "manage_requires_auth");
        }
        assert_eq!(
            open.node_registry.scheduling_state("w1").as_str(),
            "schedulable"
        );
    }

    #[tokio::test]
    async fn dry_run_changes_nothing() {
        let state = cluster(None);
        let (status, body) = post(
            &state,
            "/api/v2/workers/w1/drain?dry_run=true",
            Some(&root()),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["dry_run"], true);
        assert_eq!(body["changed"], true);
        assert!(
            body["summary"]
                .as_str()
                .unwrap()
                .starts_with("drains worker w1"),
            "{body}"
        );
        assert!(body.get("result").is_none(), "{body}");
        assert_eq!(
            state.node_registry.scheduling_state("w1").as_str(),
            "schedulable"
        );
        assert!(store(&state).cordons().is_empty());
        assert!(audit_actions(&state).is_empty());

        let (status, body) = post(
            &state,
            "/api/v2/workers/w1/drain?dry_run=maybe",
            Some(&root()),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["code"], "invalid_argument");
    }

    #[tokio::test]
    async fn unknown_worker_is_not_found() {
        let state = cluster(None);
        for op in ["cordon", "drain", "uncordon"] {
            let (status, body) =
                post(&state, &format!("/api/v2/workers/nope/{op}"), Some(&root())).await;
            assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
            assert_eq!(body["error"]["code"], "not_found");
            assert!(
                body["error"]["hint"]
                    .as_str()
                    .unwrap()
                    .contains("GET /api/v2/workers"),
                "{body}"
            );
        }
        let (status, body) = get(&state, "/api/v2/workers/nope").await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
        assert!(audit_actions(&state).is_empty());
    }

    /// Review Focus 1: cordoning the last schedulable worker succeeds with a
    /// warning; the next distributed query names the cordons and the
    /// uncordon route; uncordoning lets queries be scheduled again.
    #[tokio::test]
    async fn cordoning_the_last_worker_warns_and_queries_say_why() {
        use arneb_common::types::{ColumnInfo, DataType, TableReference};
        let tmp = tempfile::tempdir().unwrap();
        let state = cluster(Some(&tmp.path().join("state.json")));
        let (_, first) = post(&state, "/api/v2/workers/w1/cordon", Some(&root())).await;
        assert_eq!(first["warning"], Value::Null, "{first}");
        let (status, last) = post(&state, "/api/v2/workers/w2/drain", Some(&root())).await;
        assert_eq!(status, StatusCode::OK, "{last}");
        assert_eq!(last["changed"], true);
        assert!(
            last["warning"]
                .as_str()
                .unwrap()
                .contains("no schedulable worker"),
            "{last}"
        );

        let coordinator = crate::coordinator::QueryCoordinator::new(
            state.node_registry.clone(),
            arneb_scheduler::DynamicFilterServiceRegistry::new(),
        );
        let scan = arneb_planner::LogicalPlan::TableScan {
            table: TableReference::table("t"),
            schema: vec![ColumnInfo {
                name: "c0".into(),
                data_type: DataType::Int64,
                nullable: false,
            }],
            alias: None,
            properties: Default::default(),
            dynamic_filters_consumed: Vec::new(),
        };
        let ctx = arneb_execution::ExecutionContext::new();
        let err = coordinator
            .execute(scan.clone(), &ctx)
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("no workers available for distributed execution"),
            "{err}"
        );
        assert!(err.contains("2 cordoned"), "{err}");
        assert!(err.contains("POST /api/v2/workers/{id}/uncordon"), "{err}");

        let (_, back) = post(&state, "/api/v2/workers/w1/uncordon", Some(&root())).await;
        assert_eq!(back["changed"], true);
        assert_eq!(back["warning"], Value::Null, "{back}");
        // w1 is scheduled again: the query now reaches it (and fails on its
        // fake address instead).
        let (picked, _guard) = state.node_registry.schedule_workers();
        let picked: Vec<String> = picked.into_iter().map(|w| w.worker_id).collect();
        assert_eq!(picked, vec!["w1".to_string()]);
        let err = coordinator
            .execute(scan, &ctx)
            .await
            .unwrap_err()
            .to_string();
        assert!(!err.contains("no workers available"), "{err}");
    }

    #[tokio::test]
    async fn stale_cordon_is_listed_and_can_be_cleared() {
        let state = cluster(None);
        store(&state)
            .set_cordon(CordonRecord {
                worker_id: "gone".into(),
                kind: CordonKind::Cordon,
                since_ms: 7,
                by: "root".into(),
            })
            .unwrap();
        assert_eq!(
            restore_worker_cordons(store(&state), &state.node_registry),
            1
        );
        let (status, list) = get(&state, "/api/v2/workers").await;
        assert_eq!(status, StatusCode::OK, "{list}");
        let ids: Vec<&str> = list["workers"]
            .as_array()
            .unwrap()
            .iter()
            .map(|w| w["worker_id"].as_str().unwrap())
            .collect();
        assert_eq!(ids, ["gone", "w1", "w2"]);
        assert_eq!(list["truncated"], false);
        let gone = &list["workers"][0];
        assert_eq!(gone["registered"], false);
        assert_eq!(gone["alive"], false);
        assert_eq!(gone["scheduling"], "cordoned");
        assert_eq!(gone["state"], "unknown");
        assert_eq!(gone["address"], Value::Null);
        assert_eq!(gone["cordoned_since_ms"], 7);
        assert_eq!(gone["cordoned_by"], "root");

        // Cordon needs a registered id; uncordon takes a stored one.
        let (status, _) = post(&state, "/api/v2/workers/gone/drain", Some(&root())).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        let (status, body) = post(&state, "/api/v2/workers/gone/uncordon", Some(&root())).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["changed"], true);
        assert!(store(&state).cordons().is_empty());
        assert!(state.node_registry.cordoned_ids().is_empty());
        let (status, body) = post(&state, "/api/v2/workers/gone/uncordon", Some(&root())).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
        assert_eq!(body["error"]["code"], "not_found");
    }

    #[tokio::test]
    async fn cordons_survive_a_restart() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("state.json");
        let state = cluster(Some(&path));
        let (status, body) = post(&state, "/api/v2/workers/w1/cordon", Some(&root())).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        drop(state);

        let state = with_authz(web_state(true, Some(&path)), None, &["root"]);
        assert_eq!(
            restore_worker_cordons(store(&state), &state.node_registry),
            1
        );
        let (_, before) = get(&state, "/api/v2/workers/w1").await;
        assert_eq!(before["registered"], false, "{before}");
        assert_eq!(before["scheduling"], "cordoned");
        assert_eq!(before["cordoned_by"], "root");

        state
            .node_registry
            .heartbeat("w1".into(), "http://127.0.0.1:1".into(), 256);
        assert!(state.node_registry.active_workers().is_empty());
        let (_, after) = get(&state, "/api/v2/workers/w1").await;
        assert_eq!(after["registered"], true);
        assert_eq!(after["alive"], true);
        assert_eq!(after["scheduling"], "cordoned");
    }

    #[tokio::test]
    async fn worker_listing_is_bounded() {
        let state = with_authz(web_state(true, None), None, &["root"]);
        for n in 0..=MAX_WORKER_LIMIT {
            state
                .node_registry
                .heartbeat(format!("w{n:04}"), "http://127.0.0.1:1".into(), 256);
        }
        let (status, body) = get(&state, "/api/v2/workers?limit=1001").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["code"], "invalid_argument");
        assert!(
            body["error"]["hint"].as_str().unwrap().contains("1000"),
            "{body}"
        );
        let (status, body) = get(&state, "/api/v2/workers?limit=0").await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        let (status, body) = get(&state, "/api/v2/workers?bogus=1").await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");

        let (status, body) = get(&state, "/api/v2/workers").await;
        assert_eq!(status, StatusCode::OK);
        let workers = body["workers"].as_array().unwrap();
        assert_eq!(workers.len(), DEFAULT_WORKER_LIMIT);
        assert_eq!(workers[0]["worker_id"], "w0000");
        assert_eq!(body["truncated"], true);
        let (_, body) = get(&state, "/api/v2/workers?limit=1000").await;
        assert_eq!(body["workers"].as_array().unwrap().len(), 1000);
        assert_eq!(body["truncated"], true);
    }

    #[tokio::test]
    async fn in_memory_store_warns_on_cordon() {
        let state = cluster(None);
        let (_, dry) = post(
            &state,
            "/api/v2/workers/w1/cordon?dry_run=true",
            Some(&root()),
        )
        .await;
        assert!(
            dry["warning"].as_str().unwrap().contains("lost on restart"),
            "{dry}"
        );
        let (_, body) = post(&state, "/api/v2/workers/w1/cordon", Some(&root())).await;
        assert!(
            body["warning"]
                .as_str()
                .unwrap()
                .contains("lost on restart"),
            "{body}"
        );
        // A no-op changes nothing, so nothing is lost.
        let (_, body) = post(&state, "/api/v2/workers/w1/cordon", Some(&root())).await;
        assert_eq!(body["warning"], Value::Null, "{body}");

        let tmp = tempfile::tempdir().unwrap();
        let on_disk = cluster(Some(&tmp.path().join("state.json")));
        let (_, body) = post(&on_disk, "/api/v2/workers/w1/cordon", Some(&root())).await;
        assert_eq!(body["warning"], Value::Null, "{body}");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_failed_persist_changes_nothing() {
        use std::os::unix::fs::PermissionsExt;
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("sub");
        std::fs::create_dir(&dir).unwrap();
        let state = cluster(Some(&dir.join("state.json")));
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o500)).unwrap();
        let (status, body) = post(&state, "/api/v2/workers/w1/cordon", Some(&root())).await;
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{body}");
        assert_eq!(body["error"]["code"], "internal");
        assert_eq!(
            state.node_registry.scheduling_state("w1").as_str(),
            "schedulable"
        );
        assert!(store(&state).cordons().is_empty());
        let audited = state.audit.query(&arneb_state::AuditQuery {
            limit: 10,
            before_seq: None,
            user: None,
            action: None,
        });
        assert_eq!(
            serde_json::to_value(&audited.entries[0].record.outcome).unwrap(),
            json!({"error": "internal"})
        );
    }

    /// The worker id whose cordon `apply` stops between persisting and
    /// updating the registry, until the test releases it.
    static PAUSED_ID: &str = "race-w";

    /// The race test's handshake: `apply` sends on the first sender once it
    /// is inside the change, then waits for the second channel.
    type Gate = (std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>);
    static PAUSE_GATE: std::sync::Mutex<Option<Gate>> = std::sync::Mutex::new(None);

    pub(super) fn pause_after_persist(id: &str, op: WorkerOp) {
        if id != PAUSED_ID || op != WorkerOp::Cordon {
            return;
        }
        let gate = PAUSE_GATE.lock().unwrap().take();
        if let Some((inside, release)) = gate {
            inside.send(()).unwrap();
            release.recv().unwrap();
        }
    }

    /// Concurrent calls on one id leave the store and the registry agreeing,
    /// and audit in the order they applied. A cordon stops after its
    /// persist; an uncordon started meanwhile must wait for the whole cordon
    /// (audit append included), or it would clear the store, the cordon
    /// would then land in the registry alone, and the log could read
    /// uncordon before cordon.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_changes_keep_store_and_registry_in_step() {
        let state = cluster(None);
        state
            .node_registry
            .heartbeat(PAUSED_ID.into(), "http://127.0.0.1:3".into(), 256);
        let store = state.state.clone().unwrap();
        let root_id = Identity {
            user: "root".into(),
            via: crate::web::auth::AuthVia::Basic,
        };
        let (inside_tx, inside_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        *PAUSE_GATE.lock().unwrap() = Some((inside_tx, release_rx));

        let cordon = tokio::spawn({
            let (state, who) = (state.clone(), root_id.clone());
            async move { worker_action_for(&state, &who, PAUSED_ID, WorkerOp::Cordon, false).await }
        });
        tokio::task::spawn_blocking(move || inside_rx.recv().unwrap())
            .await
            .unwrap();
        // The cordon is persisted and holds the change lock.
        assert!(store.cordon(PAUSED_ID).is_some());

        let mut uncordon = std::pin::pin!(worker_action_for(
            &state,
            &root_id,
            PAUSED_ID,
            WorkerOp::Uncordon,
            false
        ));
        assert!(
            futures::poll!(uncordon.as_mut()).is_pending(),
            "the uncordon waits for the cordon"
        );
        // While it waits, it has neither touched the store nor audited.
        assert!(store.cordon(PAUSED_ID).is_some());
        assert!(audit_actions(&state).is_empty());

        release_tx.send(()).unwrap();
        assert!(cordon.await.unwrap().unwrap().changed);
        assert!(
            uncordon.await.unwrap().changed,
            "uncordon ran after the cordon"
        );
        assert_eq!(store.cordon(PAUSED_ID), None);
        assert_eq!(state.node_registry.cordon_of(PAUSED_ID), None);
        // Newest first: the uncordon was applied and audited last.
        let actions: Vec<String> = audit_actions(&state).into_iter().map(|a| a.0).collect();
        assert_eq!(actions, ["worker.uncordon", "worker.cordon"]);
    }

    /// A request dropped while its cordon runs still holds the change lock
    /// until the audit entry is appended, so no later change can apply (and
    /// audit) in between.
    #[tokio::test]
    async fn dropped_request_holds_the_lock_until_audited() {
        let state = cluster(None);
        let (inside, release) =
            crate::web::manage::test_hooks::stop_after_apply(&state.cordon_changes);
        let call = tokio::spawn({
            let state = state.clone();
            async move { post(&state, "/api/v2/workers/w1/cordon", Some(&root())).await }
        });
        tokio::task::spawn_blocking(move || inside.recv().unwrap())
            .await
            .unwrap();
        // Applied, not yet audited; now the client goes away.
        call.abort();
        assert!(call.await.unwrap_err().is_cancelled());
        assert!(
            state.cordon_changes.try_lock().is_err(),
            "the lock is held until the append"
        );
        assert!(audit_actions(&state).is_empty());
        release.send(()).unwrap();
        let _next = state.cordon_changes.lock().await;
        assert_eq!(audit_actions(&state)[0].0, "worker.cordon");
    }

    #[tokio::test]
    async fn cluster_workers_report_scheduling() {
        let state = cluster(None);
        state
            .node_registry
            .set_cordon("w1", Some(CordonMode::Cordon));
        for path in ["/api/v1/cluster/workers", "/api/v2/cluster/workers"] {
            let (status, body) = get(&state, path).await;
            assert_eq!(status, StatusCode::OK, "{body}");
            let mut workers = body.as_array().unwrap().clone();
            workers.sort_by_key(|w| w["worker_id"].as_str().unwrap().to_owned());
            assert_eq!(workers.len(), 2);
            for w in &workers {
                let keys: BTreeSet<&str> =
                    w.as_object().unwrap().keys().map(String::as_str).collect();
                assert_eq!(
                    keys,
                    BTreeSet::from([
                        "worker_id",
                        "address",
                        "alive",
                        "max_splits",
                        "last_heartbeat_secs_ago",
                        "version",
                        "state",
                        "scheduling",
                    ]),
                    "{w}"
                );
                assert_eq!(w["state"], "active");
            }
            assert_eq!(workers[0]["scheduling"], "cordoned");
            assert_eq!(workers[1]["scheduling"], "schedulable");
        }
    }
}
