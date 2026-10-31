//! REST API handlers.

use arneb_protocol::agent::truncate_on_char_boundary;
use arneb_scheduler::QueryState;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json, Response};
use axum::routing::get;
use axum::{Extension, Router};
use serde::Serialize;

use super::auth::{AuthVia, Identity};
use super::error::{ApiError, ErrorCode};
use super::privilege::Layer;
use super::WebState;

/// JSON response for a query.
#[derive(Serialize)]
pub(super) struct QueryResponse {
    query_id: String,
    state: String,
    pub(super) sql: String,
    error: Option<String>,
    /// Protocol that submitted it (`pgwire`, `trino`).
    source: String,
    user: Option<String>,
    /// Milliseconds since the Unix epoch.
    pub(super) created_ms: u64,
    /// Milliseconds since creation, until now or the end.
    pub(super) elapsed_ms: u64,
    rows: Option<u64>,
}

/// Milliseconds since the Unix epoch (0 before it).
fn epoch_ms(t: std::time::SystemTime) -> u64 {
    t.duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Milliseconds `q` has run: from creation until its end, or until now.
fn elapsed_ms(q: &arneb_scheduler::QueryInfo) -> u64 {
    q.ended
        .unwrap_or_else(std::time::SystemTime::now)
        .duration_since(q.created)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

impl From<arneb_scheduler::QueryInfo> for QueryResponse {
    fn from(q: arneb_scheduler::QueryInfo) -> Self {
        let elapsed_ms = elapsed_ms(&q);
        Self {
            query_id: q.query_id.to_string(),
            state: format!("{:?}", q.state),
            sql: q.sql,
            error: q.error,
            source: q.source,
            user: q.user,
            created_ms: epoch_ms(q.created),
            elapsed_ms,
            rows: q.rows,
        }
    }
}

/// JSON response for the query list.
#[derive(Serialize)]
pub(super) struct QueriesResponse {
    queries: Vec<QueryResponse>,
}

/// Query parameters for listing queries.
#[derive(serde::Deserialize, Default)]
pub struct ListQueryParams {
    pub(super) state: Option<String>,
}

/// Most queries `list_queries` returns when `limit` is omitted.
pub(super) const DEFAULT_QUERY_LIMIT: usize = 50;
/// The largest accepted `limit`.
pub(super) const MAX_QUERY_LIMIT: usize = 500;

/// A tracked query with its `sql` cut to the cell budget and its `state`
/// in lowercase, the spelling the `state` filter and the statement API use.
#[derive(Serialize)]
pub(super) struct BoundedQuery {
    #[serde(flatten)]
    query: QueryResponse,
    /// True when `sql` was cut at the cell budget.
    sql_truncated: bool,
}

impl BoundedQuery {
    /// `info` with its `sql` cut to at most `max_cell_bytes` bytes, on a
    /// char boundary.
    pub(super) fn new(info: arneb_scheduler::QueryInfo, max_cell_bytes: usize) -> Self {
        let mut query = QueryResponse::from(info);
        query.state.make_ascii_lowercase();
        let sql_truncated = truncate_on_char_boundary(&mut query.sql, max_cell_bytes);
        Self {
            query,
            sql_truncated,
        }
    }
}

/// Every [`QueryState`] by the lowercase name the bounded list reports it
/// under, in lifecycle order.
const QUERY_STATES: [(&str, QueryState); 8] = [
    ("queued", QueryState::Queued),
    ("planning", QueryState::Planning),
    ("starting", QueryState::Starting),
    ("running", QueryState::Running),
    ("finishing", QueryState::Finishing),
    ("finished", QueryState::Finished),
    ("failed", QueryState::Failed),
    ("cancelled", QueryState::Cancelled),
];

/// The lowercase name of `state`, as the bounded list reports it.
pub(super) fn state_name(state: QueryState) -> &'static str {
    QUERY_STATES
        .iter()
        .find(|(_, s)| *s == state)
        .map_or("unknown", |(name, _)| name)
}

/// The [`QueryState`] named `s` (any case). A name selects exactly that
/// state: `running` does not include `planning`, `starting` or
/// `finishing`. An unknown name is `invalid_argument` listing all eight.
pub(super) fn parse_query_state(s: &str) -> Result<QueryState, ApiError> {
    QUERY_STATES
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(s.trim()))
        .map(|(_, state)| *state)
        .ok_or_else(|| {
            let names: Vec<&str> = QUERY_STATES.iter().map(|(n, _)| *n).collect();
            ApiError::new(ErrorCode::InvalidArgument, format!("unknown state {s:?}"))
                .with_hint(format!("state is one of {}", names.join(", ")))
        })
}

/// The bounded query list: newest first, with whether more matched.
#[derive(Serialize)]
pub(super) struct BoundedQueries {
    queries: Vec<BoundedQuery>,
    /// True when more queries matched than `limit` allowed.
    truncated: bool,
}

/// Filters of the bounded query list.
#[derive(Default)]
pub(super) struct BoundedParams {
    pub(super) state: Option<String>,
    pub(super) user: Option<String>,
    pub(super) min_elapsed_ms: Option<u64>,
    pub(super) limit: Option<usize>,
}

/// The queries `who` may see, newest first, at most `limit` (default 50,
/// at most 500) of those matching `state` (an unknown one is refused),
/// `user` and `min_elapsed_ms`. Same view rules as [`queries_response`].
pub(super) fn bounded_queries(
    state: &WebState,
    who: &Identity,
    params: BoundedParams,
    max_cell_bytes: usize,
) -> Result<BoundedQueries, ApiError> {
    let limit = params.limit.unwrap_or(DEFAULT_QUERY_LIMIT);
    if limit == 0 || limit > MAX_QUERY_LIMIT {
        return Err(ApiError::new(
            ErrorCode::InvalidArgument,
            format!("limit {limit} is not valid"),
        )
        .with_hint(format!("use a limit between 1 and {MAX_QUERY_LIMIT}")));
    }
    let state_filter = params.state.as_deref().map(parse_query_state).transpose()?;
    // Filter, sort and cut on the tracker's records; only the survivors
    // are converted.
    let mut matched: Vec<arneb_scheduler::QueryInfo> = state
        .query_tracker
        .list_queries(state_filter)
        .into_iter()
        .filter(|q| may_view(state, who, q.user.as_deref()))
        .filter(|q| {
            params
                .user
                .as_deref()
                .is_none_or(|u| q.user.as_deref() == Some(u))
        })
        .filter(|q| params.min_elapsed_ms.is_none_or(|m| elapsed_ms(q) >= m))
        .collect();
    // Newest first by the reported millisecond; the id breaks ties of the
    // same millisecond so the order (and the cut at `limit`) is stable
    // across calls. Uuid order is the order of its hyphenated text.
    matched.sort_by(|a, b| {
        epoch_ms(b.created)
            .cmp(&epoch_ms(a.created))
            .then_with(|| a.query_id.0.cmp(&b.query_id.0))
    });
    let truncated = matched.len() > limit;
    matched.truncate(limit);
    let queries = matched
        .into_iter()
        .map(|q| BoundedQuery::new(q, max_cell_bytes))
        .collect();
    Ok(BoundedQueries { queries, truncated })
}

/// JSON response for cluster overview.
#[derive(Serialize)]
pub(super) struct ClusterResponse {
    worker_count: usize,
    role: String,
    /// Admission counters (admission-control D5); `max_running_queries`
    /// 0 means unlimited.
    running_queries: usize,
    queued_queries: usize,
    max_running_queries: usize,
    max_queued_queries: usize,
}

/// JSON response for a worker.
#[derive(Serialize)]
pub(super) struct WorkerResponse {
    worker_id: String,
    address: String,
    alive: bool,
    max_splits: usize,
    last_heartbeat_secs_ago: u64,
    version: String,
    /// Lifecycle, reported by the worker: `active` or `draining`.
    state: &'static str,
    /// Coordinator-side: `schedulable`, `cordoned`, `draining` or `drained`.
    scheduling: &'static str,
}

/// JSON response for server info.
#[derive(Serialize)]
pub(super) struct InfoResponse {
    version: String,
    uptime_secs: u64,
    role: String,
}

pub fn api_routes() -> Router<WebState> {
    Router::new()
        .route("/queries", get(list_queries))
        .route("/queries/{id}", get(get_query).delete(cancel_query))
        .route("/cluster", get(cluster_overview))
        .route("/cluster/workers", get(list_workers))
        .route("/info", get(server_info))
}

/// The query rules apply to authenticated callers only: under `[auth] type
/// = "none"` the API stays as open as the Web UI always was. With no rules
/// file (or no `queries` section) everything is allowed.
fn may_view(state: &WebState, who: &Identity, owner: Option<&str>) -> bool {
    who.via == AuthVia::Unauthenticated
        || state
            .access_rules
            .as_ref()
            .is_none_or(|r| r.can_view_query(&who.user, owner.unwrap_or_default()))
}

async fn list_queries(
    State(state): State<WebState>,
    Extension(who): Extension<Identity>,
    Query(params): Query<ListQueryParams>,
) -> Json<QueriesResponse> {
    Json(queries_response(&state, &who, params))
}

/// The queries `who` may see, filtered by `?state=` (an unknown state
/// filters nothing). Shared with `/api/v2`.
pub(super) fn queries_response(
    state: &WebState,
    who: &Identity,
    params: ListQueryParams,
) -> QueriesResponse {
    let state_filter = params.state.and_then(|s| parse_query_state(&s).ok());

    let queries = state
        .query_tracker
        .list_queries(state_filter)
        .into_iter()
        .filter(|q| may_view(state, who, q.user.as_deref()))
        .map(QueryResponse::from)
        .collect();

    QueriesResponse { queries }
}

/// The query `id` if `who` may see it. A hidden query is `404` exactly
/// like a missing one (the v1 empty body), never `403`.
pub(super) fn visible_query(
    state: &WebState,
    who: &Identity,
    id: &str,
) -> Result<arneb_scheduler::QueryInfo, StatusCode> {
    lookup_query(state, id, |q| may_view(state, who, q.user.as_deref()))
}

/// The query `id` for a cancel by `who` (manage-api D2): as
/// [`visible_query`], except that a query `who` may kill (as a manager, or
/// by a `queries` rule allowing `kill` without `view`) is found too, so a
/// kill grant is never hidden behind `not_found`.
pub(super) fn killable_query(
    state: &WebState,
    who: &Identity,
    id: &str,
) -> Result<arneb_scheduler::QueryInfo, StatusCode> {
    lookup_query(state, id, |q| {
        may_view(state, who, q.user.as_deref())
            || state.authorizer.check_kill(who, q.user.as_deref()).is_ok()
    })
}

fn lookup_query(
    state: &WebState,
    id: &str,
    visible: impl FnOnce(&arneb_scheduler::QueryInfo) -> bool,
) -> Result<arneb_scheduler::QueryInfo, StatusCode> {
    let uuid: uuid::Uuid = id.parse().map_err(|_| StatusCode::BAD_REQUEST)?;
    let query_id = arneb_common::identifiers::QueryId(uuid);
    state
        .query_tracker
        .get_query(&query_id)
        .filter(visible)
        .ok_or(StatusCode::NOT_FOUND)
}

async fn get_query(
    State(state): State<WebState>,
    Extension(who): Extension<Identity>,
    Path(id): Path<String>,
) -> Result<Json<QueryResponse>, StatusCode> {
    let info = visible_query(&state, &who, &id)?;
    Ok(Json(QueryResponse::from(info)))
}

/// `DELETE /api/v1/queries/{id}`: `204`, or an empty `400` / `404`.
/// Under `[auth] type = "none"` any caller may cancel any query, as the
/// Web UI always could (ruling R1). Under password auth it is the
/// `/api/v2` cancel (the D2 kill rule, audited), mapped back to `204`, an
/// empty `400` / `404`, or the error envelope (`403`, `503`).
async fn cancel_query(
    State(state): State<WebState>,
    Extension(who): Extension<Identity>,
    Path(id): Path<String>,
) -> Response {
    if who.via == AuthVia::Unauthenticated {
        let info = match visible_query(&state, &who, &id) {
            Ok(info) => info,
            Err(status) => return status.into_response(),
        };
        return match state.query_tracker.kill_query(&info.query_id, &who.user) {
            Ok(_) => StatusCode::NO_CONTENT.into_response(),
            Err(_) => StatusCode::NOT_FOUND.into_response(),
        };
    }
    match super::v2::cancel_query_for(&state, &who, &id, false).await {
        Ok(_) => StatusCode::NO_CONTENT.into_response(),
        Err(e) if e.code == ErrorCode::InvalidArgument => StatusCode::BAD_REQUEST.into_response(),
        Err(e) if e.code == ErrorCode::NotFound => StatusCode::NOT_FOUND.into_response(),
        Err(e) => e.into_response(),
    }
}

/// Admission counters for the overview: the controller's, or with none
/// (admission unlimited) zero counters, `max_running` 0 (unlimited) and the
/// configured `max_queued_queries`.
fn admission_counters(
    admission: Option<&arneb_scheduler::AdmissionController>,
    configured_max_queued: usize,
) -> arneb_scheduler::AdmissionSnapshot {
    admission
        .map(|a| a.snapshot())
        .unwrap_or(arneb_scheduler::AdmissionSnapshot {
            running: 0,
            queued: 0,
            max_running: 0,
            max_queued: configured_max_queued,
        })
}

async fn cluster_overview(
    State(state): State<WebState>,
    Extension(who): Extension<Identity>,
) -> Result<Json<ClusterResponse>, ApiError> {
    state.authorizer.check(&who, Layer::Observe)?;
    Ok(Json(cluster_response(&state)))
}

/// The cluster overview. Shared with `/api/v2`.
pub(super) fn cluster_response(state: &WebState) -> ClusterResponse {
    let admission = admission_counters(state.admission.as_ref(), state.max_queued_queries);
    ClusterResponse {
        worker_count: state.node_registry.alive_count(),
        role: state.role.clone(),
        running_queries: admission.running,
        queued_queries: admission.queued,
        max_running_queries: admission.max_running,
        max_queued_queries: admission.max_queued,
    }
}

async fn list_workers(
    State(state): State<WebState>,
    Extension(who): Extension<Identity>,
) -> Result<Json<Vec<WorkerResponse>>, ApiError> {
    state.authorizer.check(&who, Layer::Observe)?;
    Ok(Json(workers_response(&state)))
}

/// The alive workers. Shared with `/api/v2`.
pub(super) fn workers_response(state: &WebState) -> Vec<WorkerResponse> {
    state
        .node_registry
        .alive_workers()
        .into_iter()
        .map(|w| WorkerResponse {
            scheduling: state.node_registry.scheduling_state(&w.worker_id).as_str(),
            worker_id: w.worker_id,
            address: w.address,
            alive: w.alive,
            max_splits: w.max_splits,
            last_heartbeat_secs_ago: w.last_heartbeat.elapsed().as_secs(),
            version: w.version,
            state: if w.draining { "draining" } else { "active" },
        })
        .collect()
}

async fn server_info(State(state): State<WebState>) -> Json<InfoResponse> {
    Json(info_response(&state))
}

/// Server version, uptime and role. Shared with `/api/v2`.
pub(super) fn info_response(state: &WebState) -> InfoResponse {
    InfoResponse {
        version: env!("CARGO_PKG_VERSION").to_string(),
        uptime_secs: state.start_time.elapsed().as_secs(),
        role: state.role.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// #217 review: every state the bounded list reports parses back to
    /// itself, and nothing else does.
    #[test]
    fn every_reported_state_parses() {
        for (name, state) in QUERY_STATES {
            assert_eq!(format!("{state:?}").to_ascii_lowercase(), name);
            assert_eq!(parse_query_state(name).unwrap(), state);
            assert_eq!(parse_query_state(&name.to_uppercase()).unwrap(), state);
        }
        let e = parse_query_state("bogus").unwrap_err();
        let hint = e.hint.clone().unwrap_or_default();
        for (name, _) in QUERY_STATES {
            assert!(hint.contains(name), "{hint}");
        }
    }

    /// #150 review: with no controller the overview reports the configured
    /// `max_queued_queries`, not 0; with one, the controller's limits.
    #[test]
    fn admission_counters_report_the_configured_queue_limit() {
        let none = admission_counters(None, 256);
        assert_eq!((none.max_running, none.max_queued), (0, 256));
        assert_eq!((none.running, none.queued), (0, 0));

        let controller = arneb_scheduler::AdmissionController::new(4, 32);
        let some = admission_counters(Some(&controller), 256);
        assert_eq!((some.max_running, some.max_queued), (4, 32));
    }
}
