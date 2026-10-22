//! REST API handlers.

use arneb_scheduler::QueryState;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::Json;
use axum::routing::get;
use axum::Router;
use serde::Serialize;

use super::WebState;

/// JSON response for a query.
#[derive(Serialize)]
struct QueryResponse {
    query_id: String,
    state: String,
    sql: String,
    error: Option<String>,
    /// Protocol that submitted it (`pgwire`, `trino`).
    source: String,
    user: Option<String>,
    /// Milliseconds since the Unix epoch.
    created_ms: u64,
    /// Milliseconds since creation, until now or the end.
    elapsed_ms: u64,
    rows: Option<u64>,
}

impl From<arneb_scheduler::QueryInfo> for QueryResponse {
    fn from(q: arneb_scheduler::QueryInfo) -> Self {
        let epoch_ms = |t: std::time::SystemTime| {
            t.duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0)
        };
        let end = q.ended.unwrap_or_else(std::time::SystemTime::now);
        Self {
            query_id: q.query_id.to_string(),
            state: format!("{:?}", q.state),
            sql: q.sql,
            error: q.error,
            source: q.source,
            user: q.user,
            created_ms: epoch_ms(q.created),
            elapsed_ms: end
                .duration_since(q.created)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
            rows: q.rows,
        }
    }
}

/// JSON response for the query list.
#[derive(Serialize)]
struct QueriesResponse {
    queries: Vec<QueryResponse>,
}

/// Query parameters for listing queries.
#[derive(serde::Deserialize, Default)]
pub struct ListQueryParams {
    state: Option<String>,
}

/// JSON response for cluster overview.
#[derive(Serialize)]
struct ClusterResponse {
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
struct WorkerResponse {
    worker_id: String,
    address: String,
    alive: bool,
    max_splits: usize,
    last_heartbeat_secs_ago: u64,
}

/// JSON response for server info.
#[derive(Serialize)]
struct InfoResponse {
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

async fn list_queries(
    State(state): State<WebState>,
    Query(params): Query<ListQueryParams>,
) -> Json<QueriesResponse> {
    let state_filter = params.state.and_then(|s| match s.to_uppercase().as_str() {
        "QUEUED" => Some(QueryState::Queued),
        "RUNNING" => Some(QueryState::Running),
        "FINISHED" => Some(QueryState::Finished),
        "FAILED" => Some(QueryState::Failed),
        "CANCELLED" => Some(QueryState::Cancelled),
        _ => None,
    });

    let queries = state
        .query_tracker
        .list_queries(state_filter)
        .into_iter()
        .map(QueryResponse::from)
        .collect();

    Json(QueriesResponse { queries })
}

async fn get_query(
    State(state): State<WebState>,
    Path(id): Path<String>,
) -> Result<Json<QueryResponse>, StatusCode> {
    let uuid: uuid::Uuid = id.parse().map_err(|_| StatusCode::BAD_REQUEST)?;
    let query_id = arneb_common::identifiers::QueryId(uuid);
    let info = state
        .query_tracker
        .get_query(&query_id)
        .ok_or(StatusCode::NOT_FOUND)?;

    Ok(Json(QueryResponse::from(info)))
}

async fn cancel_query(
    State(state): State<WebState>,
    Path(id): Path<String>,
) -> Result<StatusCode, StatusCode> {
    let uuid: uuid::Uuid = id.parse().map_err(|_| StatusCode::BAD_REQUEST)?;
    let query_id = arneb_common::identifiers::QueryId(uuid);
    state
        .query_tracker
        .cancel_query(&query_id)
        .map_err(|_| StatusCode::NOT_FOUND)?;
    Ok(StatusCode::NO_CONTENT)
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

async fn cluster_overview(State(state): State<WebState>) -> Json<ClusterResponse> {
    let admission = admission_counters(state.admission.as_ref(), state.max_queued_queries);
    Json(ClusterResponse {
        worker_count: state.node_registry.alive_count(),
        role: state.role.clone(),
        running_queries: admission.running,
        queued_queries: admission.queued,
        max_running_queries: admission.max_running,
        max_queued_queries: admission.max_queued,
    })
}

async fn list_workers(State(state): State<WebState>) -> Json<Vec<WorkerResponse>> {
    let workers = state
        .node_registry
        .alive_workers()
        .into_iter()
        .map(|w| WorkerResponse {
            worker_id: w.worker_id,
            address: w.address,
            alive: w.alive,
            max_splits: w.max_splits,
            last_heartbeat_secs_ago: w.last_heartbeat.elapsed().as_secs(),
        })
        .collect();
    Json(workers)
}

async fn server_info(State(state): State<WebState>) -> Json<InfoResponse> {
    Json(InfoResponse {
        version: env!("CARGO_PKG_VERSION").to_string(),
        uptime_secs: state.start_time.elapsed().as_secs(),
        role: state.role.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

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
