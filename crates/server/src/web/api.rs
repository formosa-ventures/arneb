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
        .map(|q| QueryResponse {
            query_id: q.query_id.to_string(),
            state: format!("{:?}", q.state),
            sql: q.sql,
            error: q.error,
        })
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

    Ok(Json(QueryResponse {
        query_id: info.query_id.to_string(),
        state: format!("{:?}", info.state),
        sql: info.sql,
        error: info.error,
    }))
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

async fn cluster_overview(State(state): State<WebState>) -> Json<ClusterResponse> {
    Json(ClusterResponse {
        worker_count: state.node_registry.alive_count(),
        role: state.role.clone(),
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
