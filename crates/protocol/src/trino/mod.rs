//! Trino client REST protocol (v1) server.
//!
//! Lets unmodified Trino clients — the `trino` CLI, Trino JDBC,
//! `trino-python-client`, Superset/Metabase Trino drivers, dbt-trino — talk
//! to an Arneb coordinator or standalone node over HTTP:
//!
//! - `POST /v1/statement` submits SQL and returns the first `QueryResults`
//!   document (state `QUEUED`, with a `nextUri`).
//! - `GET <nextUri>` long-polls (up to `max_wait`) until the query finishes,
//!   then pages through the result set in bounded chunks.
//! - `DELETE <nextUri>` cancels the query.
//! - `GET /v1/info` / `GET /v1/info/state` answer health checks.
//! - `GET /v1/query/{id}` reports basic query info (`infoUri`).
//!
//! Statements run through the same planning/execution path as the
//! PostgreSQL wire protocol ([`crate::handler`]). See
//! `docs/guide/trino-clients.md` for the supported surface.

mod engine;
mod error;
mod metadata;
mod session;
mod statements;
mod types;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures::FutureExt;
use serde_json::{json, Value};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::AbortHandle;

use arneb_catalog::CatalogManager;
use arneb_common::identifiers::QueryId;
use arneb_connectors::ConnectorRegistry;
use arneb_execution::memory_pool::{MemoryPool, UnboundedMemoryPool};
use arneb_scheduler::{QueryState as TrackerState, QueryTracker};

use crate::handler::DistributedExecutor;
use engine::{Engine, QueryOutput};
use error::TrinoError;
use session::{ClientSession, HeaderFamily};
use types::{column_json, encode_cell, materialize};

/// Trino release whose client protocol this server mirrors. Reported in
/// `/v1/info` as `<compat>-arneb-<version>` so clients that parse the leading
/// number of the server version (e.g. JDBC `getDatabaseMajorVersion`) keep
/// working.
const TRINO_COMPAT_VERSION: &str = "476";

/// Configuration of the Trino HTTP listener.
#[derive(Debug, Clone)]
pub struct TrinoConfig {
    /// `host:port` to listen on.
    pub bind_address: String,
    /// A running query (or one with unfetched results) is canceled as
    /// abandoned when the client has not polled it for this long.
    pub client_timeout: Duration,
    /// How long a finished/failed query is kept after the client's last
    /// request, so a retried request for the final page still succeeds.
    pub retention: Duration,
    /// Longest a `GET nextUri` long-poll waits for a running query.
    pub max_wait: Duration,
    /// Approximate encoded-size target of one result page.
    pub target_page_bytes: usize,
    /// Hard cap on rows per result page.
    pub max_page_rows: usize,
}

impl Default for TrinoConfig {
    fn default() -> Self {
        Self {
            bind_address: "127.0.0.1:8080".to_string(),
            client_timeout: Duration::from_secs(300),
            retention: Duration::from_secs(120),
            max_wait: Duration::from_secs(1),
            target_page_bytes: 1024 * 1024,
            max_page_rows: 10_000,
        }
    }
}

/// Trino client-protocol HTTP server.
pub struct TrinoServer {
    config: TrinoConfig,
    catalog_manager: Arc<CatalogManager>,
    connector_registry: Arc<ConnectorRegistry>,
    distributed_executor: Option<Arc<dyn DistributedExecutor>>,
    memory_pool: Arc<dyn MemoryPool>,
    query_tracker: Option<Arc<QueryTracker>>,
}

impl TrinoServer {
    /// Creates a server over the same catalog/connector state the pgwire
    /// server uses.
    pub fn new(
        config: TrinoConfig,
        catalog_manager: Arc<CatalogManager>,
        connector_registry: Arc<ConnectorRegistry>,
    ) -> Self {
        Self {
            config,
            catalog_manager,
            connector_registry,
            distributed_executor: None,
            memory_pool: Arc::new(UnboundedMemoryPool::new()),
            query_tracker: None,
        }
    }

    /// Routes queries through the coordinator's distributed executor.
    pub fn with_distributed_executor(mut self, executor: Arc<dyn DistributedExecutor>) -> Self {
        self.distributed_executor = Some(executor);
        self
    }

    /// Memory pool threaded into every query's execution context.
    pub fn with_memory_pool(mut self, pool: Arc<dyn MemoryPool>) -> Self {
        self.memory_pool = pool;
        self
    }

    /// Registers each query with the tracker so it shows in the Web UI.
    pub fn with_query_tracker(mut self, tracker: Arc<QueryTracker>) -> Self {
        self.query_tracker = Some(tracker);
        self
    }

    fn state(&self) -> AppState {
        AppState {
            engine: Arc::new(Engine {
                catalog_manager: Arc::clone(&self.catalog_manager),
                connector_registry: Arc::clone(&self.connector_registry),
                distributed_executor: self.distributed_executor.clone(),
                memory_pool: Arc::clone(&self.memory_pool),
            }),
            queries: Arc::new(Mutex::new(HashMap::new())),
            config: Arc::new(self.config.clone()),
            tracker: self.query_tracker.clone(),
            started: Instant::now(),
            counter: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Binds `config.bind_address` and serves until the process exits.
    pub async fn start(&self) -> std::io::Result<()> {
        let listener = TcpListener::bind(&self.config.bind_address).await?;
        tracing::info!(
            address = %self.config.bind_address,
            "Trino client protocol listening"
        );
        self.serve(listener).await
    }

    /// Serves on an already-bound listener (used by tests with port 0).
    pub async fn serve(&self, listener: TcpListener) -> std::io::Result<()> {
        let state = self.state();
        tokio::spawn(reaper(state.clone()));
        axum::serve(listener, router(state)).await
    }
}

#[derive(Clone)]
struct AppState {
    engine: Arc<Engine>,
    queries: Arc<Mutex<HashMap<String, Arc<Query>>>>,
    config: Arc<TrinoConfig>,
    tracker: Option<Arc<QueryTracker>>,
    started: Instant,
    counter: Arc<AtomicU64>,
}

fn router(state: AppState) -> Router {
    Router::new()
        .route("/v1/statement", post(submit))
        .route(
            "/v1/statement/executing/{id}/{slug}/{token}",
            get(fetch).delete(cancel),
        )
        .route(
            "/v1/statement/queued/{id}/{slug}/{token}",
            get(fetch).delete(cancel),
        )
        .route("/v1/query/{id}", get(query_info).delete(kill))
        .route("/v1/query", get(list_queries))
        .route("/v1/info", get(info))
        .route("/v1/info/state", get(info_state))
        .route("/v1/status", get(info))
        .fallback(not_found)
        .with_state(state)
}

async fn not_found() -> Response {
    (StatusCode::NOT_FOUND, "Not Found").into_response()
}

// ---------------------------------------------------------------------------
// Query state
// ---------------------------------------------------------------------------

enum Phase {
    Running,
    Finished(Results),
    Failed(TrinoError),
}

struct Results {
    output: QueryOutput,
    batch: usize,
    row: usize,
    rows_returned: u64,
}

struct Inner {
    phase: Phase,
    /// Token the client is expected to request next.
    next_token: u64,
    /// Last served page, for idempotent re-fetch of the same token.
    last: Option<(u64, Value)>,
    last_access: Instant,
    ended: Option<Instant>,
}

struct Query {
    id: String,
    slug: String,
    sql: String,
    user: String,
    source: Option<String>,
    client_info: Option<String>,
    family: HeaderFamily,
    created: Instant,
    inner: Mutex<Inner>,
    done: watch::Sender<bool>,
    abort: Mutex<Option<AbortHandle>>,
    tracker_id: Option<QueryId>,
}

impl Query {
    fn state_name(&self, inner: &Inner) -> &'static str {
        match &inner.phase {
            Phase::Running => "RUNNING",
            Phase::Finished(r) if r.batch < r.output.batches.len() => "RUNNING",
            Phase::Finished(_) => "FINISHED",
            Phase::Failed(_) => "FAILED",
        }
    }
}

fn new_query_id(counter: &AtomicU64) -> String {
    let n = counter.fetch_add(1, Ordering::Relaxed) % 100_000;
    let now = chrono::Utc::now().format("%Y%m%d_%H%M%S");
    let rand = uuid::Uuid::new_v4().simple().to_string();
    format!("{now}_{n:05}_{}", &rand[..5])
}

fn base_uri(headers: &HeaderMap) -> String {
    let host = headers
        .get("X-Forwarded-Host")
        .or_else(|| headers.get("Host"))
        .and_then(|v| v.to_str().ok())
        .unwrap_or("localhost");
    let proto = headers
        .get("X-Forwarded-Proto")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("http");
    format!("{proto}://{host}")
}

fn tracker_transition(state: &AppState, id: Option<QueryId>, to: &[TrackerState]) {
    if let (Some(tracker), Some(id)) = (&state.tracker, id) {
        for s in to {
            let _ = tracker.transition_query(&id, *s);
        }
    }
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

async fn submit(State(state): State<AppState>, headers: HeaderMap, body: Bytes) -> Response {
    let sql = String::from_utf8_lossy(&body).trim().to_string();
    if sql.is_empty() {
        return (StatusCode::BAD_REQUEST, "SQL statement is empty").into_response();
    }
    let session = ClientSession::from_headers(&headers);
    let id = new_query_id(&state.counter);
    let slug = format!("y{}", uuid::Uuid::new_v4().simple());
    let tracker_id = state.tracker.as_ref().map(|t| t.create_query(sql.clone()));
    let (done, _) = watch::channel(false);
    let query = Arc::new(Query {
        id: id.clone(),
        slug,
        sql: sql.clone(),
        user: session.user.clone(),
        source: session.source.clone(),
        client_info: session.client_info.clone(),
        family: session.family,
        created: Instant::now(),
        inner: Mutex::new(Inner {
            phase: Phase::Running,
            next_token: 1,
            last: None,
            last_access: Instant::now(),
            ended: None,
        }),
        done,
        abort: Mutex::new(None),
        tracker_id,
    });
    state
        .queries
        .lock()
        .unwrap()
        .insert(id.clone(), Arc::clone(&query));
    tracing::debug!(query_id = %id, user = %session.user, "trino query submitted");

    let task_state = state.clone();
    let task_query = Arc::clone(&query);
    let handle = tokio::spawn(async move {
        tracker_transition(
            &task_state,
            task_query.tracker_id,
            &[
                TrackerState::Planning,
                TrackerState::Starting,
                TrackerState::Running,
            ],
        );
        let result = std::panic::AssertUnwindSafe(
            task_state.engine.run_statement(&session, &task_query.sql),
        )
        .catch_unwind()
        .await
        .unwrap_or_else(|_| Err(TrinoError::internal("query execution panicked")));
        complete(&task_state, &task_query, result);
    });
    *query.abort.lock().unwrap() = Some(handle.abort_handle());

    let base = base_uri(&headers);
    let body = {
        let inner = query.inner.lock().unwrap();
        let mut doc = results_document(&query, &inner, &base, Some(1), None, None);
        // The submit response always reports QUEUED, as Trino does.
        doc["stats"]["state"] = json!("QUEUED");
        doc["stats"]["queued"] = json!(true);
        doc["stats"]["scheduled"] = json!(false);
        doc
    };
    json_response(StatusCode::OK, body, &[], query.family)
}

fn complete(state: &AppState, query: &Query, result: Result<QueryOutput, TrinoError>) {
    {
        let mut inner = query.inner.lock().unwrap();
        if !matches!(inner.phase, Phase::Running) {
            return;
        }
        inner.ended = Some(Instant::now());
        inner.phase = match result {
            Ok(output) => {
                tracker_transition(
                    state,
                    query.tracker_id,
                    &[TrackerState::Finishing, TrackerState::Finished],
                );
                Phase::Finished(Results {
                    output,
                    batch: 0,
                    row: 0,
                    rows_returned: 0,
                })
            }
            Err(err) => {
                if let (Some(t), Some(id)) = (&state.tracker, query.tracker_id) {
                    let _ = t.fail_query(&id, err.message.clone());
                }
                Phase::Failed(err)
            }
        };
    }
    let _ = query.done.send(true);
}

fn lookup(state: &AppState, id: &str, slug: &str) -> Option<Arc<Query>> {
    let queries = state.queries.lock().unwrap();
    queries.get(id).filter(|q| q.slug == slug).cloned()
}

async fn fetch(
    State(state): State<AppState>,
    Path((id, slug, token)): Path<(String, String, u64)>,
    headers: HeaderMap,
) -> Response {
    let Some(query) = lookup(&state, &id, &slug) else {
        return (StatusCode::NOT_FOUND, "Query not found").into_response();
    };
    let base = base_uri(&headers);

    // Long-poll while the query is still executing.
    let running = {
        let mut inner = query.inner.lock().unwrap();
        inner.last_access = Instant::now();
        matches!(inner.phase, Phase::Running)
    };
    if running {
        let mut rx = query.done.subscribe();
        let _ = tokio::time::timeout(state.config.max_wait, rx.wait_for(|d| *d)).await;
    }

    let mut inner = query.inner.lock().unwrap();
    inner.last_access = Instant::now();
    if let Some((last_token, body)) = &inner.last {
        if *last_token == token {
            let body = body.clone();
            let headers = session_headers(&inner);
            return json_response(StatusCode::OK, body, &headers, query.family);
        }
    }
    if token != inner.next_token {
        return (StatusCode::GONE, "Invalid or expired result token").into_response();
    }
    let page = match &mut inner.phase {
        Phase::Running => {
            // Still executing: same token again, nothing consumed.
            let body = results_document(&query, &inner, &base, Some(token), None, None);
            return json_response(StatusCode::OK, body, &[], query.family);
        }
        Phase::Failed(err) => {
            let err = err.clone();
            let body = results_document(&query, &inner, &base, None, None, Some(&err));
            inner.last = Some((token, body.clone()));
            inner.next_token = token + 1;
            return json_response(StatusCode::OK, body, &[], query.family);
        }
        Phase::Finished(results) => {
            let page = next_page(results, &state.config);
            if results.batch >= results.output.batches.len() {
                // Everything is handed out: release the batches now rather
                // than at expiry; only the cached final page is kept.
                results.output.batches = Vec::new();
                results.batch = 0;
                (page, None)
            } else {
                (page, Some(token + 1))
            }
        }
    };
    let (rows, next) = page;
    let body = results_document(&query, &inner, &base, next, rows, None);
    inner.last = Some((token, body.clone()));
    inner.next_token = token + 1;
    let headers = session_headers(&inner);
    json_response(StatusCode::OK, body, &headers, query.family)
}

fn session_headers(inner: &Inner) -> Vec<(&'static str, String)> {
    match &inner.phase {
        Phase::Finished(r) => r.output.headers.clone(),
        _ => Vec::new(),
    }
}

/// Encodes the next bounded page of rows.
fn next_page(results: &mut Results, config: &TrinoConfig) -> Option<Vec<Value>> {
    let mut rows = Vec::new();
    let mut bytes = 0usize;
    while results.batch < results.output.batches.len() {
        let batch = &results.output.batches[results.batch];
        let num_rows = batch.num_rows();
        if results.row >= num_rows {
            results.batch += 1;
            results.row = 0;
            continue;
        }
        let per_row = (batch.get_array_memory_size() / num_rows.max(1)).max(8);
        let columns: Vec<_> = batch.columns().iter().map(materialize).collect();
        while results.row < num_rows
            && rows.len() < config.max_page_rows
            && (rows.is_empty() || bytes < config.target_page_bytes)
        {
            let r = results.row;
            rows.push(Value::Array(
                columns.iter().map(|c| encode_cell(c.as_ref(), r)).collect(),
            ));
            bytes += per_row;
            results.row += 1;
        }
        if results.row >= num_rows {
            results.batch += 1;
            results.row = 0;
        }
        if rows.len() >= config.max_page_rows || bytes >= config.target_page_bytes {
            break;
        }
    }
    // Skip trailing empty batches so `more` is accurate.
    while results.batch < results.output.batches.len()
        && results.output.batches[results.batch].num_rows() == 0
    {
        results.batch += 1;
    }
    results.rows_returned += rows.len() as u64;
    (!rows.is_empty()).then_some(rows)
}

fn results_document(
    query: &Query,
    inner: &Inner,
    base: &str,
    next_token: Option<u64>,
    data: Option<Vec<Value>>,
    error: Option<&TrinoError>,
) -> Value {
    let state = query.state_name(inner);
    let elapsed = inner
        .ended
        .unwrap_or_else(Instant::now)
        .duration_since(query.created)
        .as_millis() as u64;
    let processed_rows = match &inner.phase {
        Phase::Finished(r) => r.rows_returned,
        _ => 0,
    };
    let mut doc = json!({
        "id": query.id,
        "infoUri": format!("{base}/v1/query/{}", query.id),
        "stats": {
            "state": state,
            "queued": state == "QUEUED",
            "scheduled": state != "QUEUED",
            "nodes": 1,
            "totalSplits": 0,
            "queuedSplits": 0,
            "runningSplits": 0,
            "completedSplits": 0,
            "cpuTimeMillis": 0,
            "wallTimeMillis": elapsed,
            "queuedTimeMillis": 0,
            "elapsedTimeMillis": elapsed,
            "planningTimeMillis": 0,
            "analysisTimeMillis": 0,
            "processedRows": processed_rows,
            "processedBytes": 0,
            "physicalInputBytes": 0,
            "physicalWrittenBytes": 0,
            "internalNetworkInputBytes": 0,
            "peakMemoryBytes": 0,
            "spilledBytes": 0,
        },
        "warnings": [],
    });
    let obj = doc.as_object_mut().expect("object literal");
    if let Some(token) = next_token {
        obj.insert(
            "nextUri".into(),
            json!(format!(
                "{base}/v1/statement/executing/{}/{}/{token}",
                query.id, query.slug
            )),
        );
    }
    if let Phase::Finished(results) = &inner.phase {
        if let Some(columns) = &results.output.columns {
            obj.insert(
                "columns".into(),
                Value::Array(
                    columns
                        .iter()
                        .map(|c| column_json(&c.name, &c.ty))
                        .collect(),
                ),
            );
        }
        if let Some(kind) = &results.output.update_type {
            obj.insert("updateType".into(), json!(kind));
        }
    }
    if let Some(rows) = data {
        obj.insert("data".into(), Value::Array(rows));
    }
    if let Some(err) = error {
        obj.insert("error".into(), err.to_json());
    }
    doc
}

fn json_response(
    status: StatusCode,
    body: Value,
    session_headers: &[(&'static str, String)],
    family: HeaderFamily,
) -> Response {
    let mut response = (status, Json(body)).into_response();
    let headers = response.headers_mut();
    for (suffix, value) in session_headers {
        let (Ok(name), Ok(value)) = (
            HeaderName::try_from(family.name(suffix)),
            HeaderValue::from_str(value),
        ) else {
            continue;
        };
        headers.append(name, value);
    }
    response
}

fn cancel_query(state: &AppState, query: &Query, error: TrinoError, reason: &str) {
    if let Some(handle) = query.abort.lock().unwrap().take() {
        handle.abort();
    }
    {
        let mut inner = query.inner.lock().unwrap();
        let active = matches!(inner.phase, Phase::Running)
            || matches!(&inner.phase, Phase::Finished(r) if r.batch < r.output.batches.len());
        if active {
            inner.phase = Phase::Failed(error);
            inner.ended = Some(Instant::now());
            inner.last = None;
            if let (Some(t), Some(id)) = (&state.tracker, query.tracker_id) {
                let _ = t.cancel_query(&id);
            }
        }
    }
    let _ = query.done.send(true);
    tracing::debug!(query_id = %query.id, reason, "trino query canceled");
}

async fn cancel(
    State(state): State<AppState>,
    Path((id, slug, _token)): Path<(String, String, u64)>,
) -> Response {
    let Some(query) = lookup(&state, &id, &slug) else {
        return StatusCode::NO_CONTENT.into_response();
    };
    cancel_query(&state, &query, TrinoError::canceled(), "client DELETE");
    state.queries.lock().unwrap().remove(&id);
    StatusCode::NO_CONTENT.into_response()
}

async fn kill(State(state): State<AppState>, Path(id): Path<String>) -> Response {
    let query = state.queries.lock().unwrap().get(&id).cloned();
    match query {
        Some(q) => {
            cancel_query(&state, &q, TrinoError::canceled(), "kill");
            StatusCode::NO_CONTENT.into_response()
        }
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

fn query_summary(query: &Query, base: &str) -> Value {
    let inner = query.inner.lock().unwrap();
    let error = match &inner.phase {
        Phase::Failed(e) => Some(e.to_json()),
        _ => None,
    };
    json!({
        "queryId": query.id,
        "state": query.state_name(&inner),
        "query": query.sql,
        "self": format!("{base}/v1/query/{}", query.id),
        "session": {
            "user": query.user,
            "source": query.source,
            "clientInfo": query.client_info,
        },
        "arnebQueryId": query.tracker_id.map(|id| id.to_string()),
        "elapsedTimeMillis": inner
            .ended
            .unwrap_or_else(Instant::now)
            .duration_since(query.created)
            .as_millis() as u64,
        "error": error,
    })
}

async fn query_info(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let query = state.queries.lock().unwrap().get(&id).cloned();
    match query {
        Some(q) => Json(query_summary(&q, &base_uri(&headers))).into_response(),
        None => (StatusCode::NOT_FOUND, "Query not found").into_response(),
    }
}

async fn list_queries(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let base = base_uri(&headers);
    let queries: Vec<Arc<Query>> = state.queries.lock().unwrap().values().cloned().collect();
    Json(Value::Array(
        queries.iter().map(|q| query_summary(q, &base)).collect(),
    ))
    .into_response()
}

async fn info(State(state): State<AppState>) -> Response {
    let uptime = state.started.elapsed().as_secs_f64();
    Json(json!({
        "nodeVersion": {
            "version": format!("{TRINO_COMPAT_VERSION}-arneb-{}", env!("CARGO_PKG_VERSION")),
        },
        "environment": "arneb",
        "coordinator": true,
        "starting": false,
        "uptime": format!("{:.2}m", uptime / 60.0),
    }))
    .into_response()
}

async fn info_state() -> Response {
    Json(json!("ACTIVE")).into_response()
}

/// Expires finished queries and cancels abandoned ones.
async fn reaper(state: AppState) {
    let tick = (state.config.client_timeout.min(state.config.retention) / 4)
        .clamp(Duration::from_millis(50), Duration::from_secs(5));
    let mut interval = tokio::time::interval(tick);
    loop {
        interval.tick().await;
        let queries: Vec<Arc<Query>> = state.queries.lock().unwrap().values().cloned().collect();
        for query in queries {
            let (idle, active) = {
                let inner = query.inner.lock().unwrap();
                let active = matches!(inner.phase, Phase::Running)
                    || matches!(&inner.phase, Phase::Finished(r) if r.batch < r.output.batches.len());
                (inner.last_access.elapsed(), active)
            };
            if active && idle > state.config.client_timeout {
                cancel_query(&state, &query, TrinoError::abandoned(), "abandoned");
                state.queries.lock().unwrap().remove(&query.id);
            } else if !active && idle > state.config.retention {
                state.queries.lock().unwrap().remove(&query.id);
            }
        }
    }
}
