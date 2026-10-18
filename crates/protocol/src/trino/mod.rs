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

mod auth;
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
use axum::Extension;
use axum::{Json, Router};
use futures::FutureExt;
use serde_json::{json, Value};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::AbortHandle;
use tokio_rustls::TlsAcceptor;

use arneb_catalog::CatalogManager;
use arneb_common::identifiers::QueryId;
use arneb_connectors::ConnectorRegistry;
use arneb_execution::memory_pool::{MemoryPool, UnboundedMemoryPool};
use arneb_scheduler::{QueryState as TrackerState, QueryTracker};

use crate::auth::{AuthConfigError, UserCredentials};
use crate::handler::DistributedExecutor;
use auth::{AuthUser, Authenticator};
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
    tls: Option<TlsAcceptor>,
    auth: Option<Authenticator>,
    verifications: Arc<AtomicU64>,
    query_memory_limit: Option<usize>,
    admission: Option<arneb_scheduler::AdmissionController>,
    statement_timeout: Option<Duration>,
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
            tls: None,
            auth: None,
            verifications: Arc::new(AtomicU64::new(0)),
            query_memory_limit: None,
            admission: None,
            statement_timeout: None,
        }
    }

    /// Require HTTP Basic auth against these users (`[auth] type =
    /// "password"`); default: none, every `X-Trino-User` is trusted.
    pub fn with_password_auth(
        mut self,
        creds: Option<Arc<UserCredentials>>,
    ) -> Result<Self, AuthConfigError> {
        self.auth = creds
            .map(|c| Authenticator::new(c, Arc::clone(&self.verifications)))
            .transpose()?;
        Ok(self)
    }

    /// Count of full password verifications (cache misses); test hook.
    #[doc(hidden)]
    pub fn password_verifications(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.verifications)
    }

    /// Serve HTTPS only with this acceptor (default: plain HTTP).
    pub fn with_tls(mut self, tls: Option<TlsAcceptor>) -> Self {
        self.tls = tls;
        self
    }

    /// Server default time limit for queries whose session sets neither
    /// `query_max_run_time` nor `query_max_execution_time`
    /// (`[admission] statement_timeout`); it counts queue time, like
    /// `query_max_run_time`. Default: off.
    pub fn with_statement_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.statement_timeout = timeout;
        self
    }

    /// Queue statements through this admission controller (shared with
    /// pgwire). Default: none, every statement runs at once.
    pub fn with_admission(
        mut self,
        admission: Option<arneb_scheduler::AdmissionController>,
    ) -> Self {
        self.admission = admission;
        self
    }

    /// Limit each statement to `limit` bytes of the memory pool on this node
    /// (`query_max_memory_per_node`; default: no per-query limit).
    pub fn with_query_memory_limit(mut self, limit: Option<usize>) -> Self {
        self.query_memory_limit = limit;
        self
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
                query_memory_limit: self.query_memory_limit,
            }),
            queries: Arc::new(Mutex::new(HashMap::new())),
            config: Arc::new(self.config.clone()),
            tracker: self.query_tracker.clone(),
            started: Instant::now(),
            counter: Arc::new(AtomicU64::new(0)),
            scheme: if self.tls.is_some() { "https" } else { "http" },
            auth: self.auth.clone(),
            admission: self.admission.clone(),
            statement_timeout: self.statement_timeout,
        }
    }

    /// Binds `config.bind_address` and serves until the process exits.
    pub async fn start(&self) -> std::io::Result<()> {
        let listener = TcpListener::bind(&self.config.bind_address).await?;
        tracing::info!(
            address = %self.config.bind_address,
            scheme = if self.tls.is_some() { "https" } else { "http" },
            auth = if self.auth.is_some() { "password" } else { "none" },
            "Trino client protocol listening"
        );
        self.serve(listener).await
    }

    /// Serves on an already-bound listener (used by tests with port 0).
    ///
    /// Fails closed with [`std::io::ErrorKind::InvalidInput`] when password
    /// auth is on without TLS and the listener is not loopback-only: Trino
    /// clients send Basic credentials only over HTTPS, so such a listener
    /// would be unusable or would take passwords in cleartext. (The server
    /// binary rejects password auth without `[tls]` outright, loopback
    /// included.)
    pub async fn serve(&self, listener: TcpListener) -> std::io::Result<()> {
        check_transport(
            self.auth.is_some(),
            self.tls.is_some(),
            listener.local_addr()?,
        )?;
        let state = self.state();
        tokio::spawn(reaper(state.clone()));
        match &self.tls {
            None => axum::serve(listener, router(state)).await,
            Some(acceptor) => serve_tls(listener, acceptor.clone(), router(state)).await,
        }
    }
}

fn check_transport(
    password_auth: bool,
    tls: bool,
    local: std::net::SocketAddr,
) -> std::io::Result<()> {
    if password_auth && !tls && !local.ip().is_loopback() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "Trino listener on {local}: password auth requires TLS on a non-loopback \
                 address"
            ),
        ));
    }
    Ok(())
}

/// How long a client gets to finish the TLS handshake.
const TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// HTTPS accept loop: TCP accept → rustls handshake → hyper (HTTP/1.1 or h2
/// by ALPN) serving the axum router. A failed handshake only drops that
/// connection.
async fn serve_tls(
    listener: TcpListener,
    acceptor: TlsAcceptor,
    router: Router,
) -> std::io::Result<()> {
    use hyper_util::rt::{TokioExecutor, TokioIo};
    use hyper_util::server::conn::auto;
    use hyper_util::service::TowerToHyperService;

    loop {
        let (tcp, peer) = match listener.accept().await {
            Ok(conn) => conn,
            Err(e) => {
                // Same back-off axum::serve uses (e.g. EMFILE).
                tracing::warn!(error = %e, "Trino listener accept failed");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };
        let acceptor = acceptor.clone();
        let router = router.clone();
        tokio::spawn(async move {
            let tls = match tokio::time::timeout(TLS_HANDSHAKE_TIMEOUT, acceptor.accept(tcp)).await
            {
                Ok(Ok(stream)) => stream,
                Ok(Err(e)) => {
                    tracing::debug!(peer = %peer, error = %e, "Trino TLS handshake failed");
                    return;
                }
                Err(_) => {
                    tracing::debug!(peer = %peer, "Trino TLS handshake timed out");
                    return;
                }
            };
            let service = TowerToHyperService::new(router);
            if let Err(e) = auto::Builder::new(TokioExecutor::new())
                .serve_connection_with_upgrades(TokioIo::new(tls), service)
                .await
            {
                tracing::debug!(peer = %peer, error = %e, "Trino HTTPS connection closed");
            }
        });
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
    /// Scheme for `nextUri` / `infoUri` when the client sends no
    /// `X-Forwarded-Proto`: `https` when the listener serves TLS.
    scheme: &'static str,
    auth: Option<Authenticator>,
    admission: Option<arneb_scheduler::AdmissionController>,
    statement_timeout: Option<Duration>,
}

fn router(state: AppState) -> Router {
    let auth = state.auth.clone();
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
        .layer(axum::middleware::from_fn_with_state(
            auth,
            auth::require_auth,
        ))
        .layer(axum::middleware::map_request(host_from_authority))
        .with_state(state)
}

/// HTTP/2 carries the host in the `:authority` pseudo-header, not `Host`.
/// Copy it into `Host` so [`base_uri`] builds `nextUri` / `infoUri` from the
/// address the client actually used (h2 is negotiated on the HTTPS
/// listener).
async fn host_from_authority(mut req: axum::extract::Request) -> axum::extract::Request {
    if !req.headers().contains_key("Host") {
        let authority = req.uri().authority().map(|a| a.as_str().to_owned());
        if let Some(v) = authority.and_then(|a| HeaderValue::from_str(&a).ok()) {
            req.headers_mut().insert("Host", v);
        }
    }
    req
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
    /// Waiting for an admission slot: reported as `QUEUED`.
    queued: std::sync::atomic::AtomicBool,
}

impl Query {
    fn state_name(&self, inner: &Inner) -> &'static str {
        match &inner.phase {
            Phase::Running if self.queued.load(Ordering::Relaxed) => "QUEUED",
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

fn base_uri(headers: &HeaderMap, default_scheme: &str) -> String {
    let host = headers
        .get("X-Forwarded-Host")
        .or_else(|| headers.get("Host"))
        .and_then(|v| v.to_str().ok())
        .unwrap_or("localhost");
    let proto = headers
        .get("X-Forwarded-Proto")
        .and_then(|v| v.to_str().ok())
        .unwrap_or(default_scheme);
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

async fn submit(
    State(state): State<AppState>,
    user: Option<Extension<AuthUser>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let sql = String::from_utf8_lossy(&body).trim().to_string();
    if sql.is_empty() {
        return (StatusCode::BAD_REQUEST, "SQL statement is empty").into_response();
    }
    let session = ClientSession::from_headers(&headers, user.as_ref().map(|Extension(u)| u));
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
        queued: std::sync::atomic::AtomicBool::new(false),
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
        // admission-control D5: wait for a slot before planning; the permit
        // is held until the statement task completes (paging out the
        // materialized results does not hold it).
        // admission-control D7: `query_max_run_time` (or the server
        // default) runs from submission and covers the queue wait;
        // `query_max_execution_time` runs from admission. On expiry the
        // statement future is dropped, which cancels it on the workers.
        let limits = match TimeLimits::from_session(&session, task_state.statement_timeout) {
            Ok(l) => l,
            Err(err) => {
                complete(&task_state, &task_query, Err(err));
                return;
            }
        };
        // A limit past the clock's range (`checked_add` overflow) never fires.
        let run_deadline = limits
            .run
            .and_then(|d| tokio::time::Instant::from_std(task_query.created).checked_add(d));
        let _admission =
            match with_deadline(run_deadline, admit(&task_state, &task_query, &session)).await {
                Some(Ok(permit)) => permit,
                Some(Err(err)) => {
                    complete(&task_state, &task_query, Err(err));
                    return;
                }
                None => {
                    complete(
                        &task_state,
                        &task_query,
                        Err(limits.exceeded("query_max_run_time")),
                    );
                    return;
                }
            };
        let exec_deadline = match (
            run_deadline,
            limits
                .execution
                .and_then(|d| tokio::time::Instant::now().checked_add(d)),
        ) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        tracker_transition(
            &task_state,
            task_query.tracker_id,
            &[
                TrackerState::Planning,
                TrackerState::Starting,
                TrackerState::Running,
            ],
        );
        let run = std::panic::AssertUnwindSafe(
            task_state.engine.run_statement(&session, &task_query.sql),
        )
        .catch_unwind();
        let result = match with_deadline(exec_deadline, run).await {
            Some(outcome) => {
                outcome.unwrap_or_else(|_| Err(TrinoError::internal("query execution panicked")))
            }
            None => Err(limits.exceeded_during_execution(run_deadline, exec_deadline)),
        };
        complete(&task_state, &task_query, result);
    });
    *query.abort.lock().unwrap() = Some(handle.abort_handle());

    let base = base_uri(&headers, state.scheme);
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

/// A query's time limits from its session (admission-control D7).
struct TimeLimits {
    /// `query_max_execution_time`: from admission.
    execution: Option<Duration>,
    /// `query_max_run_time`, else the server default: from submission.
    run: Option<Duration>,
}

impl TimeLimits {
    fn from_session(
        session: &ClientSession,
        server_default: Option<Duration>,
    ) -> Result<Self, TrinoError> {
        let property = |name: &str| -> Result<Option<Option<Duration>>, TrinoError> {
            session
                .properties
                .iter()
                .rev()
                .find(|(k, _)| k.eq_ignore_ascii_case(name))
                .map(|(_, v)| {
                    arneb_common::duration::parse_time_limit(v)
                        .map_err(|e| TrinoError::invalid_session_property(format!("{name}: {e}")))
                })
                .transpose()
        };
        let execution = property("query_max_execution_time")?;
        let run = property("query_max_run_time")?;
        let run = match (execution, run) {
            (_, Some(run)) => run,
            (None, None) => server_default,
            (Some(_), None) => None,
        };
        Ok(Self {
            execution: execution.flatten(),
            run,
        })
    }

    fn exceeded(&self, property: &str) -> TrinoError {
        let limit = match property {
            "query_max_execution_time" => self.execution,
            _ => self.run,
        };
        TrinoError::exceeded_time_limit(format!(
            "Query exceeded the maximum {} limit of {}",
            property.trim_start_matches("query_max_").replace('_', " "),
            arneb_common::duration::format_pg_time_limit(limit)
        ))
    }

    /// Which limit fired during execution: the earlier deadline.
    fn exceeded_during_execution(
        &self,
        run_deadline: Option<tokio::time::Instant>,
        exec_deadline: Option<tokio::time::Instant>,
    ) -> TrinoError {
        if run_deadline.is_some() && run_deadline == exec_deadline {
            self.exceeded("query_max_run_time")
        } else {
            self.exceeded("query_max_execution_time")
        }
    }
}

/// Await `fut` until `deadline`; `None` when the deadline passed first.
async fn with_deadline<F: std::future::Future>(
    deadline: Option<tokio::time::Instant>,
    fut: F,
) -> Option<F::Output> {
    match deadline {
        None => Some(fut.await),
        Some(at) => tokio::time::timeout_at(at, fut).await.ok(),
    }
}

/// Take an admission slot for a statement that executes a plan, reporting
/// the query as `QUEUED` while it waits.
async fn admit(
    state: &AppState,
    query: &Query,
    session: &ClientSession,
) -> Result<Option<arneb_scheduler::AdmissionPermit>, TrinoError> {
    let Some(controller) = &state.admission else {
        return Ok(None);
    };
    if !needs_admission(session, &query.sql) {
        return Ok(None);
    }
    let pending = controller
        .admit(&crate::handler::admission_label(&query.sql))
        .map_err(|e| TrinoError::queue_full(e.to_string()))?;
    if pending.queue_position().is_some() {
        query.queued.store(true, Ordering::Relaxed);
    }
    let permit = pending.wait().await;
    query.queued.store(false, Ordering::Relaxed);
    Ok(Some(permit))
}

/// Whether `sql` takes an admission slot, deciding `EXECUTE` /
/// `EXECUTE IMMEDIATE` on the statement they run. Other control statements
/// (`SHOW`, `USE`, `SET SESSION`, `PREPARE`, …) never queue; an `EXECUTE`
/// that cannot be resolved fails in the engine, so it does not queue either.
fn needs_admission(session: &ClientSession, sql: &str) -> bool {
    use statements::{parse_control, ControlStatement};
    let sql = sql.trim().trim_end_matches(';').trim();
    let target = match parse_control(sql) {
        None => return crate::handler::needs_admission(sql),
        Some(ControlStatement::Execute { name, .. }) => match session.prepared_statement(&name) {
            Some(prepared) => prepared.to_owned(),
            None => return false,
        },
        Some(ControlStatement::ExecuteImmediate { sql, .. }) => sql,
        Some(_) => return false,
    };
    crate::handler::needs_admission(&target)
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

/// Under password auth, a query is only visible to the user who submitted it.
fn not_owner(user: &Option<Extension<AuthUser>>, query: &Query) -> bool {
    matches!(user, Some(Extension(AuthUser(u))) if *u != query.user)
}

fn lookup(state: &AppState, id: &str, slug: &str) -> Option<Arc<Query>> {
    let queries = state.queries.lock().unwrap();
    queries.get(id).filter(|q| q.slug == slug).cloned()
}

async fn fetch(
    State(state): State<AppState>,
    Path((id, slug, token)): Path<(String, String, u64)>,
    user: Option<Extension<AuthUser>>,
    headers: HeaderMap,
) -> Response {
    let Some(query) = lookup(&state, &id, &slug) else {
        return (StatusCode::NOT_FOUND, "Query not found").into_response();
    };
    if not_owner(&user, &query) {
        return auth::forbidden("Query belongs to another user");
    }
    let base = base_uri(&headers, state.scheme);

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
    user: Option<Extension<AuthUser>>,
) -> Response {
    let Some(query) = lookup(&state, &id, &slug) else {
        return StatusCode::NO_CONTENT.into_response();
    };
    if not_owner(&user, &query) {
        return auth::forbidden("Query belongs to another user");
    }
    cancel_query(&state, &query, TrinoError::canceled(), "client DELETE");
    state.queries.lock().unwrap().remove(&id);
    StatusCode::NO_CONTENT.into_response()
}

async fn kill(
    State(state): State<AppState>,
    Path(id): Path<String>,
    user: Option<Extension<AuthUser>>,
) -> Response {
    let query = state.queries.lock().unwrap().get(&id).cloned();
    match query {
        Some(q) if not_owner(&user, &q) => auth::forbidden("Query belongs to another user"),
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
    user: Option<Extension<AuthUser>>,
    headers: HeaderMap,
) -> Response {
    let query = state.queries.lock().unwrap().get(&id).cloned();
    match query {
        Some(q) if not_owner(&user, &q) => auth::forbidden("Query belongs to another user"),
        Some(q) => Json(query_summary(&q, &base_uri(&headers, state.scheme))).into_response(),
        None => (StatusCode::NOT_FOUND, "Query not found").into_response(),
    }
}

async fn list_queries(
    State(state): State<AppState>,
    user: Option<Extension<AuthUser>>,
    headers: HeaderMap,
) -> Response {
    let base = base_uri(&headers, state.scheme);
    let queries: Vec<Arc<Query>> = state
        .queries
        .lock()
        .unwrap()
        .values()
        .filter(|q| !not_owner(&user, q))
        .cloned()
        .collect();
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
