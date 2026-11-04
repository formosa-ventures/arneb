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

use crate::auth::{AuthConfigError, CredentialSource};
use crate::handler::DistributedExecutor;
use auth::AuthUser;
pub use auth::{BasicAuthenticator, BasicVerdict, SlotError};
pub(crate) use engine::{Engine, QueryOutput};
pub(crate) use error::TrinoError;
pub(crate) use metadata::{
    build_virtual_catalogs, catalog_names, schema_names, table_columns, table_names,
    INFORMATION_SCHEMA, SYSTEM_CATALOG,
};
pub(crate) use session::ClientSession;
use session::HeaderFamily;
pub(crate) use statements::{parse_control, ControlStatement};
use types::column_json;
pub(crate) use types::{encode_cell, materialize, trino_type_of};

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
    /// HTTPS only: an HTTP/1 connection that has not sent a full request
    /// header block this long after the request starts is closed, so slow
    /// or stalled clients cannot pin connections open.
    pub header_read_timeout: Duration,
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
            header_read_timeout: Duration::from_secs(30),
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
    query_tracker: Arc<QueryTracker>,
    tls: Option<TlsAcceptor>,
    auth: Option<BasicAuthenticator>,
    query_memory_limit: Option<usize>,
    admission: Option<arneb_scheduler::AdmissionController>,
    statement_timeout: Option<Duration>,
    access_rules: arneb_catalog::RulesHandle,
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
            query_tracker: Arc::new(QueryTracker::new()),
            tls: None,
            auth: None,
            query_memory_limit: None,
            admission: None,
            statement_timeout: None,
            access_rules: Default::default(),
        }
    }

    /// Require HTTP Basic auth against these users (`[auth] type =
    /// "password"`); default: none, every `X-Trino-User` is trusted.
    pub fn with_password_auth(
        self,
        creds: Option<Arc<dyn CredentialSource>>,
    ) -> Result<Self, AuthConfigError> {
        let auth = creds.map(BasicAuthenticator::new).transpose()?;
        Ok(self.with_authenticator(auth))
    }

    /// Require HTTP Basic auth through `auth`, an authenticator the caller
    /// shares with other listeners (one PBKDF2 limiter, queue and cache for
    /// all of them). `None`: no auth, every `X-Trino-User` is trusted.
    pub fn with_authenticator(mut self, auth: Option<BasicAuthenticator>) -> Self {
        self.auth = auth;
        self
    }

    /// Count of full password verifications (cache misses); test hook.
    #[doc(hidden)]
    pub fn password_verifications(&self) -> Arc<AtomicU64> {
        self.auth
            .as_ref()
            .map(|a| Arc::clone(&a.verifications))
            .unwrap_or_default()
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

    /// Check every statement against `rules` for the session user
    /// (file-based-access-control). Pass a [`arneb_catalog::RulesHandle`]
    /// to share live rules: each statement uses the rules current when it
    /// starts. Default: none, every user reads all.
    pub fn with_access_rules(mut self, rules: impl Into<arneb_catalog::RulesHandle>) -> Self {
        self.access_rules = rules.into();
        self
    }

    /// Record queries in `tracker` (Web UI, `system.runtime.queries`,
    /// `/metrics`) instead of a private one.
    pub fn with_query_tracker(mut self, tracker: Arc<QueryTracker>) -> Self {
        self.query_tracker = tracker;
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
                query_tracker: Arc::clone(&self.query_tracker),
                access_rules: self.access_rules.clone(),
            }),
            queries: Arc::new(Mutex::new(HashMap::new())),
            config: Arc::new(self.config.clone()),
            tracker: Arc::clone(&self.query_tracker),
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
            Some(acceptor) => {
                let header_timeout = self.config.header_read_timeout;
                serve_tls(listener, acceptor.clone(), router(state), header_timeout).await
            }
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
/// h2 keep-alive: ping an h2 connection this often...
const H2_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(30);
/// ...and close it when a ping goes unanswered this long, so a dead or
/// stalled h2 peer does not hold its connection open indefinitely.
const H2_KEEP_ALIVE_TIMEOUT: Duration = Duration::from_secs(20);

/// HTTPS accept loop: TCP accept → rustls handshake → hyper (HTTP/1.1 or h2
/// by ALPN) serving the axum router. A failed handshake only drops that
/// connection; an HTTP/1 request whose headers take longer than
/// `header_timeout` closes it, and an h2 connection whose peer stops
/// answering keep-alive pings is closed after [`H2_KEEP_ALIVE_TIMEOUT`].
async fn serve_tls(
    listener: TcpListener,
    acceptor: TlsAcceptor,
    router: Router,
    header_timeout: Duration,
) -> std::io::Result<()> {
    use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
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
            let mut builder = auto::Builder::new(TokioExecutor::new());
            // hyper only enforces `header_read_timeout` / keep-alive with a
            // timer set.
            builder
                .http1()
                .timer(TokioTimer::new())
                .header_read_timeout(header_timeout);
            builder
                .http2()
                .timer(TokioTimer::new())
                .keep_alive_interval(Some(H2_KEEP_ALIVE_INTERVAL))
                .keep_alive_timeout(H2_KEEP_ALIVE_TIMEOUT);
            if let Err(e) = builder
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
    tracker: Arc<QueryTracker>,
    started: Instant,
    counter: Arc<AtomicU64>,
    /// Scheme for `nextUri` / `infoUri` when the client sends no
    /// `X-Forwarded-Proto`: `https` when the listener serves TLS.
    scheme: &'static str,
    auth: Option<BasicAuthenticator>,
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
    /// Runs the query's work; kills and client cancels go through it, so
    /// neither relabels work that already returned.
    switch: arneb_scheduler::KillSwitch,
    tracker_id: QueryId,
    /// Waiting for an admission slot: reported as `QUEUED`.
    queued: std::sync::atomic::AtomicBool,
    /// The statement only reads ([`only_reads`]): only then can a cancel
    /// stop it while it pages out its results.
    reads: bool,
}

impl Query {
    /// Paging out the results of a statement that only reads: it finished,
    /// and pages remain. A finished write never is, even before the client
    /// fetched its update count, so no cancel relabels it.
    fn paging(&self, inner: &Inner) -> bool {
        self.reads && matches!(&inner.phase, Phase::Finished(r) if r.batch < r.output.batches.len())
    }

    fn is_paging(&self) -> bool {
        self.paging(&self.inner.lock().unwrap())
    }

    /// Still running, or paging: a cancel (or the reaper) stops it.
    fn active(&self, inner: &Inner) -> bool {
        matches!(inner.phase, Phase::Running) || self.paging(inner)
    }

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
    let tracker_id =
        state
            .tracker
            .create_query_from(sql.clone(), "trino", Some(session.user.clone()));
    let (done, _) = watch::channel(false);
    let reads = only_reads(&session, &sql);
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
        switch: arneb_scheduler::KillSwitch::new(),
        tracker_id,
        queued: std::sync::atomic::AtomicBool::new(false),
        reads,
    });
    state
        .queries
        .lock()
        .unwrap()
        .insert(id.clone(), Arc::clone(&query));
    tracing::debug!(query_id = %id, user = %session.user, "trino query submitted");

    // A kill through the tracker (manage-api D7), registered before the
    // task starts: the kill switch stops the work unless it completed, in
    // which case the kill is not applied and the query keeps its real
    // outcome. Weak references, so the tracker never keeps the query or
    // itself alive.
    let switch = query.switch.clone();
    let (weak_query, weak_tracker) = (Arc::downgrade(&query), Arc::downgrade(&state.tracker));
    state.tracker.set_canceller(
        &query.tracker_id,
        switch.canceller(move |by| {
            if let (Some(query), Some(tracker)) = (weak_query.upgrade(), weak_tracker.upgrade()) {
                stop_query(&tracker, &query, TrinoError::killed_by(by), "killed");
            }
        }),
    );

    let task_state = state.clone();
    let task_query = Arc::clone(&query);
    let handle = tokio::spawn(async move {
        let work = run_query(&task_state, &task_query, session);
        // `None`: killed first, and the kill already ended the query.
        if let Some(result) = switch.guard(work).await {
            complete(&task_state, &task_query, result);
        }
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

/// A submitted query's work: admission, then execution under its time
/// limits. The outcome is recorded by [`complete`].
async fn run_query(
    state: &AppState,
    query: &Query,
    session: ClientSession,
) -> Result<QueryOutput, TrinoError> {
    // admission-control D5: wait for a slot before planning; the permit
    // is held until the statement task completes (paging out the
    // materialized results does not hold it).
    // admission-control D7: `query_max_run_time` (or the server
    // default) runs from submission and covers the queue wait;
    // `query_max_execution_time` runs from admission. On expiry the
    // statement future is dropped, which cancels it on the workers.
    let limits = TimeLimits::from_session(&session, state.statement_timeout)?;
    // A limit past the clock's range (`checked_add` overflow) never fires.
    let run_deadline = limits
        .run
        .and_then(|d| tokio::time::Instant::from_std(query.created).checked_add(d));
    let _admission = with_deadline(run_deadline, admit(state, query, &session))
        .await
        .ok_or_else(|| limits.exceeded("query_max_run_time"))??;
    let exec_deadline = match (
        run_deadline,
        limits
            .execution
            .and_then(|d| tokio::time::Instant::now().checked_add(d)),
    ) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    };
    let _ = state
        .tracker
        .advance_to(&query.tracker_id, TrackerState::Running);
    let run = std::panic::AssertUnwindSafe(state.engine.run_statement(&session, &query.sql))
        .catch_unwind();
    match with_deadline(exec_deadline, run).await {
        Some(outcome) => {
            outcome.unwrap_or_else(|_| Err(TrinoError::internal("query execution panicked")))
        }
        None => Err(limits.exceeded_during_execution(run_deadline, exec_deadline)),
    }
}

/// A query's time limits from its session (admission-control D7).
pub(crate) struct TimeLimits {
    /// `query_max_execution_time`: from admission.
    pub(crate) execution: Option<Duration>,
    /// `query_max_run_time`, else the server default: from submission.
    pub(crate) run: Option<Duration>,
}

impl TimeLimits {
    pub(crate) fn from_session(
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

    pub(crate) fn exceeded(&self, property: &str) -> TrinoError {
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
    pub(crate) fn exceeded_during_execution(
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
pub(crate) async fn with_deadline<F: std::future::Future>(
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
        .admit_with(|| crate::handler::admission_label(&query.sql))
        .map_err(|e| TrinoError::queue_full(e.to_string()))?;
    if pending.queue_position().is_some() {
        query.queued.store(true, Ordering::Relaxed);
    }
    let permit = pending.wait().await;
    query.queued.store(false, Ordering::Relaxed);
    Ok(Some(permit))
}

/// Whether `sql` only reads: a query, or an `EXPLAIN` without `ANALYZE`,
/// directly or through `EXECUTE` / `EXECUTE IMMEDIATE`. Other control
/// statements (`SHOW`, `USE`, `SET SESSION`, `PREPARE`, …) write no data.
/// SQL that does not parse counts as a write (fail closed: a cancel then
/// never relabels the statement once it finished).
fn only_reads(session: &ClientSession, sql: &str) -> bool {
    use arneb_sql_parser::ast::Statement;
    let sql = sql.trim().trim_end_matches(';').trim();
    let target = match parse_control(sql) {
        None => sql.to_owned(),
        Some(ControlStatement::Execute { name, .. }) => match session.prepared_statement(&name) {
            Some(prepared) => prepared.to_owned(),
            None => return false,
        },
        Some(ControlStatement::ExecuteImmediate { sql, .. }) => sql,
        Some(_) => return true,
    };
    matches!(
        arneb_sql_parser::parse(&target),
        Ok(Statement::Query { .. } | Statement::Explain { analyze: false, .. })
    )
}

/// Whether `sql` takes an admission slot, deciding `EXECUTE` /
/// `EXECUTE IMMEDIATE` on the statement they run. Other control statements
/// (`SHOW`, `USE`, `SET SESSION`, `PREPARE`, …) never queue; an `EXECUTE`
/// that cannot be resolved fails in the engine, so it does not queue either.
pub(crate) fn needs_admission(session: &ClientSession, sql: &str) -> bool {
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

/// Records the statement's outcome, unless it was cancelled meanwhile. A
/// kill that lands after the work completed, or during the poll that
/// completed it, is not applied (see [`cancel_now`]), so a completed query
/// keeps its real result.
fn complete(state: &AppState, query: &Query, result: Result<QueryOutput, TrinoError>) {
    {
        let mut inner = query.inner.lock().unwrap();
        if !matches!(inner.phase, Phase::Running) {
            return;
        }
        inner.ended = Some(Instant::now());
        inner.phase = ended_phase(&state.tracker, &query.tracker_id, result);
    }
    let _ = query.done.send(true);
}

/// Records `result` in the tracker and returns the query's terminal phase.
/// Work that completed keeps its result, even if the tracker already marked
/// the query cancelled: its effects (a committed write) happened. A failure
/// of a query the tracker marked cancelled reports the cancellation.
fn ended_phase(
    tracker: &QueryTracker,
    id: &QueryId,
    result: Result<QueryOutput, TrinoError>,
) -> Phase {
    let killed = || {
        tracker
            .cancelled_by(id)
            .map(|by| TrinoError::killed_by(&by))
    };
    match result {
        Ok(output) => {
            let rows = output.batches.iter().map(|b| b.num_rows() as u64).sum();
            let _ = tracker.finish_query(id, rows);
            Phase::Finished(Results {
                output,
                batch: 0,
                row: 0,
                rows_returned: 0,
            })
        }
        Err(err) => {
            let failed = tracker.fail_query(id, err.message.clone());
            Phase::Failed(failed.err().and_then(|_| killed()).unwrap_or(err))
        }
    }
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
        let per_row =
            (arneb_common::memory_pool::batch_memory_size(batch) / num_rows.max(1)).max(8);
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

/// A client cancel or the reaper: kill the query's work through its kill
/// switch, then stop the query. Work that already returned (or returns in
/// the poll in flight) keeps its outcome, which `complete` records; the
/// returned task waits, without blocking a thread, for at most that one
/// poll. It runs on its own, so a dropped request does not leave it half
/// done; awaiting it is optional.
///
/// Whether the query is paging ([`Query::paging`]) is read here, when the
/// cancel arrives, not in the task: see [`cancel_now`].
fn cancel_query(
    tracker: &Arc<QueryTracker>,
    query: &Arc<Query>,
    error: TrinoError,
    reason: &'static str,
) -> tokio::task::JoinHandle<()> {
    let paging = query.is_paging();
    let (tracker, query) = (Arc::clone(tracker), Arc::clone(query));
    tokio::spawn(async move { cancel_now(&tracker, &query, error, reason, paging).await })
}

/// The body of [`cancel_query`]; `paging` is whether the query was paging
/// out a read's results when the cancel arrived.
///
/// That is decided before the kill: the statement task records the result
/// (`complete`) in the same poll that completes the work, so by the time a
/// kill pending on that poll resumes, the phase already reads `Finished`.
/// A kill that was not applied therefore stops the query only when it was
/// already paging when the cancel arrived; a write that finished never is.
async fn cancel_now(
    tracker: &QueryTracker,
    query: &Query,
    error: TrinoError,
    reason: &str,
    paging: bool,
) {
    if !query.switch.kill().await && !paging {
        tracing::debug!(query_id = %query.id, reason, "trino cancel after the work returned");
        return;
    }
    stop_query(tracker, query, error, reason);
}

/// Ends a query whose work a kill stopped (or that is paging out results):
/// aborts its task, marks it failed with `error` and cancelled in the
/// tracker.
fn stop_query(tracker: &QueryTracker, query: &Query, error: TrinoError, reason: &str) {
    if let Some(handle) = query.abort.lock().unwrap().take() {
        handle.abort();
    }
    {
        let mut inner = query.inner.lock().unwrap();
        if query.active(&inner) {
            inner.phase = Phase::Failed(error);
            inner.ended = Some(Instant::now());
            inner.last = None;
            let _ = tracker.cancel_query(&query.tracker_id);
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
    let _ = cancel_query(
        &state.tracker,
        &query,
        TrinoError::canceled(),
        "client DELETE",
    )
    .await;
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
            let _ = cancel_query(&state.tracker, &q, TrinoError::canceled(), "kill").await;
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
        "arnebQueryId": query.tracker_id.to_string(),
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
                (inner.last_access.elapsed(), query.active(&inner))
            };
            if active && idle > state.config.client_timeout {
                // Not awaited: a kill may wait for one poll of the work,
                // which must not hold up the rest of the pass.
                drop(cancel_query(
                    &state.tracker,
                    &query,
                    TrinoError::abandoned(),
                    "abandoned",
                ));
                state.queries.lock().unwrap().remove(&query.id);
            } else if !active && idle > state.config.retention {
                state.queries.lock().unwrap().remove(&query.id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arneb_scheduler::KillOutcome;

    fn rows() -> QueryOutput {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("n", arrow::datatypes::DataType::Int64, false),
        ]));
        let column: arrow::array::ArrayRef = Arc::new(arrow::array::Int64Array::from(vec![1, 2]));
        QueryOutput {
            columns: None,
            batches: vec![arrow::record_batch::RecordBatch::try_new(schema, vec![column]).unwrap()],
            update_type: None,
            headers: Vec::new(),
        }
    }

    /// A kill that lands after the work completed, before `complete`
    /// recorded it (an INSERT that committed): the kill is not applied and
    /// the query keeps its real result.
    #[tokio::test]
    async fn a_kill_after_the_work_completed_keeps_the_result() {
        let tracker = QueryTracker::new();
        let id = tracker.create_query_from("INSERT INTO t".into(), "trino", Some("bob".into()));
        tracker.advance_to(&id, TrackerState::Running).unwrap();
        let switch = arneb_scheduler::KillSwitch::new();
        tracker.set_canceller(&id, switch.canceller(|_| panic!("stopped completed work")));
        let result = switch.guard(async { Ok(rows()) }).await.unwrap();
        assert_eq!(
            tracker.kill_query(&id, "root").await.unwrap(),
            KillOutcome::NotApplied {
                state: TrackerState::Running
            }
        );
        assert!(matches!(
            ended_phase(&tracker, &id, result),
            Phase::Finished(_)
        ));
        let info = tracker.get_query(&id).unwrap();
        assert_eq!(info.state, TrackerState::Finished);
        assert_eq!(info.rows, Some(2));
    }

    /// A kill before the task starts (the canceller is registered before
    /// the spawn) stops all of the work; a failure of a query the tracker
    /// marked cancelled reports the cancellation.
    #[tokio::test]
    async fn a_kill_before_the_task_starts_stops_the_work() {
        let tracker = QueryTracker::new();
        let id = tracker.create_query_from("INSERT INTO t".into(), "trino", Some("bob".into()));
        let switch = arneb_scheduler::KillSwitch::new();
        tracker.set_canceller(&id, switch.canceller(|_| {}));
        assert!(matches!(
            tracker.kill_query(&id, "root").await.unwrap(),
            KillOutcome::Killed { .. }
        ));
        let out = switch
            .guard(async { panic!("the killed work ran") as Result<QueryOutput, TrinoError> })
            .await;
        assert!(out.is_none());
        assert_eq!(
            tracker.get_query(&id).unwrap().state,
            TrackerState::Cancelled
        );
        let phase = ended_phase(&tracker, &id, Err(TrinoError::internal("boom")));
        assert!(matches!(phase, Phase::Failed(e) if e.name == "USER_CANCELED"));
    }

    /// The Trino side of tracked query `tracker_id`, running its SQL.
    fn test_query(tracker: &QueryTracker, tracker_id: QueryId) -> Arc<Query> {
        let sql = tracker.get_query(&tracker_id).unwrap().sql;
        let session = ClientSession::from_headers(&HeaderMap::new(), None);
        Arc::new(Query {
            id: "q".into(),
            slug: "s".into(),
            reads: only_reads(&session, &sql),
            sql,
            user: "bob".into(),
            source: None,
            client_info: None,
            family: session::HeaderFamily::Trino,
            created: Instant::now(),
            inner: Mutex::new(Inner {
                phase: Phase::Running,
                next_token: 1,
                last: None,
                last_access: Instant::now(),
                ended: None,
            }),
            done: watch::channel(false).0,
            abort: Mutex::new(None),
            switch: arneb_scheduler::KillSwitch::new(),
            tracker_id,
            queued: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// A client cancel (or the reaper) that lands after the work returned,
    /// before `complete` ran, leaves the outcome to `complete`: an INSERT
    /// that committed is not reported USER_CANCELED.
    #[tokio::test]
    async fn a_client_cancel_after_the_work_returned_keeps_the_result() {
        let tracker = Arc::new(QueryTracker::new());
        let tracker_id = tracker.create_query_from("INSERT INTO t".into(), "trino", None);
        let query = test_query(&tracker, tracker_id);
        let result = query.switch.guard(async { Ok(rows()) }).await.unwrap();
        cancel_query(&tracker, &query, TrinoError::canceled(), "client")
            .await
            .unwrap();
        assert!(matches!(query.inner.lock().unwrap().phase, Phase::Running));
        assert_eq!(
            tracker.get_query(&tracker_id).unwrap().state,
            TrackerState::Queued
        );
        let phase = ended_phase(&tracker, &tracker_id, result);
        assert!(matches!(phase, Phase::Finished(_)));
        assert_eq!(
            tracker.get_query(&tracker_id).unwrap().state,
            TrackerState::Finished
        );
    }

    /// A client cancel (nextUri DELETE, `/v1/query` kill, the reaper) that
    /// lands while the poll completing an INSERT is in flight waits for that
    /// poll and is not applied: the committed write is reported FINISHED,
    /// never USER_CANCELED. (The poll blocks briefly so the cancel, on
    /// another worker, arrives inside it.)
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_client_cancel_during_the_completing_poll_keeps_the_result() {
        let tracker = Arc::new(QueryTracker::new());
        let tracker_id = tracker.create_query_from("INSERT INTO t".into(), "trino", None);
        let query = test_query(&tracker, tracker_id);
        let (t, q) = (Arc::clone(&tracker), Arc::clone(&query));
        let cancel = Arc::new(Mutex::new(None));
        let slot = Arc::clone(&cancel);
        let result = query
            .switch
            .guard(async move {
                let handle = tokio::spawn(async move {
                    cancel_query(&t, &q, TrinoError::canceled(), "client DELETE")
                        .await
                        .unwrap();
                });
                std::thread::sleep(Duration::from_millis(200));
                assert!(!handle.is_finished(), "the cancel waits for the poll");
                *slot.lock().unwrap() = Some(handle);
                Ok(rows())
            })
            .await
            .unwrap();
        let handle = cancel.lock().unwrap().take().unwrap();
        handle.await.unwrap();
        // What `complete` does: the phase is still running, so it records
        // the real result.
        let mut inner = query.inner.lock().unwrap();
        assert!(matches!(inner.phase, Phase::Running), "relabelled");
        inner.phase = ended_phase(&tracker, &tracker_id, result);
        assert!(matches!(inner.phase, Phase::Finished(_)));
        drop(inner);
        assert_eq!(
            tracker.get_query(&tracker_id).unwrap().state,
            TrackerState::Finished
        );
    }

    /// A client cancel that lands during the poll completing the work, in
    /// the order production runs it: the statement task records the result
    /// (`complete`) in that same poll, before the woken cancel resumes. The
    /// cancel must not relabel the completed query. Deterministic: the
    /// cancel is polled once inside the work's poll, so its kill is pending
    /// there, and resumed only after `complete`.
    #[tokio::test]
    async fn a_cancel_resuming_after_complete_keeps_the_result() {
        let tracker = Arc::new(QueryTracker::new());
        let tracker_id = tracker.create_query_from("SELECT n".into(), "trino", None);
        let query = test_query(&tracker, tracker_id);
        let (t, q) = (Arc::clone(&tracker), Arc::clone(&query));
        let cancel = Arc::new(Mutex::new(None));
        let slot = Arc::clone(&cancel);
        let task = {
            let (tracker, query) = (Arc::clone(&tracker), Arc::clone(&query));
            async move {
                let work = async move {
                    let mut c: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> =
                        Box::pin(async move {
                            let paging = q.is_paging();
                            cancel_now(&t, &q, TrinoError::canceled(), "client DELETE", paging)
                                .await
                        });
                    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
                    assert!(c.as_mut().poll(&mut cx).is_pending(), "the kill is pending");
                    *slot.lock().unwrap() = Some(c);
                    Ok(rows())
                };
                // The statement task: guard, then `complete`, in one poll.
                if let Some(result) = query.switch.guard(work).await {
                    let mut inner = query.inner.lock().unwrap();
                    if matches!(inner.phase, Phase::Running) {
                        inner.phase = ended_phase(&tracker, &query.tracker_id, result);
                    }
                }
            }
        };
        task.await;
        assert!(matches!(
            query.inner.lock().unwrap().phase,
            Phase::Finished(_)
        ));
        let c = cancel.lock().unwrap().take().unwrap();
        c.await;
        assert!(
            matches!(query.inner.lock().unwrap().phase, Phase::Finished(_)),
            "the cancel relabelled a completed query"
        );
        assert_eq!(
            tracker.get_query(&tracker_id).unwrap().state,
            TrackerState::Finished
        );
    }

    /// A cancel of a query paging out its results (it completed before the
    /// cancel) still stops it: USER_CANCELED.
    #[tokio::test]
    async fn a_cancel_while_paging_stops_the_query() {
        let tracker = Arc::new(QueryTracker::new());
        let tracker_id = tracker.create_query_from("SELECT n".into(), "trino", None);
        let query = test_query(&tracker, tracker_id);
        let result = query.switch.guard(async { Ok(rows()) }).await.unwrap();
        {
            let mut inner = query.inner.lock().unwrap();
            inner.phase = ended_phase(&tracker, &tracker_id, result);
        }
        cancel_query(&tracker, &query, TrinoError::canceled(), "client DELETE")
            .await
            .unwrap();
        assert!(matches!(
            &query.inner.lock().unwrap().phase,
            Phase::Failed(e) if e.name == "USER_CANCELED"
        ));
    }

    /// A write that finished keeps its result when a cancel (client
    /// `DELETE`, `/v1/query` kill, the reaper) lands before the client
    /// fetched its update count: only a read pages, so the cancel is not
    /// applied and the reaper does not count the query as active.
    #[tokio::test]
    async fn a_cancel_of_a_finished_write_keeps_its_result() {
        let tracker = Arc::new(QueryTracker::new());
        let tracker_id =
            tracker.create_query_from("INSERT INTO t VALUES (1)".into(), "trino", None);
        let query = test_query(&tracker, tracker_id);
        let result = query.switch.guard(async { Ok(rows()) }).await.unwrap();
        {
            let mut inner = query.inner.lock().unwrap();
            inner.phase = ended_phase(&tracker, &tracker_id, result);
            assert!(!query.active(&inner), "a finished write is not paging");
        }
        cancel_query(&tracker, &query, TrinoError::canceled(), "client DELETE")
            .await
            .unwrap();
        assert!(
            matches!(query.inner.lock().unwrap().phase, Phase::Finished(_)),
            "the cancel relabelled a committed write"
        );
        assert_eq!(
            tracker.get_query(&tracker_id).unwrap().state,
            TrackerState::Finished
        );
    }

    /// Whether a cancel stops a paging query is decided when the cancel
    /// arrives: one that arrives while a SELECT runs, whose kill is pending
    /// on the poll that completes it, does not stop it once it reads as
    /// paging (its task runs only after `complete`).
    #[tokio::test]
    async fn a_cancel_decides_paging_when_it_arrives() {
        let tracker = Arc::new(QueryTracker::new());
        let tracker_id = tracker.create_query_from("SELECT n".into(), "trino", None);
        let query = test_query(&tracker, tracker_id);
        let (t, q) = (Arc::clone(&tracker), Arc::clone(&query));
        let cancel = Arc::new(Mutex::new(None));
        let slot = Arc::clone(&cancel);
        let work = async move {
            // Arrives inside the completing poll; its task runs later.
            *slot.lock().unwrap() = Some(cancel_query(
                &t,
                &q,
                TrinoError::canceled(),
                "client DELETE",
            ));
            Ok(rows())
        };
        if let Some(result) = query.switch.guard(work).await {
            let mut inner = query.inner.lock().unwrap();
            inner.phase = ended_phase(&tracker, &tracker_id, result);
            assert!(query.paging(&inner));
        }
        let handle = cancel.lock().unwrap().take().unwrap();
        handle.await.unwrap();
        assert!(
            matches!(query.inner.lock().unwrap().phase, Phase::Finished(_)),
            "the cancel relabelled a completed query"
        );
    }

    /// A client cancel during a poll that does not complete the work stops
    /// it once that poll ends: USER_CANCELED, and the work is never polled
    /// again.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_client_cancel_during_a_poll_stops_the_work() {
        let tracker = Arc::new(QueryTracker::new());
        let tracker_id = tracker.create_query_from("SELECT 1".into(), "trino", None);
        let query = test_query(&tracker, tracker_id);
        let (t, q) = (Arc::clone(&tracker), Arc::clone(&query));
        let out = query
            .switch
            .guard(async move {
                tokio::spawn(async move {
                    cancel_query(&t, &q, TrinoError::canceled(), "client DELETE")
                        .await
                        .unwrap();
                });
                std::thread::sleep(Duration::from_millis(200));
                std::future::pending::<Result<QueryOutput, TrinoError>>().await
            })
            .await;
        assert!(out.is_none());
        for _ in 0..200 {
            if matches!(&query.inner.lock().unwrap().phase, Phase::Failed(e) if e.name == "USER_CANCELED")
            {
                assert_eq!(
                    tracker.get_query(&tracker_id).unwrap().state,
                    TrackerState::Cancelled
                );
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("the cancel never stopped the query");
    }

    #[test]
    fn an_unkilled_query_finishes_with_its_rows() {
        let tracker = QueryTracker::new();
        let id = tracker.create_query_from("SELECT n".into(), "trino", None);
        assert!(matches!(
            ended_phase(&tracker, &id, Ok(rows())),
            Phase::Finished(_)
        ));
        let info = tracker.get_query(&id).unwrap();
        assert_eq!(info.state, TrackerState::Finished);
        assert_eq!(info.rows, Some(2));
    }
}
