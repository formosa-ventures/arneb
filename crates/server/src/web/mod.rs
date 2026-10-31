//! Web UI module: REST API + embedded frontend.

mod agent;
mod api;
pub mod auth;
pub mod error;
mod frontend;
pub mod mcp;
mod metrics;
pub mod session;
mod v2;

pub use session::SessionStore;

use std::sync::Arc;

use arneb_scheduler::{NodeRegistry, QueryTracker};
use axum::Router;

/// Shared state for web API handlers.
#[derive(Clone)]
pub struct WebState {
    pub query_tracker: Arc<QueryTracker>,
    pub node_registry: NodeRegistry,
    pub start_time: std::time::Instant,
    pub role: String,
    /// Admission controller shared with pgwire and Trino (coordinator /
    /// standalone); `None` on workers or when admission is off.
    pub admission: Option<arneb_scheduler::AdmissionController>,
    /// Configured `[admission] max_queued_queries`, reported when there is
    /// no controller (`max_running_queries = 0`, i.e. unlimited).
    pub max_queued_queries: usize,
    /// The node memory pool, for `/metrics`.
    pub memory_pool: Arc<dyn arneb_common::memory_pool::MemoryPool>,
    /// The pool's budget in bytes (`None` = unbounded), for `/metrics`.
    pub memory_limit_bytes: Option<u64>,
    /// Credential check for the API and Web UI; `Some` only under
    /// `[auth] type = "password"` (the state store).
    pub credentials: Option<Arc<dyn arneb_protocol::CredentialSource>>,
    /// The coordinator state store; `Some` only under password auth.
    pub state: Option<Arc<arneb_state::StateStore>>,
    /// Access rules shared with pgwire and Trino.
    pub access_rules: Option<Arc<arneb_catalog::AccessRules>>,
    /// `[web] metrics_auth`: require credentials for `/metrics`.
    pub metrics_auth: bool,
    /// HTTP Basic checker over `credentials` (cache + PBKDF2 limiter); `Some`
    /// exactly when `credentials` is. The server passes the same instance to
    /// the Trino listener, so the two ports share its limiter and cache.
    pub basic: Option<arneb_protocol::BasicAuthenticator>,
    /// Web sessions (bearer `arneb_s_…`), in memory.
    pub sessions: SessionStore,
    /// The agent query API service (`/api/v2` statements, discovery and
    /// explain); `None` on workers, where those routes answer `not_found`.
    pub agent: Option<arneb_protocol::agent::AgentQueryService>,
    /// Resolved `[mcp]` settings; `Some` exactly when `/mcp` is served
    /// (enabled, on a coordinator or standalone node).
    pub mcp: Option<mcp::McpSettings>,
}

/// Build the axum Router with API and frontend routes.
///
/// [`auth::authenticate`] wraps the whole router, so every route added
/// here (including future `/api/*` nests) is covered; which paths are
/// public is decided in one place, [`auth::requires_auth`].
pub fn build_router(state: WebState) -> Router {
    let router = Router::new()
        .nest("/api/v1", api::api_routes())
        .nest("/api/v2", v2::routes())
        .route("/metrics", axum::routing::get(metrics::metrics));
    // MCP is mounted before the auth layer below, so that wraps it too.
    let router = if let Some(settings) = &state.mcp {
        router.nest_service("/mcp", mcp::mcp_service(state.clone(), settings))
    } else {
        // Not served (disabled, or a node without SQL): a plain 404 rather
        // than the SPA fallback's index.html.
        router
            .route("/mcp", axum::routing::any(mcp::not_served))
            .route("/mcp/{*rest}", axum::routing::any(mcp::not_served))
    };
    router
        .fallback(frontend::static_handler)
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            auth::authenticate,
        ))
        .with_state(state)
}
