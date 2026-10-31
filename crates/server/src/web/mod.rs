//! Web UI module: REST API + embedded frontend.

mod admission;
mod agent;
mod api;
mod audit;
pub mod auth;
pub mod error;
mod frontend;
mod manage;
pub mod mcp;
mod metrics;
pub mod privilege;
pub mod session;
mod v2;
mod workers;

pub use workers::restore_worker_cordons;

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
    /// Admission controller shared with pgwire, Trino and the agent API;
    /// `Some` on every node that serves SQL (coordinator / standalone),
    /// `None` on workers.
    pub admission: Option<arneb_scheduler::AdmissionController>,
    /// Serializes runtime admission changes (manage-api D9): held from
    /// reading the current limits to applying the new ones, so a change's
    /// preview, audit `from` and result see one state.
    pub admission_changes: Arc<tokio::sync::Mutex<()>>,
    /// Serializes worker cordon, drain and uncordon (manage-api D8): held
    /// across the whole change (preview, persist, registry update, audit
    /// append), so the audit order is the apply order.
    pub cordon_changes: Arc<tokio::sync::Mutex<()>>,
    /// Configured `[admission] max_queued_queries`, reported when there is
    /// no controller (a worker).
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
    /// The audit log: next to the state file, or in memory (always empty
    /// under `[auth] type = "none"`, where nothing is audited).
    pub audit: Arc<arneb_state::AuditLog>,
    /// Access rules shared with pgwire and Trino.
    pub access_rules: Option<Arc<arneb_catalog::AccessRules>>,
    /// The observe and manage privilege layers and the kill rule.
    pub authorizer: privilege::Authorizer,
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

/// An empty in-memory audit log, for `WebState`s built in tests.
#[cfg(test)]
pub(crate) fn test_audit_log() -> Arc<arneb_state::AuditLog> {
    let settings = arneb_state::AuditSettings {
        max_entries: 1000,
        retention_days: 0,
    };
    Arc::new(arneb_state::AuditLog::open(None, settings).expect("in-memory audit log"))
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
