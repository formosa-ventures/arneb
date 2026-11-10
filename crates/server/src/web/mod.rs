//! Web UI module: REST API + embedded frontend.

mod admission;
mod agent;
mod api;
mod audit;
pub mod auth;
mod catalogs;
pub mod error;
mod frontend;
mod manage;
pub mod mcp;
mod metrics;
mod oauth_metadata;
pub mod privilege;
mod reseed;
mod rules;
pub mod session;
mod settings;
mod users;
pub(crate) use users::is_valid_user_name;
mod v2;
mod workers;

pub use workers::restore_worker_cordons;

pub(crate) use audit::{flush_audit, spawn_audit_retry, AUDIT_RETRY_INTERVAL};
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
    /// Configured `[admission] max_running_queries` (after
    /// `ARNEB_MAX_RUNNING_QUERIES`): the seed `DELETE /api/v2/admission`
    /// returns to.
    pub max_running_queries: usize,
    /// Configured `[admission] max_queued_queries`, reported when there is
    /// no controller (a worker), and the seed `DELETE /api/v2/admission`
    /// returns to.
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
    /// The live access rules, shared with pgwire, Trino, the agent API and
    /// [`WebState::authorizer`]; changed through `/api/v2/access-rules`.
    pub rules: arneb_catalog::RulesHandle,
    /// Serializes every runtime-config change (rules, users, user tokens,
    /// catalogs, settings, re-seed; runtime-config D10): held across the
    /// whole change, through the audit append.
    pub config_changes: Arc<tokio::sync::Mutex<()>>,
    /// The `--config` path the process started with (`None`: the default
    /// `./arneb.toml` when present, else the defaults); a re-seed reloads
    /// it with `AppConfig::load` (runtime-config D2, Ruling R3).
    pub config_path: Option<std::path::PathBuf>,
    /// The live external catalogs, changed through
    /// `/api/v2/catalog-configs` (runtime-config D7); `Some` on a
    /// coordinator or standalone server.
    pub catalogs: Option<Arc<crate::catalogs::LiveCatalogs>>,
    /// The observe and manage privilege layers and the kill rule.
    pub authorizer: privilege::Authorizer,
    /// `[web] metrics_auth`: require credentials for `/metrics`.
    pub metrics_auth: bool,
    /// HTTP Basic checker over `credentials` (cache + PBKDF2 limiter); `Some`
    /// exactly when `credentials` is. The Trino listener gets a `sibling` of it:
    /// the two ports share its PBKDF2 limiter and cache, not its wait queue.
    /// Token issuance takes its PBKDF2 slot from the same limiter.
    pub basic: Option<arneb_protocol::BasicAuthenticator>,
    /// Web sessions (bearer `arneb_s_…`), in memory.
    pub sessions: SessionStore,
    /// The agent query API service (`/api/v2` statements, discovery and
    /// explain); `None` on workers, where those routes answer `not_found`.
    pub agent: Option<arneb_protocol::agent::AgentQueryService>,
    /// Resolved `[mcp]` settings; `Some` exactly when `/mcp` is served
    /// (enabled, on a coordinator or standalone node).
    pub mcp: Option<mcp::McpSettings>,
    /// OAuth access-token validator for `/mcp`; `Some` exactly when
    /// `[oauth]` is enabled on a coordinator or standalone node.
    pub oauth: Option<Arc<crate::oauth::OAuthValidator>>,
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
    // Discovery for OAuth clients. The handler answers at the path derived
    // from `resource` and at the bare prefix; everything else under the
    // well-known prefix is a plain 404 (not the SPA), also when OAuth is off.
    let metadata_paths = [
        crate::oauth::METADATA_PATH.to_owned(),
        format!("{}/", crate::oauth::METADATA_PATH),
        format!("{}/{{*rest}}", crate::oauth::METADATA_PATH),
    ];
    let router = metadata_paths.iter().fold(router, |router, path| {
        if state.oauth.is_some() {
            router.route(path, axum::routing::get(oauth_metadata::metadata))
        } else {
            router.route(path, axum::routing::any(mcp::not_served))
        }
    });
    router
        .fallback(frontend::static_handler)
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            auth::authenticate,
        ))
        .with_state(state)
}
