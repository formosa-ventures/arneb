//! Web UI module: REST API + embedded frontend.

mod api;
mod frontend;

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
}

/// Build the axum Router with API and frontend routes.
pub fn build_router(state: WebState) -> Router {
    Router::new()
        .nest("/api/v1", api::api_routes())
        .fallback(frontend::static_handler)
        .with_state(state)
}
