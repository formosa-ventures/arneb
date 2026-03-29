//! Web UI module: REST API + embedded frontend.

mod api;
mod frontend;

use std::sync::Arc;

use axum::Router;
use trino_scheduler::{NodeRegistry, QueryTracker};

/// Shared state for web API handlers.
#[derive(Clone)]
pub struct WebState {
    pub query_tracker: Arc<QueryTracker>,
    pub node_registry: NodeRegistry,
    pub start_time: std::time::Instant,
    pub role: String,
}

/// Build the axum Router with API and frontend routes.
pub fn build_router(state: WebState) -> Router {
    Router::new()
        .nest("/api/v1", api::api_routes())
        .fallback(frontend::static_handler)
        .with_state(state)
}
