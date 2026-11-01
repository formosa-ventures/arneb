//! OAuth 2.0 Protected Resource Metadata (RFC 9728) for `/mcp`, so a client
//! that got a `401` can find the authorization server.

use axum::extract::State;
use axum::http::{header, HeaderValue};
use axum::response::{IntoResponse, Response};
use axum::Json;

use super::WebState;

/// `GET /.well-known/oauth-protected-resource[/…]`. Mounted only when
/// `[oauth]` is enabled; public (outside `/api/`).
pub(super) async fn metadata(State(state): State<WebState>) -> Response {
    let Some(oauth) = &state.oauth else {
        return axum::http::StatusCode::NOT_FOUND.into_response();
    };
    let s = oauth.settings();
    let mut doc = serde_json::json!({
        "resource": s.resource,
        "authorization_servers": [s.issuer],
        "bearer_methods_supported": ["header"],
        "resource_name": "Arneb",
    });
    if !s.required_scopes.is_empty() {
        doc["scopes_supported"] = serde_json::json!(s.required_scopes);
    }
    let mut resp = Json(doc).into_response();
    let h = resp.headers_mut();
    h.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("max-age=300"),
    );
    h.insert(
        header::ACCESS_CONTROL_ALLOW_ORIGIN,
        HeaderValue::from_static("*"),
    );
    resp
}
