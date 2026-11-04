//! OAuth 2.0 Protected Resource Metadata (RFC 9728) for `/mcp`, so a client
//! that got a `401` can find the authorization server.

use axum::extract::State;
use axum::http::{header, HeaderValue, Uri};
use axum::response::{IntoResponse, Response};
use axum::Json;

use super::WebState;

/// `GET /.well-known/oauth-protected-resource[/…]`. Mounted only when
/// `[oauth]` is enabled; public (outside `/api/`). Answers at the path
/// derived from `resource` (RFC 9728 section 3.1), and at the bare
/// well-known path only when that path is exactly `/mcp` (see
/// [`serves_bare_path`]). Any other path is a plain `404`.
pub(super) async fn metadata(State(state): State<WebState>, uri: Uri) -> Response {
    let Some(oauth) = state.oauth.as_ref().filter(|o| {
        uri.path() == o.metadata_path()
            || (uri.path() == crate::oauth::METADATA_PATH && serves_bare_path(o.metadata_path()))
    }) else {
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

/// Whether the bare well-known path is answered too: only for a resource at
/// exactly `/mcp` on its origin, as a fallback for clients that probe only
/// the root. Its `resource` is not the root URL, so a strict RFC 9728
/// (section 3.3) client discards it and uses the derived path. Behind a
/// path prefix the host is shared, and answering the host-wide path would
/// claim discovery for every other resource on it.
fn serves_bare_path(metadata_path: &str) -> bool {
    metadata_path
        .strip_prefix(crate::oauth::METADATA_PATH)
        .is_some_and(|rest| rest == "/mcp")
}
