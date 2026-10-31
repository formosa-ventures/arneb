//! MCP (Model Context Protocol) endpoint: `/mcp` on the Web port.
//!
//! Stateless streamable HTTP (no MCP sessions, JSON responses): every POST
//! is independent. Authentication is the Web layer's
//! [`authenticate`](super::auth::authenticate) middleware, which wraps the
//! mount; a tool reads the resulting [`Identity`] from the request parts.

mod tools;

use std::sync::Arc;

use axum::http::{request::Parts, StatusCode};
use rmcp::{
    handler::server::router::tool::ToolRouter,
    model::{CallToolResult, Implementation, ServerCapabilities, ServerConfig},
    service::{RequestContext, RoleServer},
    tool_handler,
    transport::streamable_http_server::{
        session::never::NeverSessionManager, StreamableHttpServerConfig, StreamableHttpService,
    },
    ErrorData, ServerHandler,
};
use serde::Serialize;

use super::auth::Identity;
use super::error::{ApiError, Envelope};
use super::WebState;

/// Resolved `[mcp]` settings carried by [`WebState`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpSettings {
    /// Accepted `Host` values; `None` = the default for the auth mode,
    /// `["*"]` = any.
    pub allowed_hosts: Option<Vec<String>>,
    /// Page size of tool results when the caller gives no `max_bytes`.
    pub default_page_bytes: usize,
}

impl From<&crate::config::McpConfig> for McpSettings {
    fn from(c: &crate::config::McpConfig) -> Self {
        Self {
            allowed_hosts: c.allowed_hosts.clone(),
            default_page_bytes: c.default_page_bytes,
        }
    }
}

/// Placeholder agent guidance; the final copy lands with the instructions task.
const INSTRUCTIONS: &str = "Arneb is a SQL engine. Discover catalogs, schemas and tables, \
    describe a table, explain a query, submit it, poll its status and fetch result pages. \
    Tool errors carry a code, a hint and, for typos, candidates; read them and retry.";

/// The MCP handler: one per request (the service is stateless).
#[derive(Clone)]
pub struct ArnebMcp {
    pub(crate) state: WebState,
    tool_router: ToolRouter<Self>,
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for ArnebMcp {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new("arneb", env!("CARGO_PKG_VERSION")))
            .with_instructions(INSTRUCTIONS)
    }
}

/// The `Host` values `/mcp` accepts (empty = any), per `[mcp] allowed_hosts`
/// or its default: any host under password auth, loopback under `none`.
fn resolve_allowed_hosts(settings: &McpSettings, password_auth: bool) -> Vec<String> {
    match &settings.allowed_hosts {
        Some(hosts) if hosts.iter().any(|h| h.trim() == "*") => Vec::new(),
        Some(hosts) => hosts.clone(),
        None if password_auth => Vec::new(),
        None => vec!["localhost".into(), "127.0.0.1".into(), "::1".into()],
    }
}

/// The `/mcp` service for `state` (`state.mcp` must be `Some`).
pub(super) fn mcp_service(state: WebState) -> StreamableHttpService<ArnebMcp, NeverSessionManager> {
    let settings = state.mcp.clone().unwrap_or(McpSettings {
        allowed_hosts: None,
        default_page_bytes: 32 * 1024,
    });
    let hosts = resolve_allowed_hosts(&settings, state.credentials.is_some());
    let mut config = if hosts.is_empty() {
        StreamableHttpServerConfig::default().disable_allowed_hosts()
    } else {
        StreamableHttpServerConfig::default().with_allowed_hosts(hosts)
    };
    config.legacy_session_mode = false;
    config.json_response = true;
    StreamableHttpService::new(
        move || {
            Ok(ArnebMcp {
                state: state.clone(),
                tool_router: ArnebMcp::tool_router(),
            })
        },
        Arc::new(NeverSessionManager::default()),
        config,
    )
}

/// `/mcp` when it is not served (disabled, or no SQL on this node).
pub(super) async fn not_served() -> StatusCode {
    StatusCode::NOT_FOUND
}

/// The request's [`Identity`], set by the Web auth middleware. A missing
/// one is an internal error, never an anonymous caller.
pub(super) fn identity(ctx: &RequestContext<RoleServer>) -> Result<Identity, ErrorData> {
    ctx.extensions
        .get::<Parts>()
        .and_then(|parts| parts.extensions.get::<Identity>())
        .cloned()
        .ok_or_else(|| {
            tracing::error!("MCP request reached a tool without an authenticated identity");
            ErrorData::internal_error("request identity is unavailable", None)
        })
}

/// A successful tool result: `body` as `structuredContent`.
pub(super) fn ok<T: Serialize>(body: &T) -> Result<CallToolResult, ErrorData> {
    let value = serde_json::to_value(body)
        .map_err(|e| ErrorData::internal_error(format!("result is not serializable: {e}"), None))?;
    Ok(CallToolResult::structured(value))
}

/// A failed tool result (`isError: true`) whose `structuredContent` is the
/// same `{"error": …}` envelope the HTTP routes send.
#[allow(dead_code)] // used by the tools added after `whoami`
pub(super) fn tool_error(e: ApiError) -> CallToolResult {
    match serde_json::to_value(Envelope { error: &e }) {
        Ok(value) => CallToolResult::structured_error(value),
        Err(_) => CallToolResult::structured_error(
            serde_json::json!({"error": {"code": "internal", "message": e.message}}),
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arneb_protocol::{CredentialSource, ScramVerifier};
    use axum::body::Body;
    use axum::http::{header, HeaderMap, Method, StatusCode};
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine as _;
    use serde_json::{json, Value};
    use tower::ServiceExt as _;

    use super::*;
    use crate::web::{build_router, SessionStore};

    fn mcp_state(password: bool, mcp: Option<McpSettings>) -> WebState {
        let store = Arc::new(arneb_state::StateStore::open(None).unwrap());
        for (user, pw) in [("alice", "secret"), ("bob", "hunter2")] {
            let v = ScramVerifier::from_password(pw, b"0123456789abcdef", 4096);
            store.seed_users(&[(user.into(), v)], false).unwrap();
        }
        let credentials: Option<Arc<dyn CredentialSource>> =
            password.then(|| store.clone() as Arc<dyn CredentialSource>);
        WebState {
            query_tracker: Arc::new(arneb_scheduler::QueryTracker::new()),
            node_registry: arneb_scheduler::NodeRegistry::new(30),
            start_time: std::time::Instant::now(),
            role: "standalone".into(),
            admission: None,
            max_queued_queries: 8,
            memory_pool: Arc::new(arneb_common::memory_pool::GreedyMemoryPool::new(1 << 20)),
            memory_limit_bytes: None,
            basic: credentials
                .clone()
                .map(|c| arneb_protocol::BasicAuthenticator::new(c).unwrap()),
            credentials,
            state: password.then_some(store),
            access_rules: None,
            metrics_auth: false,
            sessions: SessionStore::new(),
            agent: None,
            mcp,
        }
    }

    fn default_settings() -> Option<McpSettings> {
        Some(McpSettings {
            allowed_hosts: None,
            default_page_bytes: 32768,
        })
    }

    fn basic(user: &str, password: &str) -> String {
        format!("Basic {}", STANDARD.encode(format!("{user}:{password}")))
    }

    const INIT: &str = r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"t","version":"1"}}}"#;

    fn whoami_body() -> String {
        json!({"jsonrpc":"2.0","id":3,"method":"tools/call",
               "params":{"name":"whoami","arguments":{}}})
        .to_string()
    }

    /// `POST /mcp` with the streamable-HTTP headers.
    async fn rpc(
        router: axum::Router,
        auth: Option<&str>,
        host: &str,
        body: &str,
    ) -> (StatusCode, HeaderMap, Value) {
        let mut req = axum::http::Request::builder()
            .method(Method::POST)
            .uri("/mcp")
            .header(header::HOST, host)
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::ACCEPT, "application/json, text/event-stream")
            .header("mcp-protocol-version", "2025-06-18");
        if let Some(a) = auth {
            req = req.header(header::AUTHORIZATION, a);
        }
        let resp = router
            .oneshot(req.body(Body::from(body.to_owned())).unwrap())
            .await
            .unwrap();
        let (status, headers) = (resp.status(), resp.headers().clone());
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        (
            status,
            headers,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    #[tokio::test]
    async fn initialize_reports_arneb() {
        let router = build_router(mcp_state(false, default_settings()));
        let (status, _, body) = rpc(router, None, "127.0.0.1:6432", INIT).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let r = &body["result"];
        assert_eq!(r["serverInfo"]["name"], "arneb");
        assert_eq!(r["serverInfo"]["version"], env!("CARGO_PKG_VERSION"));
        assert!(r["capabilities"]["tools"].is_object(), "{r}");
        assert!(!r["instructions"].as_str().unwrap_or("").is_empty());
    }

    #[tokio::test]
    async fn mcp_requires_credentials_under_password_auth() {
        let router = build_router(mcp_state(true, default_settings()));
        let (status, headers, _) = rpc(router.clone(), None, "arneb.example.com", INIT).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(headers[header::WWW_AUTHENTICATE], r#"Bearer realm="arneb""#);
        let (status, _, _) = rpc(
            router,
            Some(&basic("alice", "wrong")),
            "arneb.example.com",
            INIT,
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }

    async fn whoami(router: axum::Router, auth: &str) -> Value {
        let (status, _, body) = rpc(router, Some(auth), "arneb.example.com", &whoami_body()).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        body["result"]["structuredContent"].clone()
    }

    #[tokio::test]
    async fn whoami_runs_as_the_token_user() {
        let state = mcp_state(true, default_settings());
        let store = state.state.clone().unwrap();
        let token = store.issue_token("alice", None, None).unwrap().token;
        let session = state.sessions.create("alice").unwrap();
        let router = build_router(state);

        let w = whoami(router.clone(), &format!("Bearer {token}")).await;
        assert_eq!(
            (w["user"].as_str(), w["via"].as_str(), w["auth"].as_str()),
            (Some("alice"), Some("token"), Some("password"))
        );
        let w = whoami(router.clone(), &basic("alice", "secret")).await;
        assert_eq!(
            (w["user"].as_str(), w["via"].as_str()),
            (Some("alice"), Some("basic"))
        );
        let w = whoami(router, &format!("Bearer {session}")).await;
        assert_eq!(
            (w["user"].as_str(), w["via"].as_str()),
            (Some("alice"), Some("session"))
        );
        assert_eq!(w["version"], env!("CARGO_PKG_VERSION"));
        assert_eq!(w["role"], "standalone");
    }

    #[tokio::test]
    async fn revoked_token_is_refused_on_next_call() {
        let state = mcp_state(true, default_settings());
        let store = state.state.clone().unwrap();
        let issued = store.issue_token("alice", None, None).unwrap();
        let router = build_router(state);
        let auth = format!("Bearer {}", issued.token);
        let (status, _, _) = rpc(router.clone(), Some(&auth), "h.example", &whoami_body()).await;
        assert_eq!(status, StatusCode::OK);
        store.revoke_token("alice", &issued.info.id).unwrap();
        let (status, _, _) = rpc(router, Some(&auth), "h.example", &whoami_body()).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn auth_none_uses_claimed_user() {
        let router = build_router(mcp_state(false, default_settings()));
        let claimed = |user: Option<&'static str>| {
            let router = router.clone();
            async move {
                let mut req = axum::http::Request::builder()
                    .method(Method::POST)
                    .uri("/mcp")
                    .header(header::HOST, "127.0.0.1:6432")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::ACCEPT, "application/json, text/event-stream")
                    .header("mcp-protocol-version", "2025-06-18");
                if let Some(u) = user {
                    req = req.header("x-arneb-user", u);
                }
                let resp = router
                    .oneshot(req.body(Body::from(whoami_body())).unwrap())
                    .await
                    .unwrap();
                assert_eq!(resp.status(), StatusCode::OK);
                let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
                    .await
                    .unwrap();
                let v: Value = serde_json::from_slice(&bytes).unwrap();
                v["result"]["structuredContent"].clone()
            }
        };
        let w = claimed(Some("bob")).await;
        assert_eq!(
            (w["user"].as_str(), w["via"].as_str(), w["auth"].as_str()),
            (Some("bob"), Some("unauthenticated"), Some("none"))
        );
        assert_eq!(claimed(None).await["user"], "anonymous");
    }

    #[tokio::test]
    async fn host_allow_list() {
        // Auth none, default: loopback only.
        let router = build_router(mcp_state(false, default_settings()));
        let (status, _, body) = rpc(router.clone(), None, "evil.example", &whoami_body()).await;
        assert_ne!(status, StatusCode::OK);
        assert!(body["result"].is_null(), "tool ran: {body}");
        let (status, _, _) = rpc(router, None, "127.0.0.1:6432", &whoami_body()).await;
        assert_eq!(status, StatusCode::OK);

        // Password auth, default: any host.
        let state = mcp_state(true, default_settings());
        let auth = basic("alice", "secret");
        let router = build_router(state);
        let (status, _, _) = rpc(router, Some(&auth), "arneb.example.com", &whoami_body()).await;
        assert_eq!(status, StatusCode::OK);

        // Password auth with an explicit list.
        let state = mcp_state(
            true,
            Some(McpSettings {
                allowed_hosts: Some(vec!["db.internal".into()]),
                default_page_bytes: 32768,
            }),
        );
        let router = build_router(state);
        let (status, _, body) = rpc(
            router.clone(),
            Some(&auth),
            "arneb.example.com",
            &whoami_body(),
        )
        .await;
        assert_ne!(status, StatusCode::OK);
        assert!(body["result"].is_null(), "tool ran: {body}");
        let (status, _, _) = rpc(router, Some(&auth), "db.internal:6432", &whoami_body()).await;
        assert_eq!(status, StatusCode::OK);
    }

    #[tokio::test]
    async fn disabled_mcp_is_not_mounted() {
        let router = build_router(mcp_state(false, None));
        let (status, _, body) = rpc(router, None, "127.0.0.1:6432", INIT).await;
        assert!(
            status == StatusCode::NOT_FOUND || status == StatusCode::METHOD_NOT_ALLOWED,
            "{status}"
        );
        assert!(body["jsonrpc"].is_null(), "{body}");
    }

    #[tokio::test]
    async fn tool_error_uses_the_http_envelope() {
        use crate::web::error::ErrorCode;
        let e = ApiError::new(ErrorCode::TableNotFound, "no table").with_hint("list_tables");
        let r = tool_error(e);
        assert_eq!(r.is_error, Some(true));
        assert_eq!(
            r.structured_content.unwrap(),
            json!({"error": {"code": "table_not_found", "message": "no table", "hint": "list_tables"}})
        );
    }
}
