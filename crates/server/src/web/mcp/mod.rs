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

/// What the agent reads on `initialize`: the intended workflow (at most
/// 1500 characters; a test enforces it).
const INSTRUCTIONS: &str = "\
Arneb is a distributed SQL engine that queries many data sources through catalogs. \
Names are catalog.schema.table.

Workflow:
1. Discover: list_catalogs, list_schemas, list_tables.
2. Learn the table: describe_table for columns and types, sample_table for a few real rows.
3. Check the query: explain_query plans it without running it.
4. Run it: submit_query with wait_ms (up to 10000) so short queries return finished.
5. If the state is queued or running, call get_query_status (with wait_ms) until it is finished, failed or cancelled.
6. Read rows: fetch_results with the cursor, repeating while next_cursor is not null.
7. Stop a statement you no longer need with cancel_query.

Limits: a statement keeps at most max_rows rows (default 1000, ceiling 10000) and reports truncated when more existed. Pages are small by default; ask for more with max_bytes. Long cells and SQL text are cut. Listings take limit and report truncated.

Errors: a failed tool call comes back as an error result with a code and a hint. Read the hint and any candidates (close matches for a misspelled table or column), fix the call, and retry. A SQL error after submit is a status with state failed, not an error result.

Queries run as your user and see only what your user may see.";

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
/// `*` among the entries means any host.
pub fn resolve_allowed_hosts(settings: &McpSettings, password_auth: bool) -> Vec<String> {
    match &settings.allowed_hosts {
        Some(hosts) if hosts.iter().any(|h| h.trim() == "*") => Vec::new(),
        Some(hosts) => hosts.clone(),
        None if password_auth => Vec::new(),
        None => vec!["localhost".into(), "127.0.0.1".into(), "::1".into()],
    }
}

/// The `/mcp` service for `state`, configured by the resolved `settings`.
pub(super) fn mcp_service(
    state: WebState,
    settings: &McpSettings,
) -> StreamableHttpService<ArnebMcp, NeverSessionManager> {
    let hosts = resolve_allowed_hosts(settings, state.credentials.is_some());
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

    #[test]
    fn star_among_other_hosts_means_any() {
        let s = |h: &[&str]| McpSettings {
            allowed_hosts: Some(h.iter().map(|x| x.to_string()).collect()),
            default_page_bytes: 1,
        };
        assert!(resolve_allowed_hosts(&s(&["a.example", "*"]), false).is_empty());
        assert_eq!(
            resolve_allowed_hosts(&s(&["a.example"]), true),
            ["a.example"]
        );
        assert!(resolve_allowed_hosts(
            &McpSettings {
                allowed_hosts: None,
                default_page_bytes: 1
            },
            true
        )
        .is_empty());
    }

    #[tokio::test]
    async fn mcp_subpaths_need_credentials_and_get_is_not_served() {
        let router = build_router(mcp_state(true, default_settings()));
        for path in ["/mcp/x", "/mcp/"] {
            let req = axum::http::Request::builder()
                .method(Method::POST)
                .uri(path)
                .header(header::HOST, "h.example")
                .body(Body::empty())
                .unwrap();
            let resp = router.clone().oneshot(req).await.unwrap();
            assert_eq!(resp.status(), StatusCode::UNAUTHORIZED, "{path}");
        }
        for (accept, expected) in [
            (Some("text/event-stream"), StatusCode::METHOD_NOT_ALLOWED),
            (None, StatusCode::METHOD_NOT_ALLOWED),
        ] {
            let mut req = axum::http::Request::builder()
                .method(Method::GET)
                .uri("/mcp")
                .header(header::HOST, "h.example")
                .header(header::AUTHORIZATION, basic("alice", "secret"));
            if let Some(a) = accept {
                req = req.header(header::ACCEPT, a);
            }
            let resp = router
                .clone()
                .oneshot(req.body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(resp.status(), expected, "Accept: {accept:?}");
            assert_eq!(resp.headers()[header::ALLOW], "POST");
            let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
                .await
                .unwrap();
            let v: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
            assert!(v["result"].is_null(), "{v}");
        }
    }

    #[tokio::test]
    async fn disabled_mcp_is_not_mounted() {
        let router = build_router(mcp_state(false, None));
        let (status, _, body) = rpc(router, None, "127.0.0.1:6432", INIT).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
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
    /// A `tools/call` with `auth`; the JSON-RPC `result`.
    async fn call_tool(router: axum::Router, auth: &str, name: &str, args: Value) -> Value {
        let body = json!({"jsonrpc":"2.0","id":9,"method":"tools/call",
                          "params":{"name":name,"arguments":args}})
        .to_string();
        let (status, _, v) = rpc(router, Some(auth), "arneb.example.com", &body).await;
        assert_eq!(status, StatusCode::OK, "{v}");
        v["result"].clone()
    }

    fn with_rules(mut state: WebState, json: &str) -> WebState {
        state.access_rules = Some(Arc::new(
            arneb_catalog::AccessRules::from_json(json).unwrap(),
        ));
        state
    }

    #[tokio::test]
    async fn list_queries_follows_view_rules() {
        let state = with_rules(
            mcp_state(true, default_settings()),
            r#"{"queries": [{"user": "alice|bob", "allow": ["execute"]}]}"#,
        );
        let a = state.query_tracker.create_query_from(
            "SELECT 'a'".into(),
            "pgwire",
            Some("alice".into()),
        );
        let b = state.query_tracker.create_query_from(
            "SELECT 'b'".into(),
            "pgwire",
            Some("bob".into()),
        );
        let router = build_router(state);
        let alice = basic("alice", "secret");
        let r = call_tool(router.clone(), &alice, "list_queries", json!({})).await;
        let q = r["structuredContent"]["queries"].as_array().unwrap();
        assert_eq!(q.len(), 1, "{r}");
        assert_eq!(q[0]["query_id"], a.to_string());
        let r = call_tool(
            router.clone(),
            &alice,
            "get_query",
            json!({"id": b.to_string()}),
        )
        .await;
        assert_eq!(r["isError"], true);
        assert_eq!(r["structuredContent"]["error"]["code"], "not_found");
        let r = call_tool(router, &alice, "get_query", json!({"id": a.to_string()})).await;
        assert_ne!(r["isError"], true, "{r}");
    }

    #[tokio::test]
    async fn token_tools_round_trip() {
        let router = build_router(mcp_state(true, default_settings()));
        let alice = basic("alice", "secret");
        let bob = basic("bob", "hunter2");
        let r = call_tool(
            router.clone(),
            &alice,
            "create_token",
            json!({"name": "ci", "expires_in_days": 1}),
        )
        .await;
        assert_ne!(r["isError"], true, "{r}");
        let issued = &r["structuredContent"];
        let token = issued["token"].as_str().unwrap().to_owned();
        let id = issued["id"].as_str().unwrap().to_owned();
        assert!(token.starts_with("arneb_"));

        let r = call_tool(router.clone(), &alice, "list_tokens", json!({})).await;
        let tokens = r["structuredContent"]["tokens"].as_array().unwrap();
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0]["name"], "ci");
        assert!(tokens[0].get("token").is_none());

        let bearer = format!("Bearer {token}");
        let (status, _, _) = rpc(router.clone(), Some(&bearer), "h.example", &whoami_body()).await;
        assert_eq!(status, StatusCode::OK);

        // Another user's id is not_found and leaves the token alive.
        let r = call_tool(router.clone(), &bob, "revoke_token", json!({"id": id})).await;
        assert_eq!(r["isError"], true);
        assert_eq!(r["structuredContent"]["error"]["code"], "not_found");

        let r = call_tool(router.clone(), &alice, "revoke_token", json!({"id": id})).await;
        assert_eq!(r["structuredContent"], json!({"revoked": id}));
        let (status, _, _) = rpc(router, Some(&bearer), "h.example", &whoami_body()).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn create_token_with_token_is_clamped() {
        let router = build_router(mcp_state(true, default_settings()));
        let r = call_tool(
            router.clone(),
            &basic("alice", "secret"),
            "create_token",
            json!({"expires_in_days": 1}),
        )
        .await;
        let parent = r["structuredContent"].clone();
        let bearer = format!("Bearer {}", parent["token"].as_str().unwrap());
        let r = call_tool(
            router,
            &bearer,
            "create_token",
            json!({"expires_in_days": 30}),
        )
        .await;
        assert_ne!(r["isError"], true, "{r}");
        let child = &r["structuredContent"];
        assert!(child["expires_ms"].as_u64().unwrap() <= parent["expires_ms"].as_u64().unwrap());
        assert!(child["hint"].as_str().unwrap().contains("clamped"));
    }

    #[tokio::test]
    async fn token_tools_are_auth_disabled_under_none() {
        let router = build_router(mcp_state(false, default_settings()));
        for (name, args) in [
            ("create_token", json!({})),
            ("list_tokens", json!({})),
            ("revoke_token", json!({"id": "0123456789abcdef"})),
        ] {
            let body = json!({"jsonrpc":"2.0","id":9,"method":"tools/call",
                              "params":{"name":name,"arguments":args}})
            .to_string();
            let (_, _, v) = rpc(router.clone(), None, "127.0.0.1:6432", &body).await;
            assert_eq!(v["result"]["isError"], true, "{name}: {v}");
            assert_eq!(
                v["result"]["structuredContent"]["error"]["code"], "auth_disabled",
                "{name}"
            );
        }
    }
}
