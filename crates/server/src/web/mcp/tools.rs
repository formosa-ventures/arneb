//! The MCP tools. Each runs as the request's [`Identity`](super::Identity).

use rmcp::{
    model::CallToolResult,
    service::{RequestContext, RoleServer},
    tool, tool_router, ErrorData,
};

use super::{identity, ok, ArnebMcp};
use crate::web::auth::AuthVia;

#[tool_router(vis = "pub(super)")]
impl ArnebMcp {
    /// `whoami`: the caller, how it authenticated, and the server.
    #[tool(
        description = "Who the server thinks you are: the user, how the request authenticated \
                       (token, session, basic or unauthenticated), the auth mode, and the \
                       server version and role.",
        annotations(read_only_hint = true, open_world_hint = false)
    )]
    async fn whoami(&self, ctx: RequestContext<RoleServer>) -> Result<CallToolResult, ErrorData> {
        let who = identity(&ctx)?;
        ok(&serde_json::json!({
            "user": who.user,
            "via": via_name(who.via),
            "version": env!("CARGO_PKG_VERSION"),
            "role": self.state.role,
            "auth": if self.state.credentials.is_some() { "password" } else { "none" },
        }))
    }
}

fn via_name(via: AuthVia) -> &'static str {
    match via {
        AuthVia::Token => "token",
        AuthVia::Session => "session",
        AuthVia::Basic => "basic",
        AuthVia::Unauthenticated => "unauthenticated",
    }
}
