//! The MCP tools. Each runs as the request's [`Identity`](super::Identity)
//! and makes the same service call, with the same validation, as the
//! matching `/api/v2` route.

use std::borrow::Cow;
use std::sync::Arc;
use std::time::Duration;

use arneb_protocol::agent::discovery::{DEFAULT_SAMPLE_ROWS, MAX_SAMPLE_ROWS};
use arneb_protocol::agent::{
    AgentLimits, AgentQueryService, ExplainRequest, Explanation, ListRequest, SubmitRequest,
};
use rmcp::{
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::CallToolResult,
    service::{RequestContext, RoleServer},
    tool, tool_router, ErrorData,
};
use serde::{Deserialize, Serialize};

use super::{identity, ok, tool_error, ArnebMcp};
use crate::web::admission::{self, AdmissionUpdate};
use crate::web::agent::{service, wait};
use crate::web::api::{self, BoundedParams, BoundedQuery};
use crate::web::audit::{self, AuditParams};
use crate::web::auth::AuthVia;
use crate::web::error::{ApiError, ErrorCode};
use crate::web::privilege::Layer;
use crate::web::v2;
use crate::web::workers::{self, WorkerOp};
use crate::web::{catalogs, reseed, rules, settings, users};

/// Arguments of `list_catalogs`.
#[derive(Deserialize, schemars::JsonSchema)]
struct ListCatalogsArgs {
    /// Keep only names starting with this (case-insensitive).
    prefix: Option<String>,
    /// Most names to return (default {default_max_rows}, at most {max_rows}).
    limit: Option<usize>,
}

/// Arguments of `list_schemas`.
#[derive(Deserialize, schemars::JsonSchema)]
struct ListSchemasArgs {
    /// The catalog to list.
    catalog: String,
    /// Keep only names starting with this (case-insensitive).
    prefix: Option<String>,
    /// Most names to return (default {default_max_rows}, at most {max_rows}).
    limit: Option<usize>,
}

/// Arguments of `list_tables`.
#[derive(Deserialize, schemars::JsonSchema)]
struct ListTablesArgs {
    /// The catalog holding the schema.
    catalog: String,
    /// The schema to list.
    schema: String,
    /// Keep only names starting with this (case-insensitive).
    prefix: Option<String>,
    /// Most names to return (default {default_max_rows}, at most {max_rows}).
    limit: Option<usize>,
}

/// Arguments of `describe_table`.
#[derive(Deserialize, schemars::JsonSchema)]
struct TableArgs {
    /// The catalog holding the table.
    catalog: String,
    /// The schema holding the table.
    schema: String,
    /// The table to describe.
    table: String,
    /// Most columns to return (default {default_max_rows}, at most {max_rows}).
    limit: Option<usize>,
}

/// Arguments of `sample_table`.
#[derive(Deserialize, schemars::JsonSchema)]
struct SampleArgs {
    /// The catalog holding the table.
    catalog: String,
    /// The schema holding the table.
    schema: String,
    /// The table to sample.
    table: String,
    /// Rows to return (default {sample_rows}, at most {max_sample_rows}).
    rows: Option<usize>,
}

/// Arguments of `explain_query`.
#[derive(Deserialize, schemars::JsonSchema)]
struct ExplainArgs {
    /// The query to plan (it is not executed).
    sql: String,
    /// Default catalog for unqualified names.
    catalog: Option<String>,
    /// Default schema for unqualified names.
    schema: Option<String>,
}

/// Arguments of `submit_query`.
#[derive(Deserialize, schemars::JsonSchema)]
struct SubmitArgs {
    /// The SQL statement to run.
    sql: String,
    /// Default catalog for unqualified names.
    catalog: Option<String>,
    /// Default schema for unqualified names.
    schema: Option<String>,
    /// Rows to keep for fetching (default {default_max_rows}, at most {max_rows}).
    max_rows: Option<usize>,
    /// Milliseconds to wait for the statement to finish before answering
    /// (default 0, at most {max_wait_ms}).
    wait_ms: Option<u64>,
}

/// Arguments of `get_query_status`.
#[derive(Deserialize, schemars::JsonSchema)]
struct StatusArgs {
    /// The statement id from `submit_query`.
    id: String,
    /// Milliseconds to wait for the statement to finish before answering
    /// (default 0, at most {max_wait_ms}).
    wait_ms: Option<u64>,
}

/// Arguments of `fetch_results`.
#[derive(Deserialize, schemars::JsonSchema)]
struct FetchArgs {
    /// The statement id from `submit_query`.
    id: String,
    /// Row offset to start at (default 0); pass the previous page's
    /// `next_cursor`.
    cursor: Option<u64>,
    /// Most bytes of rows in the page (default {page_bytes}, at most
    /// {max_page_bytes}).
    max_bytes: Option<usize>,
}

/// Arguments of `cancel_query`.
#[derive(Deserialize, schemars::JsonSchema)]
struct CancelArgs {
    /// The id to cancel: a statement id from `submit_query`, or a query id
    /// from `list_queries`.
    id: String,
    /// Preview only: report what cancelling would do and change nothing
    /// (default false).
    dry_run: Option<bool>,
}

/// Arguments of `list_workers`.
#[derive(Deserialize, schemars::JsonSchema)]
struct ListWorkersArgs {
    /// Most workers to return: default {worker_limit}; at most
    /// {max_worker_limit}.
    limit: Option<usize>,
}

/// Arguments of `get_worker`.
#[derive(Deserialize, schemars::JsonSchema)]
struct WorkerIdArgs {
    /// The worker id from `list_workers`.
    id: String,
}

/// Arguments of `cordon_worker`, `drain_worker` and `uncordon_worker`.
#[derive(Deserialize, schemars::JsonSchema)]
struct WorkerActionArgs {
    /// The worker id from `list_workers`.
    id: String,
    /// Preview only: report what the action would do and change nothing
    /// (default false).
    dry_run: Option<bool>,
}

/// Arguments of `set_admission_limits`.
#[derive(Deserialize, schemars::JsonSchema)]
struct AdmissionArgs {
    /// New most queries running at once; 0 means unlimited. Omit to keep it.
    max_running_queries: Option<i64>,
    /// New most queries waiting to run. Omit to keep it. At least one limit
    /// is required.
    max_queued_queries: Option<i64>,
    /// Preview only: report what the change would do and change nothing
    /// (default false).
    dry_run: Option<bool>,
}

/// Arguments of `list_audit_log`.
#[derive(Deserialize, schemars::JsonSchema)]
struct AuditArgs {
    /// Most entries to return (default {audit_limit}, at most
    /// {max_audit_limit}).
    limit: Option<usize>,
    /// The previous page's `next_cursor`: entries older than it.
    cursor: Option<u64>,
    /// Keep only actions by this user.
    user: Option<String>,
    /// Keep only this action, such as worker.cordon or query.cancel.
    action: Option<String>,
}

/// Arguments of `list_queries`.
#[derive(Deserialize, schemars::JsonSchema)]
struct ListQueriesArgs {
    /// Keep only this exact state (any case): queued, planning, starting,
    /// running, finishing, finished, failed or cancelled. `running` does not
    /// include planning, starting or finishing.
    state: Option<String>,
    /// Keep only queries of this user.
    user: Option<String>,
    /// Keep only queries that took (or have taken) at least this many
    /// milliseconds: the slow-query view.
    min_elapsed_ms: Option<u64>,
    /// Most queries to return (default {query_limit}, at most
    /// {max_query_limit}).
    limit: Option<usize>,
}

/// Arguments of `get_query`.
#[derive(Deserialize, schemars::JsonSchema)]
struct QueryIdArgs {
    /// The query id (a UUID) from `list_queries`.
    id: String,
}

/// Arguments of `create_token`.
#[derive(Deserialize, schemars::JsonSchema)]
struct CreateTokenArgs {
    /// A label for the token (at most 128 characters).
    name: Option<String>,
    /// Days until it expires (1 to 3650); omit for no expiry.
    expires_in_days: Option<i64>,
}

/// Arguments of `revoke_token`.
#[derive(Deserialize, schemars::JsonSchema)]
struct TokenIdArgs {
    /// The token id from `list_tokens`.
    id: String,
}

/// The configured numbers the agent-facing copy quotes, as `{name}`
/// placeholders and their values. Tool descriptions, input-property
/// descriptions and the server instructions are written with these
/// placeholders and filled once, when the router is built, so the text an
/// agent reads always matches what the server enforces.
pub(super) fn copy_values(limits: &AgentLimits, page_bytes: usize) -> Vec<(&'static str, String)> {
    vec![
        ("{default_max_rows}", limits.default_max_rows.to_string()),
        ("{max_rows}", limits.max_rows.to_string()),
        ("{max_wait_ms}", limits.max_wait.as_millis().to_string()),
        ("{page_bytes}", page_bytes.to_string()),
        ("{max_page_bytes}", limits.max_page_bytes.to_string()),
        (
            "{sample_rows}",
            DEFAULT_SAMPLE_ROWS.min(limits.max_rows).to_string(),
        ),
        (
            "{max_sample_rows}",
            MAX_SAMPLE_ROWS.min(limits.max_rows).to_string(),
        ),
        ("{query_limit}", api::DEFAULT_QUERY_LIMIT.to_string()),
        ("{max_query_limit}", api::MAX_QUERY_LIMIT.to_string()),
        ("{worker_limit}", workers::DEFAULT_WORKER_LIMIT.to_string()),
        ("{max_worker_limit}", workers::MAX_WORKER_LIMIT.to_string()),
        ("{audit_limit}", audit::DEFAULT_AUDIT_LIMIT.to_string()),
        ("{max_audit_limit}", audit::MAX_AUDIT_LIMIT.to_string()),
    ]
}

/// `text` with every placeholder of `values` replaced.
pub(super) fn fill(text: &str, values: &[(&'static str, String)]) -> String {
    values
        .iter()
        .fold(text.to_owned(), |t, (name, value)| t.replace(name, value))
}

/// Fills the placeholders in every `description` string of a JSON schema.
fn fill_schema(value: &mut serde_json::Value, values: &[(&'static str, String)]) {
    match value {
        serde_json::Value::Object(map) => {
            for (key, v) in map.iter_mut() {
                match v {
                    serde_json::Value::String(text) if key == "description" => {
                        *text = fill(text, values);
                    }
                    other => fill_schema(other, values),
                }
            }
        }
        serde_json::Value::Array(items) => {
            for v in items {
                fill_schema(v, values);
            }
        }
        _ => {}
    }
}

/// A tool result for a service outcome: the body, or the error envelope.
/// Arguments of the tools that only preview or run with no other input
/// (`clear_access_rules`, `reseed_state`).
#[derive(Deserialize, schemars::JsonSchema)]
struct DryRunArgs {
    /// Preview only: report what the change would do and change nothing
    /// (default false).
    dry_run: Option<bool>,
}

/// Arguments of `set_access_rules`.
#[derive(Deserialize, schemars::JsonSchema)]
struct SetRulesArgs {
    /// The full access rules as a JSON object in Trino's file-based access
    /// control format (catalogs, schemas, tables, queries,
    /// system_information). It replaces all current rules.
    rules: serde_json::Value,
    /// Preview only: report what the change would do and change nothing
    /// (default false).
    dry_run: Option<bool>,
}

/// Arguments of `list_users` and `list_catalog_configs`.
#[derive(Deserialize, schemars::JsonSchema)]
struct ListConfigArgs {
    /// Most entries to return, up to 1000 (default 100).
    limit: Option<usize>,
}

/// Arguments of `create_user` and `set_user_password`.
#[derive(Deserialize, schemars::JsonSchema)]
struct UserPasswordArgs {
    /// The user name: 1 to 64 characters of letters, digits, `_`, `.`, `@`
    /// or `-`.
    name: String,
    /// The password, 8 to 1024 characters. Never repeat it back to the user.
    password: String,
    /// Preview only: report what the change would do and change nothing
    /// (default false).
    dry_run: Option<bool>,
}

/// Arguments of `delete_user`, `drop_catalog`.
#[derive(Deserialize, schemars::JsonSchema)]
struct NameActionArgs {
    /// The user or catalog name.
    name: String,
    /// Preview only: report what the change would do and change nothing
    /// (default false).
    dry_run: Option<bool>,
}

/// Arguments of `list_user_tokens`.
#[derive(Deserialize, schemars::JsonSchema)]
struct UserTokensArgs {
    /// The user whose tokens to list.
    name: String,
    /// Most tokens to return, up to 1000 (default 100).
    limit: Option<usize>,
}

/// Arguments of `issue_user_token`.
#[derive(Deserialize, schemars::JsonSchema)]
struct IssueUserTokenArgs {
    /// The user to issue the token for.
    name: String,
    /// A label for the token, to tell tokens apart.
    token_name: Option<String>,
    /// Days until the token expires, 1 to 3650; omit for no expiry.
    expires_in_days: Option<i64>,
    /// Preview only: report what the change would do and change nothing
    /// (default false).
    dry_run: Option<bool>,
}

/// Arguments of `revoke_user_token`.
#[derive(Deserialize, schemars::JsonSchema)]
struct RevokeUserTokenArgs {
    /// The user who owns the token.
    name: String,
    /// The token id from `list_user_tokens` (16 hex characters).
    id: String,
    /// Preview only: report what the change would do and change nothing
    /// (default false).
    dry_run: Option<bool>,
}

/// Arguments of `get_catalog_config`.
#[derive(Deserialize, schemars::JsonSchema)]
struct CatalogNameArgs {
    /// The catalog name from `list_catalog_configs`.
    name: String,
}

/// Arguments of `create_catalog`.
#[derive(Deserialize, schemars::JsonSchema)]
struct CreateCatalogArgs {
    /// The catalog definition as a JSON object: name, type (such as hive or
    /// postgresql) and the type's settings, as `[[catalogs]]` in the config.
    spec: serde_json::Value,
    /// Preview only: report what the change would do and change nothing
    /// (default false).
    dry_run: Option<bool>,
}

/// Arguments of `update_catalog`.
#[derive(Deserialize, schemars::JsonSchema)]
struct UpdateCatalogArgs {
    /// The catalog to replace.
    name: String,
    /// The full new definition as a JSON object. A secret sent as
    /// "<redacted>" keeps the stored one, unless the destination changes.
    spec: serde_json::Value,
    /// Preview only: report what the change would do and change nothing
    /// (default false).
    dry_run: Option<bool>,
}

/// Arguments of `update_settings`.
#[derive(Deserialize, schemars::JsonSchema)]
struct UpdateSettingsArgs {
    /// The slow-query log threshold in milliseconds; 0 turns the log off.
    slow_query_threshold_ms: Option<i64>,
    /// Preview only: report what the change would do and change nothing
    /// (default false).
    dry_run: Option<bool>,
}

fn respond<T: Serialize>(outcome: Result<T, ApiError>) -> Result<CallToolResult, ErrorData> {
    match outcome {
        Ok(body) => ok(&body),
        Err(e) => Ok(tool_error(e)),
    }
}

impl ArnebMcp {
    /// The tool router with its copy filled from `values` (see
    /// [`copy_values`]): descriptions and input-schema descriptions. Built
    /// once per service and cloned per request.
    pub(super) fn configured_router(values: &[(&'static str, String)]) -> ToolRouter<Self> {
        let mut router = Self::tool_router();
        for route in router.map.values_mut() {
            let tool = &mut route.attr;
            if let Some(d) = tool.description.as_mut() {
                *d = Cow::Owned(fill(d, values));
            }
            let mut schema = serde_json::Value::Object((*tool.input_schema).clone());
            fill_schema(&mut schema, values);
            if let serde_json::Value::Object(map) = schema {
                tool.input_schema = Arc::new(map);
            }
        }
        router
    }

    /// The longest query text a result carries: the agent limit, or its
    /// default when this node has no agent service.
    fn cell_bytes(&self) -> usize {
        self.state
            .agent
            .as_ref()
            .map_or(AgentLimits::default().max_cell_bytes, |a| {
                a.limits().max_cell_bytes
            })
    }

    /// A worker scheduling change as the caller: the shared path of the
    /// cordon, drain and uncordon tools.
    async fn worker_action(
        &self,
        a: WorkerActionArgs,
        ctx: RequestContext<RoleServer>,
        op: WorkerOp,
    ) -> Result<CallToolResult, ErrorData> {
        let who = identity(&ctx)?;
        respond(
            workers::worker_action_for(&self.state, &who, &a.id, op, a.dry_run.unwrap_or(false))
                .await,
        )
    }

    /// The MCP page budget of inline first pages and omitted `max_bytes`.
    fn page_bytes(&self, svc: &AgentQueryService) -> usize {
        self.state
            .mcp
            .as_ref()
            .map_or(svc.limits().default_page_bytes, |m| m.default_page_bytes)
    }
}

#[tool_router(vis = "pub(super)")]
impl ArnebMcp {
    /// `whoami`: the caller, how it authenticated, and the server.
    #[tool(
        description = "Who the server thinks you are: the user, how the request authenticated \
                       (token, session, basic or unauthenticated), the auth mode, and the \
                       server version and role, and your `privileges` (`observe`, `manage`).",
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
            "privileges": v2::Privileges::of(&self.state.authorizer, &who),
        }))
    }
    /// `list_catalogs`: `GET /api/v2/catalogs`.
    #[tool(
        description = "List the catalogs you can see, sorted, with the total and whether the \
                       list was cut at `limit` (default {default_max_rows}, at most {max_rows}). \
                       Next: list_schemas.",
        annotations(read_only_hint = true, open_world_hint = false)
    )]
    async fn list_catalogs(
        &self,
        Parameters(a): Parameters<ListCatalogsArgs>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let who = identity(&ctx)?;
        respond(
            async {
                let req = ListRequest {
                    prefix: a.prefix,
                    limit: a.limit,
                };
                Ok(service(&self.state)?.list_catalogs(&who.user, &req).await?)
            }
            .await,
        )
    }

    /// `list_schemas`: `GET /api/v2/catalogs/{c}/schemas`.
    #[tool(
        description = "List the schemas of a catalog, sorted, with the total and whether the \
                       list was cut at `limit` (default {default_max_rows}, at most {max_rows}). \
                       Next: list_tables.",
        annotations(read_only_hint = true, open_world_hint = false)
    )]
    async fn list_schemas(
        &self,
        Parameters(a): Parameters<ListSchemasArgs>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let who = identity(&ctx)?;
        respond(
            async {
                let req = ListRequest {
                    prefix: a.prefix,
                    limit: a.limit,
                };
                Ok(service(&self.state)?
                    .list_schemas(&who.user, &a.catalog, &req)
                    .await?)
            }
            .await,
        )
    }

    /// `list_tables`: `GET /api/v2/catalogs/{c}/schemas/{s}/tables`.
    #[tool(
        description = "List the tables of a schema, sorted, with the total and whether the \
                       list was cut at `limit` (default {default_max_rows}, at most {max_rows}). \
                       Next: describe_table.",
        annotations(read_only_hint = true, open_world_hint = false)
    )]
    async fn list_tables(
        &self,
        Parameters(a): Parameters<ListTablesArgs>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let who = identity(&ctx)?;
        respond(
            async {
                let req = ListRequest {
                    prefix: a.prefix,
                    limit: a.limit,
                };
                Ok(service(&self.state)?
                    .list_tables(&who.user, &a.catalog, &a.schema, &req)
                    .await?)
            }
            .await,
        )
    }

    /// `describe_table`: `GET …/tables/{t}`.
    #[tool(
        description = "Describe a table: its columns with types and nullability. Next: \
                       sample_table to see rows, or explain_query / submit_query to query it.",
        annotations(read_only_hint = true, open_world_hint = false)
    )]
    async fn describe_table(
        &self,
        Parameters(a): Parameters<TableArgs>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let who = identity(&ctx)?;
        respond(
            async {
                Ok(service(&self.state)?
                    .describe_table(&who.user, &a.catalog, &a.schema, &a.table, a.limit)
                    .await?)
            }
            .await,
        )
    }

    /// `sample_table`: `GET …/tables/{t}/sample`.
    #[tool(
        description = "Read a few rows of a table (default {sample_rows}, at most \
                       {max_sample_rows}) and return the \
                       finished statement with its first page. Use it to see real values \
                       before writing a query; then submit_query.",
        annotations(read_only_hint = true, open_world_hint = false)
    )]
    async fn sample_table(
        &self,
        Parameters(a): Parameters<SampleArgs>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let who = identity(&ctx)?;
        respond(
            async {
                let svc = service(&self.state)?;
                let page_bytes = Some(self.page_bytes(svc));
                Ok(svc
                    .sample(
                        &who.user, &a.catalog, &a.schema, &a.table, a.rows, page_bytes,
                    )
                    .await?)
            }
            .await,
        )
    }

    /// `explain_query`: `POST /api/v2/explain`.
    #[tool(
        description = "Plan a query without running it: the plan text and the tables it would \
                       scan. Errors name the bad column or table with candidates. Next: \
                       submit_query.",
        annotations(read_only_hint = true, open_world_hint = false)
    )]
    async fn explain_query(
        &self,
        Parameters(a): Parameters<ExplainArgs>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let who = identity(&ctx)?;
        let outcome: Result<Explanation, ApiError> = async {
            let svc = service(&self.state)?;
            let req = ExplainRequest {
                sql: a.sql,
                catalog: a.catalog,
                schema: a.schema,
            };
            Ok(svc
                .explain(&who.user, &req, Some(self.page_bytes(svc)))
                .await?)
        }
        .await;
        match outcome {
            // Like every tool, the text block repeats the JSON: MCP clients
            // that only show the model text content still see the plan, and
            // the MCP page budget bounds both copies.
            Ok(x) => ok(&x),
            Err(e) => Ok(tool_error(e)),
        }
    }

    /// `submit_query`: `POST /api/v2/statements`.
    #[tool(
        description = "Run a SQL statement. Returns its status; with `wait_ms` (at most \
                       {max_wait_ms}) a short query is already finished, with a first page of \
                       rows. At most `max_rows` (default {default_max_rows}, at most {max_rows}) \
                       rows are kept and `truncated` says if more existed. A failed \
                       statement is a status with state `failed` and an error. Next: \
                       get_query_status until finished, then fetch_results starting from \
                       `first_page.next_cursor` (null means you have every row).",
        annotations(open_world_hint = false)
    )]
    async fn submit_query(
        &self,
        Parameters(a): Parameters<SubmitArgs>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let who = identity(&ctx)?;
        respond(
            async {
                let svc = service(&self.state)?;
                let wait: Duration = wait(svc, a.wait_ms)?;
                let req = SubmitRequest {
                    sql: a.sql,
                    catalog: a.catalog,
                    schema: a.schema,
                    max_rows: a.max_rows,
                };
                let page_bytes = Some(self.page_bytes(svc));
                Ok(svc.submit(&who.user, req, wait, page_bytes).await?)
            }
            .await,
        )
    }

    /// `get_query_status`: `GET /api/v2/statements/{id}`.
    #[tool(
        description = "Get the status of a statement you submitted, optionally waiting up to \
                       `wait_ms` (at most {max_wait_ms}) for it to finish. A finished one carries the \
                       columns and a first page of rows. Next: fetch_results starting from \
                       `first_page.next_cursor` while it is set, or cancel_query to stop it.",
        annotations(read_only_hint = true, open_world_hint = false)
    )]
    async fn get_query_status(
        &self,
        Parameters(a): Parameters<StatusArgs>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let who = identity(&ctx)?;
        respond(
            async {
                let svc = service(&self.state)?;
                let wait: Duration = wait(svc, a.wait_ms)?;
                let page_bytes = Some(self.page_bytes(svc));
                Ok(svc.status(&who.user, &a.id, wait, page_bytes).await?)
            }
            .await,
        )
    }

    /// `fetch_results`: `GET /api/v2/statements/{id}/results`.
    #[tool(
        description = "Fetch a page of a finished statement's rows from `cursor`, at most \
                       `max_bytes` of rows (default {page_bytes}, at most {max_page_bytes}; long \
                       cells are cut). Repeat with `next_cursor` until it is null.",
        annotations(read_only_hint = true, open_world_hint = false)
    )]
    async fn fetch_results(
        &self,
        Parameters(a): Parameters<FetchArgs>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let who = identity(&ctx)?;
        respond(
            async {
                let svc = service(&self.state)?;
                let max_bytes = a.max_bytes.or_else(|| Some(self.page_bytes(svc)));
                Ok(svc.results(&who.user, &a.id, a.cursor.unwrap_or(0), max_bytes)?)
            }
            .await,
        )
    }

    /// `cancel_query`: `DELETE /api/v2/statements/{id}` or
    /// `DELETE /api/v2/queries/{id}`.
    #[tool(
        description = "Cancel a query by id. Your own statement from submit_query returns its \
                       status after cancelling (safe to repeat). Any other id, or a dry_run, \
                       returns the manage envelope (dry_run, action, summary, changed, result); \
                       cancelling another user's query needs the manage privilege or a kill \
                       rule and is audited. A query that already ended is changed: false.",
        annotations(
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn cancel_query(
        &self,
        Parameters(a): Parameters<CancelArgs>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let who = identity(&ctx)?;
        let dry_run = a.dry_run.unwrap_or(false);
        if !dry_run {
            // The caller's own agent statement keeps the H3 behavior and
            // result; any other id is not found here and goes on.
            if let Some(svc) = self.state.agent.as_ref() {
                match svc.cancel(&who.user, &a.id, Some(self.page_bytes(svc))) {
                    Ok(status) => return ok(&status),
                    Err(e) => {
                        let e = ApiError::from(e);
                        if e.code != ErrorCode::NotFound {
                            return Ok(tool_error(e));
                        }
                    }
                }
            }
        }
        respond(v2::cancel_query_for(&self.state, &who, &a.id, dry_run).await)
    }

    /// `list_queries`: the bounded `GET /api/v2/queries`.
    #[tool(
        description = "List tracked queries (any protocol) you may see, newest first, at most \
                       `limit` (default {query_limit}, at most {max_query_limit}); `truncated` says if more matched. \
                       Each `state` is lowercase: queued, planning, starting, running, \
                       finishing, finished, failed or cancelled. Filter by `state` (exactly \
                       one of those, so running excludes planning), `user`, or \
                       `min_elapsed_ms` for slow queries. Long `sql` is cut and flagged with \
                       `sql_truncated`. Next: get_query.",
        annotations(read_only_hint = true, open_world_hint = false)
    )]
    async fn list_queries(
        &self,
        Parameters(a): Parameters<ListQueriesArgs>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let who = identity(&ctx)?;
        let params = BoundedParams {
            state: a.state,
            user: a.user,
            min_elapsed_ms: a.min_elapsed_ms,
            limit: a.limit,
        };
        respond(api::bounded_queries(
            &self.state,
            &who,
            params,
            self.cell_bytes(),
        ))
    }

    /// `get_query`: `GET /api/v2/queries/{id}`.
    #[tool(
        description = "Get one tracked query by id: state, user, source, timings, rows, error \
                       and its `sql` (cut and flagged with `sql_truncated` when long).",
        annotations(read_only_hint = true, open_world_hint = false)
    )]
    async fn get_query(
        &self,
        Parameters(a): Parameters<QueryIdArgs>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let who = identity(&ctx)?;
        respond(
            api::visible_query(&self.state, &who, &a.id)
                .map(|info| BoundedQuery::new(info, self.cell_bytes()))
                .map_err(v2::lookup_error),
        )
    }

    /// `get_cluster`: `GET /api/v2/cluster`.
    #[tool(
        description = "Cluster overview: role, alive worker count and admission counters \
                       (running and queued queries and their limits; 0 running limit means \
                       unlimited).",
        annotations(read_only_hint = true, open_world_hint = false)
    )]
    async fn get_cluster(
        &self,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let who = identity(&ctx)?;
        respond(
            self.state
                .authorizer
                .check(&who, Layer::Observe)
                .map(|_| api::cluster_response(&self.state)),
        )
    }

    /// `list_workers`: `GET /api/v2/workers`.
    #[tool(
        description = "List the workers, sorted by id: `limit` defaults to {worker_limit}, at \
                       most {max_worker_limit}; `truncated` says if more exist. Each has \
                       its lifecycle `state` and its `scheduling` (schedulable, cordoned, \
                       draining or drained). Next: get_worker, or cordon_worker to take one \
                       out of rotation.",
        annotations(read_only_hint = true, open_world_hint = false)
    )]
    async fn list_workers(
        &self,
        Parameters(a): Parameters<ListWorkersArgs>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let who = identity(&ctx)?;
        respond(workers::list_workers_for(&self.state, &who, a.limit))
    }

    /// `get_worker`: `GET /api/v2/workers/{id}`.
    #[tool(
        description = "Get one worker by id: address, version, heartbeat age, lifecycle state, \
                       scheduling and the queries in flight on it.",
        annotations(read_only_hint = true, open_world_hint = false)
    )]
    async fn get_worker(
        &self,
        Parameters(a): Parameters<WorkerIdArgs>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let who = identity(&ctx)?;
        respond(workers::get_worker_for(&self.state, &who, &a.id))
    }

    /// `cordon_worker`: `POST /api/v2/workers/{id}/cordon`.
    #[tool(
        description = "Cordon a worker: it gets no new queries, running ones continue. Needs \
                       the manage privilege. Preview with dry_run first. Next: drain_worker to \
                       wait for its queries to finish, or uncordon_worker to undo.",
        annotations(
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn cordon_worker(
        &self,
        Parameters(a): Parameters<WorkerActionArgs>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        self.worker_action(a, ctx, WorkerOp::Cordon).await
    }

    /// `drain_worker`: `POST /api/v2/workers/{id}/drain`.
    #[tool(
        description = "Drain a worker: cordon it and watch its in-flight queries; scheduling \
                       is draining until they finish, then drained, when it is safe to stop. \
                       Needs the manage privilege. Preview with dry_run first. Next: \
                       get_worker until drained, or uncordon_worker to undo.",
        annotations(
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn drain_worker(
        &self,
        Parameters(a): Parameters<WorkerActionArgs>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        self.worker_action(a, ctx, WorkerOp::Drain).await
    }

    /// `uncordon_worker`: `POST /api/v2/workers/{id}/uncordon`.
    #[tool(
        description = "Return a cordoned or draining worker to scheduling so it gets new \
                       queries again. Needs the manage privilege. Preview with dry_run first.",
        annotations(
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn uncordon_worker(
        &self,
        Parameters(a): Parameters<WorkerActionArgs>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        self.worker_action(a, ctx, WorkerOp::Uncordon).await
    }

    /// `get_admission`: `GET /api/v2/admission`.
    #[tool(
        description = "The admission limits (max_running_queries, 0 meaning unlimited, and \
                       max_queued_queries), the running and queued counts, and whether the \
                       limits come from config or from a runtime change. Next: \
                       set_admission_limits.",
        annotations(read_only_hint = true, open_world_hint = false)
    )]
    async fn get_admission(
        &self,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let who = identity(&ctx)?;
        respond(admission::get_admission_for(&self.state, &who))
    }

    /// `set_admission_limits`: `PUT /api/v2/admission`.
    #[tool(
        description = "Change the admission limits at runtime, persisted across restarts. Give \
                       one or both of max_running_queries and max_queued_queries (non-negative \
                       integers). Lowering never stops a running query. Needs the manage \
                       privilege. Preview with dry_run first; read the result with \
                       get_admission.",
        annotations(
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn set_admission_limits(
        &self,
        Parameters(a): Parameters<AdmissionArgs>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let who = identity(&ctx)?;
        // Validation (negatives, ranges, at least one field) lives in
        // `set_admission_for`, so MCP and HTTP refuse identically.
        let req = AdmissionUpdate {
            max_running_queries: a.max_running_queries.map(serde_json::Value::from),
            max_queued_queries: a.max_queued_queries.map(serde_json::Value::from),
        };
        respond(
            admission::set_admission_for(&self.state, &who, req, a.dry_run.unwrap_or(false)).await,
        )
    }

    /// `list_audit_log`: `GET /api/v2/audit`.
    #[tool(
        description = "Read the audit log of manage actions (worker, admission, query cancel \
                       and token changes), newest first, at most `limit` (default \
                       {audit_limit}, at most {max_audit_limit}). Continue with `next_cursor` \
                       as `cursor`; `truncated` says if more exist. Filter by `user` or \
                       `action`.",
        annotations(read_only_hint = true, open_world_hint = false)
    )]
    async fn list_audit_log(
        &self,
        Parameters(a): Parameters<AuditArgs>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let who = identity(&ctx)?;
        let params = AuditParams {
            limit: a.limit,
            cursor: a.cursor,
            user: a.user,
            action: a.action,
        };
        respond(audit::audit_page_for(&self.state, &who, params))
    }

    /// `list_tokens`: `GET /api/v2/tokens`.
    #[tool(
        description = "List your live API tokens (id, name, created, expiry; never the secret). \
                       Needs password auth.",
        annotations(read_only_hint = true, open_world_hint = false)
    )]
    async fn list_tokens(
        &self,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let who = identity(&ctx)?;
        respond(v2::list_tokens_for(&self.state, &who.user))
    }

    /// `create_token`: `POST /api/v2/tokens`.
    #[tool(
        description = "Create an API token for yourself, optionally named and expiring in \
                       1..3650 days. The secret `token` is shown only in this result. A token \
                       cannot mint one that outlives itself. Needs password auth.",
        annotations(open_world_hint = false)
    )]
    async fn create_token(
        &self,
        Parameters(a): Parameters<CreateTokenArgs>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let who = identity(&ctx)?;
        let headers = ctx
            .extensions
            .get::<axum::http::request::Parts>()
            .map(|p| p.headers.clone())
            .unwrap_or_default();
        let req = v2::IssueTokenRequest {
            name: a.name,
            expires_in_days: a.expires_in_days.map(serde_json::Value::from),
        };
        respond(v2::issue_token_for(&self.state, &who, v2::TokenTarget::Own, &headers, req).await)
    }

    /// `revoke_token`: `DELETE /api/v2/tokens/{id}`.
    #[tool(
        description = "Revoke one of your API tokens by id. An unknown id is not_found. Needs password auth.",
        annotations(
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn revoke_token(
        &self,
        Parameters(a): Parameters<TokenIdArgs>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let who = identity(&ctx)?;
        respond(
            v2::revoke_token_for(&self.state, &who, v2::TokenTarget::Own, a.id.clone())
                .await
                .map(|()| serde_json::json!({"revoked": a.id})),
        )
    }

    /// `get_access_rules`: `GET /api/v2/access-rules`.
    #[tool(
        description = "The stored access rules (null when none) and whether they came from \
                       the config file. Needs the manage privilege. Next: set_access_rules or \
                       clear_access_rules.",
        annotations(read_only_hint = true, open_world_hint = false)
    )]
    async fn get_access_rules(
        &self,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let who = identity(&ctx)?;
        respond(rules::get_rules_for(&self.state, &who).await)
    }

    /// `set_access_rules`: `PUT /api/v2/access-rules`.
    #[tool(
        description = "Replace all access rules with `rules` (a JSON object in Trino's \
                       file-based format), effective for the next statement. A change that \
                       would remove your own manage privilege is refused. Needs the manage \
                       privilege. Preview with dry_run first.",
        annotations(
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn set_access_rules(
        &self,
        Parameters(a): Parameters<SetRulesArgs>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let who = identity(&ctx)?;
        respond(
            rules::set_rules_for(
                &self.state,
                &who,
                a.dry_run.unwrap_or(false),
                a.rules.to_string(),
            )
            .await,
        )
    }

    /// `clear_access_rules`: `DELETE /api/v2/access-rules`.
    #[tool(
        description = "Remove every access rule, which means every user may do everything. A \
                       change that would remove your own manage privilege is refused. Needs \
                       the manage privilege. Preview with dry_run first.",
        annotations(
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn clear_access_rules(
        &self,
        Parameters(a): Parameters<DryRunArgs>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let who = identity(&ctx)?;
        respond(rules::clear_rules_for(&self.state, &who, a.dry_run.unwrap_or(false)).await)
    }

    /// `list_users`: `GET /api/v2/users`.
    #[tool(
        description = "List the users in name order (never passwords): `limit` defaults to \
                       100, at most 1000; `truncated` says if more exist. Needs the manage \
                       privilege. Next: create_user, list_user_tokens.",
        annotations(read_only_hint = true, open_world_hint = false)
    )]
    async fn list_users(
        &self,
        Parameters(a): Parameters<ListConfigArgs>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let who = identity(&ctx)?;
        respond(users::list_users_for(&self.state, &who, a.limit).await)
    }

    /// `create_user`: `POST /api/v2/users`.
    #[tool(
        description = "Create a user with a password. The password is not logged or audited \
                       and never returned: do not repeat it back to the user. Needs the manage \
                       privilege. Preview with dry_run first. Next: issue_user_token.",
        annotations(
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = false
        )
    )]
    async fn create_user(
        &self,
        Parameters(a): Parameters<UserPasswordArgs>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let who = identity(&ctx)?;
        respond(
            users::create_user_for(
                &self.state,
                &who,
                a.dry_run.unwrap_or(false),
                a.name,
                a.password,
            )
            .await,
        )
    }

    /// `set_user_password`: `PATCH /api/v2/users/{name}`.
    #[tool(
        description = "Set a new password for a user; it revokes all of the user's tokens and \
                       ends their sessions. The password is not logged or audited and never \
                       returned: do not repeat it back to the user. Needs the manage \
                       privilege. Preview with dry_run first.",
        annotations(
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn set_user_password(
        &self,
        Parameters(a): Parameters<UserPasswordArgs>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let who = identity(&ctx)?;
        respond(
            users::set_password_for(
                &self.state,
                &who,
                a.dry_run.unwrap_or(false),
                a.name,
                a.password,
            )
            .await,
        )
    }

    /// `delete_user`: `DELETE /api/v2/users/{name}`.
    #[tool(
        description = "Delete a user and all of the user's tokens. Needs the manage \
                       privilege. Preview with dry_run first.",
        annotations(
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn delete_user(
        &self,
        Parameters(a): Parameters<NameActionArgs>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let who = identity(&ctx)?;
        respond(users::delete_user_for(&self.state, &who, a.dry_run.unwrap_or(false), a.name).await)
    }

    /// `list_user_tokens`: `GET /api/v2/users/{name}/tokens`.
    #[tool(
        description = "List a user's live API tokens (id, name, created, expiry; never the \
                       secret): `limit` defaults to 100, at most 1000. Needs the manage \
                       privilege. Next: revoke_user_token.",
        annotations(read_only_hint = true, open_world_hint = false)
    )]
    async fn list_user_tokens(
        &self,
        Parameters(a): Parameters<UserTokensArgs>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let who = identity(&ctx)?;
        respond(users::list_user_tokens_for(&self.state, &who, a.name, a.limit).await)
    }

    /// `issue_user_token`: `POST /api/v2/users/{name}/tokens`.
    #[tool(
        description = "Issue an API token for a user, optionally labeled and expiring in \
                       1..3650 days. The secret `token` is shown only in this result, once. \
                       Needs the manage privilege. Preview with dry_run first.",
        annotations(
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = false
        )
    )]
    async fn issue_user_token(
        &self,
        Parameters(a): Parameters<IssueUserTokenArgs>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let who = identity(&ctx)?;
        let headers = ctx
            .extensions
            .get::<axum::http::request::Parts>()
            .map(|p| p.headers.clone())
            .unwrap_or_default();
        let req = v2::IssueTokenRequest {
            name: a.token_name,
            expires_in_days: a.expires_in_days.map(serde_json::Value::from),
        };
        respond(
            users::issue_user_token_for(
                &self.state,
                &who,
                a.dry_run.unwrap_or(false),
                a.name,
                &headers,
                req,
            )
            .await,
        )
    }

    /// `revoke_user_token`: `DELETE /api/v2/users/{name}/tokens/{id}`.
    #[tool(
        description = "Revoke one of a user's API tokens by id; it stops working at once. A \
                       token of another user is not_found. Needs the manage privilege. \
                       Preview with dry_run first.",
        annotations(
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn revoke_user_token(
        &self,
        Parameters(a): Parameters<RevokeUserTokenArgs>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let who = identity(&ctx)?;
        respond(
            users::revoke_user_token_for(
                &self.state,
                &who,
                a.dry_run.unwrap_or(false),
                a.name,
                a.id,
            )
            .await,
        )
    }

    /// `list_catalog_configs`: `GET /api/v2/catalog-configs`.
    #[tool(
        description = "List the catalog definitions in name order, secrets shown as \
                       \"<redacted>\": `limit` defaults to 100, at most 1000. Needs the manage \
                       privilege. Next: get_catalog_config, create_catalog.",
        annotations(read_only_hint = true, open_world_hint = false)
    )]
    async fn list_catalog_configs(
        &self,
        Parameters(a): Parameters<ListConfigArgs>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let who = identity(&ctx)?;
        respond(catalogs::list_catalog_configs_for(&self.state, &who, a.limit).await)
    }

    /// `get_catalog_config`: `GET /api/v2/catalog-configs/{name}`.
    #[tool(
        description = "Get one catalog definition, secrets shown as \"<redacted>\". Needs the \
                       manage privilege. Next: update_catalog with the changed definition, \
                       sending \"<redacted>\" back unchanged to keep a secret.",
        annotations(read_only_hint = true, open_world_hint = false)
    )]
    async fn get_catalog_config(
        &self,
        Parameters(a): Parameters<CatalogNameArgs>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let who = identity(&ctx)?;
        respond(catalogs::get_catalog_config_for(&self.state, &who, a.name).await)
    }

    /// `create_catalog`: `POST /api/v2/catalog-configs`.
    #[tool(
        description = "Add a catalog from `spec`: the coordinator connects to it first, then \
                       saves it and pushes it to every live worker. Send real secrets, not \
                       \"<redacted>\". Needs the manage privilege. Preview with dry_run first.",
        annotations(
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = false
        )
    )]
    async fn create_catalog(
        &self,
        Parameters(a): Parameters<CreateCatalogArgs>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let who = identity(&ctx)?;
        respond(
            catalogs::create_catalog_for(&self.state, &who, a.dry_run.unwrap_or(false), a.spec)
                .await,
        )
    }

    /// `update_catalog`: `PUT /api/v2/catalog-configs/{name}`.
    #[tool(
        description = "Replace catalog `name` with `spec`. A secret sent as \"<redacted>\" keeps \
                       the stored one, unless the host or database changes, then send the \
                       secret again. A config-file catalog becomes a runtime one. Needs the \
                       manage privilege. Preview with dry_run first.",
        annotations(
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn update_catalog(
        &self,
        Parameters(a): Parameters<UpdateCatalogArgs>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let who = identity(&ctx)?;
        respond(
            catalogs::update_catalog_for(
                &self.state,
                &who,
                a.dry_run.unwrap_or(false),
                a.name,
                a.spec,
            )
            .await,
        )
    }

    /// `drop_catalog`: `DELETE /api/v2/catalog-configs/{name}`.
    #[tool(
        description = "Drop a catalog from the coordinator and every worker. Dropping the \
                       default catalog is allowed, with a warning. Needs the manage \
                       privilege. Preview with dry_run first.",
        annotations(
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn drop_catalog(
        &self,
        Parameters(a): Parameters<NameActionArgs>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let who = identity(&ctx)?;
        respond(
            catalogs::drop_catalog_for(&self.state, &who, a.dry_run.unwrap_or(false), a.name).await,
        )
    }

    /// `get_settings`: `GET /api/v2/settings`.
    #[tool(
        description = "The runtime settings (slow_query_threshold_ms, 0 meaning off) and \
                       whether the value comes from config or from a runtime change. Next: \
                       update_settings.",
        annotations(read_only_hint = true, open_world_hint = false)
    )]
    async fn get_settings(
        &self,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let who = identity(&ctx)?;
        respond(settings::get_settings_for(&self.state, &who).await)
    }

    /// `update_settings`: `PATCH /api/v2/settings`.
    #[tool(
        description = "Change the slow-query log threshold at runtime, persisted across \
                       restarts: slow_query_threshold_ms, a non-negative integer, 0 for off. \
                       Needs the manage privilege. Preview with dry_run first; read the \
                       result with get_settings.",
        annotations(
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn update_settings(
        &self,
        Parameters(a): Parameters<UpdateSettingsArgs>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let who = identity(&ctx)?;
        // Validation lives in `threshold_arg`, so MCP and HTTP refuse alike.
        let respond_err = |e| Ok(tool_error(e));
        let ms =
            match settings::threshold_arg(a.slow_query_threshold_ms.map(serde_json::Value::from)) {
                Ok(ms) => ms,
                // Authorization first, as the route does, so a non-manager
                // learns nothing about the arguments.
                Err(e) => {
                    return match self.state.authorizer.check(&who, Layer::Manage) {
                        Ok(_) => respond_err(e),
                        Err(denied) => respond_err(denied),
                    };
                }
            };
        respond(
            settings::update_settings_for(&self.state, &who, a.dry_run.unwrap_or(false), ms).await,
        )
    }

    /// `reseed_state`: `POST /api/v2/state/reseed`.
    #[tool(
        description = "Replace the runtime changes (catalogs, access rules, settings, users) \
                       with what the config file says now; tokens, cordons and the audit log \
                       stay. Refused if it would remove your own manage privilege. Needs the \
                       manage privilege. Always preview with dry_run first.",
        annotations(
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn reseed_state(
        &self,
        Parameters(a): Parameters<DryRunArgs>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let who = identity(&ctx)?;
        respond(reseed::reseed_for(&self.state, &who, a.dry_run.unwrap_or(false)).await)
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use arneb_catalog::CatalogManager;
    use arneb_common::error::ArnebError;
    use arneb_common::types::{ColumnInfo, DataType};
    use arneb_connectors::memory::{
        MemoryCatalog, MemoryConnectorFactory, MemorySchema, MemoryTable,
    };
    use arneb_connectors::ConnectorRegistry;
    use arneb_execution::ExecutionContext;
    use arneb_planner::LogicalPlan;
    use arneb_protocol::agent::{AgentLimits, AgentQueryService};
    use arneb_protocol::DistributedExecutor;
    use arrow::array::{ArrayRef, StringArray};
    use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use async_trait::async_trait;
    use axum::body::Body;
    use axum::http::{header, Method, StatusCode};
    use serde_json::{json, Value};
    use tower::ServiceExt as _;

    use crate::web::mcp::McpSettings;
    use crate::web::{build_router, SessionStore, WebState};

    const BIG_ROWS: usize = 50_000;

    /// `memory.default.big(n)` with 50,000 ~100-byte strings and
    /// `memory.default.small(n)` with two.
    fn engine_state() -> (Arc<CatalogManager>, Arc<ConnectorRegistry>) {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "n",
            ArrowDataType::Utf8,
            false,
        )]));
        let infos: Vec<ColumnInfo> = schema
            .fields()
            .iter()
            .map(|f| ColumnInfo {
                name: f.name().clone(),
                data_type: DataType::try_from(f.data_type().clone()).unwrap(),
                nullable: f.is_nullable(),
            })
            .collect();
        let table = |values: Vec<String>| {
            let col: ArrayRef = Arc::new(StringArray::from(values));
            let batch = RecordBatch::try_new(schema.clone(), vec![col]).unwrap();
            Arc::new(MemoryTable::new(infos.clone(), vec![batch]))
        };
        let pad = "x".repeat(90);
        let big = table((0..BIG_ROWS).map(|i| format!("{i:06}-{pad}")).collect());
        let small = table(vec!["a".into(), "b".into()]);
        let default_schema = Arc::new(MemorySchema::new());
        default_schema.register_table("big", big);
        default_schema.register_table("small", small);
        let memory = Arc::new(MemoryCatalog::new());
        memory.register_schema("default", default_schema);
        let cm = CatalogManager::new("memory", "default");
        cm.register_catalog("memory", memory.clone());
        let registry = ConnectorRegistry::new();
        registry.register(
            "memory",
            Arc::new(MemoryConnectorFactory::new(memory, "default")),
        );
        (Arc::new(cm), Arc::new(registry))
    }

    /// Executes every plan by never returning.
    struct HangingExecutor;

    #[async_trait]
    impl DistributedExecutor for HangingExecutor {
        async fn execute(
            &self,
            _plan: LogicalPlan,
            _exec_ctx: &ExecutionContext,
        ) -> Result<Vec<RecordBatch>, ArnebError> {
            std::future::pending::<()>().await;
            Ok(Vec::new())
        }

        fn has_workers(&self) -> bool {
            true
        }
    }

    struct Fixture {
        router: axum::Router,
        tracker: Arc<arneb_scheduler::QueryTracker>,
        /// Password auth (`alice`, `bob`, `root` with `root` a manager), as
        /// opposed to `[auth] type = "none"` claiming `X-Arneb-User`.
        password: bool,
    }

    /// The credentials of `user` on `f`.
    fn authn(f: &Fixture, user: &str) -> (&'static str, String) {
        if f.password {
            let pw = match user {
                "alice" => "secret",
                "bob" => "hunter2",
                "root" => "rootpw",
                other => panic!("no password for {other}"),
            };
            ("authorization", crate::web::v2::tests::basic(user, pw))
        } else {
            ("x-arneb-user", user.to_owned())
        }
    }

    /// Password auth with two workers, an admission controller, an agent
    /// service that never finishes a statement, and rules that let every
    /// user execute and view but kill nothing; `root` manages.
    fn manage_fixture() -> Fixture {
        manage_fixture_with(true)
    }

    /// [`manage_fixture`], with or without the agent service.
    fn manage_fixture_with(with_agent: bool) -> Fixture {
        let tracker = Arc::new(arneb_scheduler::QueryTracker::new());
        let rules = r#"{"queries":[{"allow":["execute","view"]}]}"#;
        let mut state = crate::web::v2::tests::with_authz(
            crate::web::v2::tests::web_state(true, None),
            Some(rules),
            &["root"],
        );
        let (cm, registry) = engine_state();
        let agent = AgentQueryService::new(cm, registry, tracker.clone(), AgentLimits::default())
            .with_access_rules(state.rules.clone())
            .with_distributed_executor(Arc::new(HangingExecutor));
        state.query_tracker = tracker.clone();
        state.admission = Some(arneb_scheduler::AdmissionController::new(8, 256));
        state.agent = with_agent.then_some(agent);
        // A re-seed reads this config: password auth and `root`.
        let config =
            std::env::temp_dir().join(format!("arneb-mcp-reseed-{}.toml", std::process::id()));
        let hash =
            arneb_protocol::ScramVerifier::from_password("rootpw", b"0123456789abcdef", 4096);
        std::fs::write(
            &config,
            format!(
                "[auth]\ntype = \"password\"\n[[auth.users]]\nname = \"root\"\npassword_hash = \"{hash}\"\n"
            ),
        )
        .unwrap();
        state.config_path = Some(config);
        state.catalogs = Some(crate::catalogs::LiveCatalogs::with_test_builder(
            Arc::new(CatalogManager::new("memory", "default")),
            Arc::new(ConnectorRegistry::new()),
            Box::new(|_spec| {
                let catalog = Arc::new(MemoryCatalog::new());
                Ok(crate::catalogs::BuiltCatalog {
                    provider: catalog.clone(),
                    connector: Arc::new(MemoryConnectorFactory::new(catalog, "default")),
                })
            }),
        ));
        state.mcp = Some(McpSettings {
            allowed_hosts: None,
            allowed_origins: None,
            default_page_bytes: 32768,
        });
        for (id, port) in [("w1", 1), ("w2", 2)] {
            state
                .node_registry
                .heartbeat(id.into(), format!("http://127.0.0.1:{port}"), 256);
        }
        Fixture {
            router: build_router(state.clone()),
            tracker,
            password: true,
        }
    }

    fn fixture(hang: bool) -> Fixture {
        fixture_with(hang, 32768)
    }

    fn fixture_with(hang: bool, page_bytes: usize) -> Fixture {
        fixture_tracked(
            hang,
            page_bytes,
            Arc::new(arneb_scheduler::QueryTracker::new()),
        )
    }

    fn fixture_tracked(
        hang: bool,
        page_bytes: usize,
        tracker: Arc<arneb_scheduler::QueryTracker>,
    ) -> Fixture {
        fixture_limited(hang, page_bytes, tracker, AgentLimits::default())
    }

    fn fixture_limited(
        hang: bool,
        page_bytes: usize,
        tracker: Arc<arneb_scheduler::QueryTracker>,
        limits: AgentLimits,
    ) -> Fixture {
        fixture_ruled(hang, page_bytes, tracker, limits, None)
    }

    fn fixture_ruled(
        hang: bool,
        page_bytes: usize,
        tracker: Arc<arneb_scheduler::QueryTracker>,
        limits: AgentLimits,
        rules: Option<Arc<arneb_catalog::AccessRules>>,
    ) -> Fixture {
        let (cm, registry) = engine_state();
        let mut agent = AgentQueryService::new(cm, registry, tracker.clone(), limits)
            .with_access_rules(rules.clone());
        if hang {
            agent = agent.with_distributed_executor(Arc::new(HangingExecutor));
        }
        let state = WebState {
            query_tracker: tracker.clone(),
            node_registry: arneb_scheduler::NodeRegistry::new(30),
            start_time: std::time::Instant::now(),
            role: "standalone".into(),
            admission: None,
            admission_changes: Default::default(),
            cordon_changes: Default::default(),
            max_queued_queries: 8,
            memory_pool: Arc::new(arneb_common::memory_pool::GreedyMemoryPool::new(1 << 20)),
            memory_limit_bytes: None,
            basic: None,
            credentials: None,
            state: None,
            audit: crate::web::test_audit_log(),
            rules: rules.into(),
            config_changes: Default::default(),
            config_path: None,
            catalogs: None,
            authorizer: crate::web::privilege::Authorizer::new(
                Default::default(),
                Vec::new(),
                false,
            ),
            metrics_auth: false,
            sessions: SessionStore::new(),
            agent: Some(agent),
            mcp: Some(McpSettings {
                allowed_hosts: None,
                allowed_origins: None,
                default_page_bytes: page_bytes,
            }),
        };
        Fixture {
            router: build_router(state.clone()),
            tracker,
            password: false,
        }
    }

    /// A `tools/call` as `user`; the whole JSON-RPC response.
    async fn tool_raw(f: &Fixture, user: &str, name: &str, args: Value) -> Value {
        let body = json!({"jsonrpc":"2.0","id":1,"method":"tools/call",
                          "params":{"name":name,"arguments":args}});
        let (auth_header, auth_value) = authn(f, user);
        let req = axum::http::Request::builder()
            .method(Method::POST)
            .uri("/mcp")
            .header(header::HOST, "127.0.0.1:6432")
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::ACCEPT, "application/json, text/event-stream")
            .header("mcp-protocol-version", "2025-06-18")
            .header(auth_header, auth_value)
            .body(Body::from(body.to_string()))
            .unwrap();
        let resp = f.router.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    /// A `tools/call` as `user`; the JSON-RPC `result`.
    async fn tool(f: &Fixture, user: &str, name: &str, args: Value) -> Value {
        let v = tool_raw(f, user, name, args).await;
        assert!(v["error"].is_null(), "JSON-RPC error: {v}");
        v["result"].clone()
    }

    /// A tool result that succeeded: its `structuredContent`.
    async fn tool_ok(f: &Fixture, user: &str, name: &str, args: Value) -> Value {
        let r = tool(f, user, name, args).await;
        assert_ne!(r["isError"], json!(true), "{name} failed: {r}");
        r["structuredContent"].clone()
    }

    /// A tool result that failed: the `error` envelope.
    async fn tool_err(f: &Fixture, user: &str, name: &str, args: Value) -> Value {
        let r = tool(f, user, name, args).await;
        assert_eq!(r["isError"], json!(true), "{name} should fail: {r}");
        r["structuredContent"]["error"].clone()
    }

    async fn api_get(f: &Fixture, user: &str, path: &str) -> Value {
        api_call(f, user, Method::GET, path).await
    }

    async fn api_call(f: &Fixture, user: &str, method: Method, path: &str) -> Value {
        let (auth_header, auth_value) = authn(f, user);
        let req = axum::http::Request::builder()
            .method(method)
            .uri(path)
            .header(auth_header, auth_value)
            .body(Body::empty())
            .unwrap();
        let resp = f.router.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "{path}");
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[tokio::test]
    async fn discovery_tools_match_the_api() {
        let f = fixture(false);
        let cases = [
            ("list_catalogs", json!({}), "/api/v2/catalogs"),
            (
                "list_schemas",
                json!({"catalog":"memory"}),
                "/api/v2/catalogs/memory/schemas",
            ),
            (
                "list_tables",
                json!({"catalog":"memory","schema":"default","prefix":"sm"}),
                "/api/v2/catalogs/memory/schemas/default/tables?prefix=sm",
            ),
            (
                "describe_table",
                json!({"catalog":"memory","schema":"default","table":"small"}),
                "/api/v2/catalogs/memory/schemas/default/tables/small",
            ),
            (
                "describe_table",
                json!({"catalog":"memory","schema":"default","table":"small","limit":1}),
                "/api/v2/catalogs/memory/schemas/default/tables/small?limit=1",
            ),
        ];
        for (name, args, path) in cases {
            let via_tool = tool_ok(&f, "alice", name, args).await;
            assert_eq!(via_tool, api_get(&f, "alice", path).await, "{name}");
        }
        // sample_table is GET .../sample with its first page at the MCP budget.
        let s = tool_ok(
            &f,
            "alice",
            "sample_table",
            json!({"catalog":"memory","schema":"default","table":"small","rows":1}),
        )
        .await;
        assert_eq!(s["state"], "finished");
        assert_eq!(s["first_page"]["rows"].as_array().unwrap().len(), 1);
    }

    /// Walks `fetch_results` from `cursor`, returning all rows.
    async fn walk(f: &Fixture, id: &str, mut cursor: Option<u64>) -> Vec<Value> {
        let mut rows = Vec::new();
        while let Some(c) = cursor {
            let page = tool_ok(f, "alice", "fetch_results", json!({"id": id, "cursor": c})).await;
            rows.extend(page["rows"].as_array().unwrap().iter().cloned());
            cursor = page["next_cursor"].as_u64();
        }
        rows
    }

    #[tokio::test]
    async fn submit_poll_fetch_walks_all_rows() {
        let f = fixture(false);
        let s = tool_ok(
            &f,
            "alice",
            "submit_query",
            json!({"sql":"SELECT n FROM big","max_rows":1000,"wait_ms":5000}),
        )
        .await;
        assert_eq!(s["state"], "finished", "{s}");
        assert_eq!(s["truncated"], true);
        assert!(s["total_rows"].is_null());
        let first = &s["first_page"];
        let inline = first["rows"].as_array().unwrap();
        assert!(inline.len() < 1000, "{} rows inline", inline.len());
        assert!(!inline.is_empty());
        assert!(serde_json::to_string(&first["rows"]).unwrap().len() <= 32768 + 200);
        let next = first["next_cursor"].as_u64().expect("more pages");

        let id = s["id"].as_str().unwrap();
        let mut rows: Vec<Value> = inline.clone();
        rows.extend(walk(&f, id, Some(next)).await);
        assert_eq!(rows.len(), 1000);

        // The status poll is re-paged the same way.
        let st = tool_ok(&f, "alice", "get_query_status", json!({"id": id})).await;
        assert_eq!(st["first_page"]["rows"], first["rows"]);
        assert_eq!(st["first_page"]["next_cursor"], first["next_cursor"]);
        // And so is cancelling a statement that already finished.
        let c = tool_ok(&f, "alice", "cancel_query", json!({"id": id})).await;
        assert_eq!(c["state"], "finished");
        assert_eq!(c["first_page"], st["first_page"]);
    }

    #[tokio::test]
    async fn submit_query_write_is_forbidden() {
        let rules = arneb_catalog::AccessRules::from_json(
            r#"{"tables": [{"user": "alice", "privileges": ["SELECT"]}]}"#,
        )
        .unwrap();
        let f = fixture_ruled(
            false,
            32768,
            Arc::new(arneb_scheduler::QueryTracker::new()),
            AgentLimits::default(),
            Some(Arc::new(rules)),
        );
        let s = tool_ok(
            &f,
            "alice",
            "submit_query",
            json!({"sql":"CREATE TABLE memory.default.t AS SELECT 1 AS x","wait_ms":5000}),
        )
        .await;
        assert_eq!(s["state"], "failed", "{s}");
        assert_eq!(s["error"]["code"], "forbidden", "{s}");
    }

    #[tokio::test]
    async fn fetch_results_default_page_is_mcp_budget() {
        let f = fixture(false);
        let s = tool_ok(
            &f,
            "alice",
            "submit_query",
            json!({"sql":"SELECT n FROM big","max_rows":1000,"wait_ms":5000}),
        )
        .await;
        let id = s["id"].as_str().unwrap();
        let page = tool_ok(&f, "alice", "fetch_results", json!({"id": id})).await;
        let bytes = serde_json::to_string(&page["rows"]).unwrap().len();
        assert!(bytes <= 32768 + 200, "{bytes}");
        assert!(page["next_cursor"].as_u64().is_some());

        let e = tool_err(
            &f,
            "alice",
            "fetch_results",
            json!({"id": id, "max_bytes": 4_194_305}),
        )
        .await;
        assert_eq!(e["code"], "invalid_argument");
        assert!(e["hint"].as_str().unwrap().contains("4194304"), "{e}");
    }

    #[tokio::test]
    async fn other_users_statement_is_not_found() {
        let f = fixture(false);
        let s = tool_ok(
            &f,
            "alice",
            "submit_query",
            json!({"sql":"SELECT n FROM small","wait_ms":5000}),
        )
        .await;
        let id = s["id"].as_str().unwrap();
        for name in ["get_query_status", "fetch_results"] {
            let e = tool_err(&f, "bob", name, json!({"id": id})).await;
            assert_eq!(e["code"], "not_found", "{name}");
        }
        // Cancelling is a kill (D2): under `none` only the owner may.
        let e = tool_err(&f, "bob", "cancel_query", json!({"id": id})).await;
        assert_eq!(e["code"], "manage_requires_auth");
    }

    #[tokio::test]
    async fn bad_column_error_carries_location_and_candidates() {
        let f = fixture(false);
        let s = tool_ok(
            &f,
            "alice",
            "submit_query",
            json!({"sql":"SELECT nn FROM big","wait_ms":5000}),
        )
        .await;
        assert_eq!(s["state"], "failed", "{s}");
        assert_eq!(s["error"]["code"], "column_not_found");
        assert!(s["error"]["candidates"]
            .as_array()
            .unwrap()
            .contains(&json!("n")));
        assert_eq!(s["error"]["location"]["line"], 1);

        // The route answers 201 for a syntax error too: it fails as a status.
        let s = tool_ok(
            &f,
            "alice",
            "submit_query",
            json!({"sql":"SELEC 1","wait_ms":5000}),
        )
        .await;
        assert_eq!(s["state"], "failed", "{s}");
        assert_eq!(s["error"]["code"], "syntax_error", "{s}");

        // What the route refuses with an HTTP error status is `isError`.
        let e = tool_err(&f, "alice", "submit_query", json!({"sql":" ; "})).await;
        assert_eq!(e["code"], "invalid_argument", "{e}");
    }

    #[tokio::test]
    async fn cancel_query_cancels_a_running_statement() {
        let f = fixture(true);
        let s = tool_ok(
            &f,
            "alice",
            "submit_query",
            json!({"sql":"SELECT n FROM small"}),
        )
        .await;
        let id = s["id"].as_str().unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        let running = tool_ok(&f, "alice", "get_query_status", json!({"id": id})).await;
        assert!(
            matches!(running["state"].as_str(), Some("queued" | "running")),
            "{running}"
        );
        let c = tool_ok(&f, "alice", "cancel_query", json!({"id": id})).await;
        assert_eq!(c["state"], "cancelled", "{c}");
        let st = tool_ok(&f, "alice", "get_query_status", json!({"id": id})).await;
        assert_eq!(st["state"], "cancelled");
    }

    #[tokio::test]
    async fn ceilings_are_refused() {
        let f = fixture(false);
        let t = ("memory", "default");
        let cases = [
            ("submit_query", json!({"sql":"SELECT 1","wait_ms":10001})),
            ("submit_query", json!({"sql":"SELECT 1","max_rows":10001})),
            ("get_query_status", json!({"id":"x","wait_ms":10001})),
            (
                "list_tables",
                json!({"catalog":t.0,"schema":t.1,"limit":10001}),
            ),
            (
                "sample_table",
                json!({"catalog":t.0,"schema":t.1,"table":"small","rows":101}),
            ),
            (
                "describe_table",
                json!({"catalog":t.0,"schema":t.1,"table":"small","limit":10001}),
            ),
        ];
        for (name, args) in cases {
            let e = tool_err(&f, "alice", name, args.clone()).await;
            assert_eq!(e["code"], "invalid_argument", "{name} {args}");
        }
    }

    #[tokio::test]
    async fn explain_query_does_not_execute() {
        let f = fixture(false);
        let before = f.tracker.list_queries(None).len();
        let x = tool_ok(
            &f,
            "alice",
            "explain_query",
            json!({"sql":"SELECT n FROM big"}),
        )
        .await;
        assert!(x["plan"].is_string(), "{x}");
        assert_eq!(x["plan_truncated"], false, "{x}");
        assert_eq!(f.tracker.list_queries(None).len(), before);
        // Text-only MCP clients see the plan too: the text block is the
        // same JSON as `structuredContent`.
        let r = tool(
            &f,
            "alice",
            "explain_query",
            json!({"sql":"SELECT n FROM big"}),
        )
        .await;
        let text: Value = serde_json::from_str(r["content"][0]["text"].as_str().unwrap()).unwrap();
        assert_eq!(text, r["structuredContent"]);
        let e = tool_err(
            &f,
            "alice",
            "explain_query",
            json!({"sql":"SELECT nn FROM big"}),
        )
        .await;
        assert_eq!(e["code"], "column_not_found");
    }

    #[tokio::test]
    async fn explain_query_plan_is_cut_at_the_mcp_budget() {
        let f = fixture_with(false, 64);
        let x = tool_ok(
            &f,
            "alice",
            "explain_query",
            json!({"sql":"SELECT n FROM big WHERE n = 'a' OR n = 'b' OR n = 'c'"}),
        )
        .await;
        assert_eq!(x["plan_truncated"], true, "{x}");
        assert!(x["plan"].as_str().unwrap().len() <= 64, "{x}");
    }

    #[tokio::test]
    async fn sample_table_first_page_is_mcp_budget() {
        let f = fixture_with(false, 2048);
        let s = tool_ok(
            &f,
            "alice",
            "sample_table",
            json!({"catalog":"memory","schema":"default","table":"big","rows":100}),
        )
        .await;
        let page = &s["first_page"];
        let rows = page["rows"].as_array().unwrap();
        let bytes = serde_json::to_string(&page["rows"]).unwrap().len();
        assert!(bytes <= 2048 + 120, "{bytes} bytes");
        assert!(rows.len() < 100, "{} rows", rows.len());
        assert!(page["next_cursor"].as_u64().is_some(), "{page}");
    }

    #[tokio::test]
    async fn missing_argument_is_a_tool_error_naming_it() {
        // rmcp reports unparsable arguments as an `isError` text result,
        // not a JSON-RPC error.
        let f = fixture(false);
        let v = tool_raw(&f, "alice", "get_query_status", json!({})).await;
        assert!(v["error"].is_null(), "{v}");
        assert_eq!(v["result"]["isError"], json!(true), "{v}");
        let text = v["result"]["content"][0]["text"].as_str().unwrap_or("");
        assert!(text.contains("`id`"), "{v}");
    }
    fn history_fixture() -> Fixture {
        fixture_tracked(
            false,
            32768,
            Arc::new(arneb_scheduler::QueryTracker::with_history_limit(1000)),
        )
    }

    #[tokio::test]
    async fn list_queries_is_bounded_newest_first() {
        let f = history_fixture();
        let mut ids = Vec::new();
        for i in 0..600 {
            ids.push(f.tracker.create_query_from(
                format!("SELECT {i}"),
                "pgwire",
                Some("alice".into()),
            ));
        }
        for id in ids.iter().take(5) {
            f.tracker.finish_query(id, 1).unwrap();
        }
        let r = tool_ok(&f, "alice", "list_queries", json!({})).await;
        let q = r["queries"].as_array().unwrap();
        assert_eq!(q.len(), 50);
        assert_eq!(r["truncated"], true);
        let created: Vec<u64> = q
            .iter()
            .map(|x| x["created_ms"].as_u64().unwrap())
            .collect();
        assert!(created.windows(2).all(|w| w[0] >= w[1]), "{created:?}");

        let r = tool_ok(&f, "alice", "list_queries", json!({"limit": 500})).await;
        assert_eq!(r["queries"].as_array().unwrap().len(), 500);
        for bad in [
            json!({"limit": 501}),
            json!({"limit": 0}),
            json!({"state": "bogus"}),
        ] {
            let e = tool_err(&f, "alice", "list_queries", bad).await;
            assert_eq!(e["code"], "invalid_argument");
        }
        let r = tool_ok(&f, "alice", "list_queries", json!({"state": "FINISHED"})).await;
        assert_eq!(r["queries"].as_array().unwrap().len(), 5);
        assert_eq!(r["truncated"], false);
        // States read the way the filter takes them: lowercase.
        let finished = r["queries"][0].clone();
        assert_eq!(finished["state"], "finished", "{finished}");
        let r = tool_ok(&f, "alice", "list_queries", json!({"state": "queued"})).await;
        assert_eq!(r["queries"][0]["state"], "queued");
        // #217 review: every reported state is a filter, and `running`
        // means exactly Running.
        f.tracker
            .transition_query(&ids[5], arneb_scheduler::QueryState::Planning)
            .unwrap();
        let r = tool_ok(&f, "alice", "list_queries", json!({"state": "planning"})).await;
        let q = r["queries"].as_array().unwrap();
        assert_eq!(q.len(), 1, "{r}");
        assert_eq!(q[0]["state"], "planning");
        let r = tool_ok(&f, "alice", "list_queries", json!({"state": "running"})).await;
        assert!(r["queries"].as_array().unwrap().is_empty(), "{r}");
        let e = tool_err(&f, "alice", "list_queries", json!({"state": "bogus"})).await;
        assert!(e["hint"].as_str().unwrap().contains("finishing"), "{e}");
        let id = finished["query_id"].as_str().unwrap();
        let one = tool_ok(&f, "alice", "get_query", json!({"id": id})).await;
        assert_eq!(one["state"], "finished");
        // `/api/v2/queries` keeps its own spelling.
        let api = api_get(&f, "alice", &format!("/api/v2/queries/{id}")).await;
        assert_eq!(api["state"], "Finished", "{api}");
        let r = tool_ok(&f, "alice", "list_queries", json!({"user": "carol"})).await;
        assert!(r["queries"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn slow_queries_filter() {
        let f = history_fixture();
        let slow = f
            .tracker
            .create_query_from("SELECT slow".into(), "pgwire", None);
        tokio::time::sleep(Duration::from_millis(500)).await;
        f.tracker
            .create_query_from("SELECT fast".into(), "pgwire", None);
        let r = tool_ok(&f, "alice", "list_queries", json!({"min_elapsed_ms": 250})).await;
        let q = r["queries"].as_array().unwrap();
        assert_eq!(q.len(), 1, "{r}");
        assert_eq!(q[0]["query_id"], slow.to_string());
        let r = tool_ok(&f, "alice", "list_queries", json!({})).await;
        assert_eq!(r["queries"].as_array().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn long_sql_is_cut_and_flagged() {
        let f = history_fixture();
        let long = "測".repeat(20_000 / 3 + 1);
        let id = f.tracker.create_query_from(long, "pgwire", None);
        let short = f
            .tracker
            .create_query_from("SELECT 1".into(), "pgwire", None);
        let r = tool_ok(&f, "alice", "list_queries", json!({})).await;
        for q in r["queries"].as_array().unwrap() {
            let sql = q["sql"].as_str().unwrap();
            if q["query_id"] == id.to_string() {
                assert!(sql.len() <= AgentLimits::default().max_cell_bytes);
                assert!(sql.len() > AgentLimits::default().max_cell_bytes - 3);
                assert!(sql.chars().all(|c| c == '測'));
                assert_eq!(q["sql_truncated"], true);
            } else {
                assert_eq!(q["sql_truncated"], false);
            }
        }
        let one = tool_ok(&f, "alice", "get_query", json!({"id": id.to_string()})).await;
        assert_eq!(one["sql_truncated"], true);
        let one = tool_ok(&f, "alice", "get_query", json!({"id": short.to_string()})).await;
        assert_eq!(
            (one["sql"].as_str(), one["sql_truncated"].as_bool()),
            (Some("SELECT 1"), Some(false))
        );
        let e = tool_err(&f, "alice", "get_query", json!({"id": "nope"})).await;
        assert_eq!(e["code"], "invalid_argument");
        let e = tool_err(
            &f,
            "alice",
            "get_query",
            json!({"id": uuid::Uuid::new_v4().to_string()}),
        )
        .await;
        assert_eq!(e["code"], "not_found");
    }

    #[tokio::test]
    async fn cluster_and_workers_match_the_api() {
        let f = fixture(false);
        assert_eq!(
            tool_ok(&f, "alice", "get_cluster", json!({})).await,
            api_get(&f, "alice", "/api/v2/cluster").await
        );
        // `list_workers` is the bounded `/workers` listing.
        let w = tool_ok(&f, "alice", "list_workers", json!({})).await;
        assert_eq!(w, api_get(&f, "alice", "/api/v2/workers").await);
    }

    #[tokio::test]
    async fn whoami_reports_privileges_over_mcp() {
        // Under none: observe, never manage.
        let f = fixture(false);
        let w = tool_ok(&f, "alice", "whoami", json!({})).await;
        assert_eq!(w["privileges"], json!({"observe": true, "manage": false}));
    }

    /// A raw JSON-RPC request; the whole response body.
    async fn rpc_raw(f: &Fixture, body: Value) -> Value {
        let req = axum::http::Request::builder()
            .method(Method::POST)
            .uri("/mcp")
            .header(header::HOST, "127.0.0.1:6432")
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::ACCEPT, "application/json, text/event-stream")
            .header("mcp-protocol-version", "2025-06-18")
            .body(Body::from(body.to_string()))
            .unwrap();
        let resp = f.router.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    /// The exact tool set (D5), sorted.
    const TOOL_NAMES: [&str; 43] = [
        "cancel_query",
        "clear_access_rules",
        "cordon_worker",
        "create_catalog",
        "create_token",
        "create_user",
        "delete_user",
        "describe_table",
        "drain_worker",
        "drop_catalog",
        "explain_query",
        "fetch_results",
        "get_access_rules",
        "get_admission",
        "get_catalog_config",
        "get_cluster",
        "get_query",
        "get_query_status",
        "get_settings",
        "get_worker",
        "issue_user_token",
        "list_audit_log",
        "list_catalog_configs",
        "list_catalogs",
        "list_queries",
        "list_schemas",
        "list_tables",
        "list_tokens",
        "list_user_tokens",
        "list_users",
        "list_workers",
        "reseed_state",
        "revoke_token",
        "revoke_user_token",
        "sample_table",
        "set_access_rules",
        "set_admission_limits",
        "set_user_password",
        "submit_query",
        "uncordon_worker",
        "update_catalog",
        "update_settings",
        "whoami",
    ];

    /// The tools that change cluster state and take `dry_run`.
    const MANAGE_TOOLS: [&str; 5] = [
        "cancel_query",
        "cordon_worker",
        "drain_worker",
        "uncordon_worker",
        "set_admission_limits",
    ];

    /// The runtime-config manage tools (D12): all take `dry_run`.
    const CONFIG_MANAGE_TOOLS: [&str; 12] = [
        "clear_access_rules",
        "create_catalog",
        "create_user",
        "delete_user",
        "drop_catalog",
        "issue_user_token",
        "reseed_state",
        "revoke_user_token",
        "set_access_rules",
        "set_user_password",
        "update_catalog",
        "update_settings",
    ];

    #[tokio::test]
    async fn tool_list_contract() {
        let f = fixture(false);
        let v = rpc_raw(
            &f,
            json!({"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}),
        )
        .await;
        let tools = v["result"]["tools"].as_array().expect("tools array");
        let mut names: Vec<&str> = tools.iter().map(|t| t["name"].as_str().unwrap()).collect();
        names.sort_unstable();
        assert_eq!(names, TOOL_NAMES);

        let by_name = |n: &str| tools.iter().find(|t| t["name"] == n).unwrap();
        for t in tools {
            let name = t["name"].as_str().unwrap();
            let d = t["description"].as_str().unwrap_or("");
            assert!(
                (40..=600).contains(&d.chars().count()),
                "{name}: description is {} characters",
                d.chars().count()
            );
            if let Some(props) = t["inputSchema"]["properties"].as_object() {
                for (prop, schema) in props {
                    let pd = schema["description"].as_str().unwrap_or("");
                    assert!(!pd.trim().is_empty(), "{name}.{prop} has no description");
                }
            }
            // D8: annotations.
            let a = &t["annotations"];
            assert_eq!(a["openWorldHint"], json!(false), "{name} open_world");
            let read_only = name == "whoami"
                || name.starts_with("list_")
                || name.starts_with("get_")
                || [
                    "describe_table",
                    "sample_table",
                    "explain_query",
                    "fetch_results",
                ]
                .contains(&name);
            let destructive = [
                "revoke_token",
                "cancel_query",
                "cordon_worker",
                "drain_worker",
                "set_admission_limits",
                "clear_access_rules",
                "delete_user",
                "drop_catalog",
                "reseed_state",
                "revoke_user_token",
                "set_access_rules",
                "set_user_password",
                "update_catalog",
                "update_settings",
            ]
            .contains(&name);
            if name == "uncordon_worker" {
                assert_eq!(a["destructiveHint"], json!(false), "{name} destructive");
                assert_eq!(a["idempotentHint"], json!(true), "{name} idempotent");
            }
            if MANAGE_TOOLS.contains(&name) || CONFIG_MANAGE_TOOLS.contains(&name) {
                assert!(
                    t["inputSchema"]["properties"]["dry_run"].is_object(),
                    "{name} has no dry_run"
                );
            }
            if read_only {
                assert_eq!(a["readOnlyHint"], json!(true), "{name} read_only");
            } else {
                assert_ne!(
                    a["readOnlyHint"],
                    json!(true),
                    "{name} must not be read_only"
                );
            }
            if destructive {
                assert_eq!(a["destructiveHint"], json!(true), "{name} destructive");
                assert_eq!(a["idempotentHint"], json!(true), "{name} idempotent");
            }
        }

        // D9: each step names the next tool.
        let desc = |n: &str| by_name(n)["description"].as_str().unwrap().to_owned();
        let d = desc("submit_query");
        assert!(
            d.contains("get_query_status") && d.contains("fetch_results"),
            "{d}"
        );
        assert!(desc("get_query_status").contains("fetch_results"));
        for n in ["submit_query", "get_query_status"] {
            assert!(desc(n).contains("first_page.next_cursor"), "{n}");
        }
        assert!(desc("list_tables").contains("describe_table"));
        assert!(desc("list_catalogs").contains("list_schemas"));
        assert!(desc("list_schemas").contains("list_tables"));
        assert!(desc("describe_table").contains("sample_table"));
        assert!(desc("explain_query").contains("submit_query"));
        assert!(desc("list_queries").contains("get_query"));
        let cordon = desc("cordon_worker");
        assert!(
            cordon.contains("drain_worker") && cordon.contains("uncordon_worker"),
            "{cordon}"
        );
        assert!(desc("list_workers").contains("cordon_worker"));
        assert!(desc("set_admission_limits").contains("get_admission"));
    }

    /// Every string an agent reads: the instructions, each tool's
    /// description and each input property's description.
    async fn agent_copy(f: &Fixture) -> Vec<String> {
        let init = json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{
            "protocolVersion":"2025-06-18","capabilities":{},
            "clientInfo":{"name":"t","version":"1"}}});
        let v = rpc_raw(f, init).await;
        let mut copy = vec![v["result"]["instructions"].as_str().unwrap().to_owned()];
        let v = rpc_raw(
            f,
            json!({"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}),
        )
        .await;
        for t in v["result"]["tools"].as_array().unwrap() {
            copy.push(t["description"].as_str().unwrap().to_owned());
            for p in t["inputSchema"]["properties"]
                .as_object()
                .into_iter()
                .flat_map(|m| m.values())
            {
                copy.push(p["description"].as_str().unwrap_or("").to_owned());
            }
        }
        copy
    }

    /// The description and property descriptions of tool `name`.
    async fn tool_text(f: &Fixture, name: &str) -> String {
        let v = rpc_raw(
            f,
            json!({"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}),
        )
        .await;
        let t = v["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .find(|t| t["name"] == name)
            .unwrap()
            .clone();
        format!("{} {}", t["description"], t["inputSchema"])
    }

    /// #217 review: the copy quotes the configured limits, not the
    /// defaults, so an agent that follows it is never refused.
    #[tokio::test]
    async fn copy_quotes_the_configured_limits() {
        let limits = AgentLimits {
            default_max_rows: 100,
            max_rows: 500,
            max_page_bytes: 1_048_576,
            max_wait: Duration::from_millis(2000),
            ..AgentLimits::default()
        };
        let f = fixture_limited(
            false,
            4096,
            Arc::new(arneb_scheduler::QueryTracker::new()),
            limits,
        );
        let copy = agent_copy(&f).await;
        let all = copy.join("\n");
        for text in &copy {
            assert!(!text.contains('{'), "unfilled placeholder: {text}");
            for default in ["10000", "1000,", "1000)", "32768", "4194304"] {
                assert!(!text.contains(default), "default {default} in: {text}");
            }
        }
        let instructions = &copy[0];
        for configured in [
            "up to 2000",
            "default 100,",
            "ceiling 500",
            "4096",
            "1048576",
        ] {
            assert!(
                instructions.contains(configured),
                "{configured}: {instructions}"
            );
        }
        for configured in [
            "at most 2000",
            "at most 500",
            "default 100,",
            "default 4096",
            "at most 1048576",
        ] {
            assert!(all.contains(configured), "{configured} missing");
        }
        // And the server enforces exactly those numbers.
        let e = tool_err(
            &f,
            "alice",
            "submit_query",
            json!({"sql":"SELECT 1","max_rows":501}),
        )
        .await;
        assert_eq!(e["code"], "invalid_argument");
        let ok = tool_ok(
            &f,
            "alice",
            "submit_query",
            json!({"sql":"SELECT 1","max_rows":500,"wait_ms":2000}),
        )
        .await;
        assert_eq!(ok["max_rows"], 500);

        // The listings quote the configured ceilings.
        for (tool, quoted) in [
            ("list_workers", ["defaults to 100", "at most 1000"]),
            ("list_audit_log", ["default 50", "at most 500"]),
        ] {
            let text = tool_text(&f, tool).await;
            for q in quoted {
                assert!(text.contains(q), "{tool}: {q} missing in {text}");
            }
        }

        // The defaults appear under the default configuration.
        let copy = agent_copy(&fixture(false)).await.join("\n");
        for default in ["at most 10000", "default 32768", "at most 4194304"] {
            assert!(copy.contains(default), "{default} missing");
        }
        assert!(!copy.contains('{'), "unfilled placeholder");
    }

    #[tokio::test]
    async fn instructions_follow_the_workflow_and_fit_the_budget() {
        let f = fixture(false);
        let init = json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{
            "protocolVersion":"2025-06-18","capabilities":{},
            "clientInfo":{"name":"t","version":"1"}}});
        let v = rpc_raw(&f, init).await;
        let text = v["result"]["instructions"].as_str().unwrap();
        assert!(
            text.chars().count() <= 1900,
            "{} chars",
            text.chars().count()
        );
        let mut at = 0;
        for step in [
            "list_catalogs",
            "describe_table",
            "explain_query",
            "submit_query",
            "get_query_status",
            "fetch_results",
            "cancel_query",
        ] {
            let pos = text[at..]
                .find(step)
                .unwrap_or_else(|| panic!("`{step}` missing or out of order in: {text}"));
            at += pos;
        }
        for word in [
            "dry_run",
            "manage",
            "audited",
            "observe access",
            "queries kill rule",
            "may see",
            "SQL text",
            "close matches",
            "max_rows",
            "truncated",
            "hint",
            "candidates",
            "first_page.next_cursor",
        ] {
            assert!(text.contains(word), "instructions omit `{word}`");
        }
    }

    #[tokio::test]
    async fn manage_tools_match_the_api() {
        let f = manage_fixture();
        for (tool, path) in [
            ("list_workers", "/api/v2/workers"),
            ("get_admission", "/api/v2/admission"),
            ("list_audit_log", "/api/v2/audit"),
        ] {
            assert_eq!(
                tool_ok(&f, "root", tool, json!({})).await,
                api_get(&f, "root", path).await,
                "{tool}"
            );
        }
        assert_eq!(
            tool_ok(&f, "root", "get_worker", json!({"id":"w1"})).await,
            api_get(&f, "root", "/api/v2/workers/w1").await
        );
        // Bounded: above the ceiling is refused, never clamped.
        let e = tool_err(&f, "root", "list_workers", json!({"limit": 1001})).await;
        assert_eq!(e["code"], "invalid_argument");
        let e = tool_err(&f, "root", "list_audit_log", json!({"limit": 501})).await;
        assert_eq!(e["code"], "invalid_argument");

        let dry = tool_ok(
            &f,
            "root",
            "cordon_worker",
            json!({"id":"w1","dry_run":true}),
        )
        .await;
        assert_eq!(dry["dry_run"], true);
        assert_eq!(
            dry,
            api_call(
                &f,
                "root",
                Method::POST,
                "/api/v2/workers/w1/cordon?dry_run=true"
            )
            .await
        );
        // A dry run changes nothing.
        let w = tool_ok(&f, "root", "get_worker", json!({"id":"w1"})).await;
        assert_eq!(w["scheduling"], "schedulable");

        // The real call cordons, is audited, and uncordon undoes it.
        let c = tool_ok(&f, "root", "cordon_worker", json!({"id":"w1"})).await;
        assert_eq!(c["result"]["scheduling"], "cordoned", "{c}");
        let log = tool_ok(
            &f,
            "root",
            "list_audit_log",
            json!({"action":"worker.cordon"}),
        )
        .await;
        assert_eq!(log["entries"][0]["user"], "root", "{log}");
        let u = tool_ok(&f, "root", "uncordon_worker", json!({"id":"w1"})).await;
        assert_eq!(u["result"]["scheduling"], "schedulable", "{u}");
    }

    #[tokio::test]
    async fn manage_tools_refuse_non_managers() {
        let f = manage_fixture();
        let calls = [
            ("cordon_worker", json!({"id":"w1"})),
            ("drain_worker", json!({"id":"w1"})),
            ("uncordon_worker", json!({"id":"w1"})),
            ("set_admission_limits", json!({"max_queued_queries":5})),
        ];
        for (name, args) in &calls {
            let e = tool_err(&f, "alice", name, args.clone()).await;
            assert_eq!(e["code"], "forbidden", "{name}");
            let mut dry = args.clone();
            dry["dry_run"] = json!(true);
            let e = tool_err(&f, "alice", name, dry).await;
            assert_eq!(e["code"], "forbidden", "{name} dry");
        }
        let w = tool_ok(&f, "root", "get_worker", json!({"id":"w1"})).await;
        assert_eq!(w["scheduling"], "schedulable");
        let a = tool_ok(&f, "root", "get_admission", json!({})).await;
        assert_eq!(a["max_queued_queries"], 256);

        // Under `none` nobody manages.
        let none = fixture(false);
        for (name, args) in &calls {
            let e = tool_err(&none, "alice", name, args.clone()).await;
            assert_eq!(e["code"], "manage_requires_auth", "{name}");
        }
    }

    /// Polls `get_query_status` until the statement runs.
    async fn wait_running(f: &Fixture, user: &str, id: &str) {
        for _ in 0..200 {
            let st = tool_ok(f, user, "get_query_status", json!({"id": id})).await;
            if st["state"] == "running" {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("statement {id} never ran");
    }

    #[tokio::test]
    async fn cancel_query_without_an_agent_service_uses_the_tracker() {
        let f = manage_fixture_with(false);
        let mine = f
            .tracker
            .create_query_from("SELECT 1".into(), "pgwire", Some("alice".into()));
        let r = tool_ok(&f, "alice", "cancel_query", json!({"id": mine.to_string()})).await;
        assert_eq!(r["action"], "query.cancel", "{r}");
        assert_eq!(r["dry_run"], false);
        let theirs = f
            .tracker
            .create_query_from("SELECT 2".into(), "pgwire", Some("bob".into()));
        let e = tool_err(
            &f,
            "alice",
            "cancel_query",
            json!({"id": theirs.to_string()}),
        )
        .await;
        assert_eq!(e["code"], "forbidden", "{e}");
        let r = tool_ok(
            &f,
            "root",
            "cancel_query",
            json!({"id": theirs.to_string()}),
        )
        .await;
        assert_eq!(r["changed"], true, "{r}");
    }

    #[tokio::test]
    async fn cancel_query_widened() {
        let f = manage_fixture();
        let submit = |user: &'static str| {
            let f = &f;
            async move {
                let s = tool_ok(
                    f,
                    user,
                    "submit_query",
                    json!({"sql":"SELECT n FROM small"}),
                )
                .await;
                s["id"].as_str().unwrap().to_owned()
            }
        };
        // Her own statement keeps the H3 status shape.
        let own = submit("alice").await;
        wait_running(&f, "alice", &own).await;
        let dry = tool_ok(
            &f,
            "alice",
            "cancel_query",
            json!({"id": own, "dry_run": true}),
        )
        .await;
        assert_eq!(dry["dry_run"], true, "{dry}");
        assert_eq!(dry["action"], "query.cancel");
        let st = tool_ok(&f, "alice", "get_query_status", json!({"id": own})).await;
        assert!(
            matches!(st["state"].as_str(), Some("queued" | "running")),
            "the dry run left the statement running: {st}"
        );
        let c = tool_ok(&f, "alice", "cancel_query", json!({"id": own})).await;
        assert_eq!(c["state"], "cancelled", "{c}");
        assert!(c["id"].is_string() && c["dry_run"].is_null());

        // A manager cancels another user's statement: the envelope, audited.
        let theirs = submit("alice").await;
        wait_running(&f, "alice", &theirs).await;
        let r = tool_ok(&f, "root", "cancel_query", json!({"id": theirs})).await;
        assert_eq!(r["dry_run"], false, "{r}");
        assert_eq!(r["result"]["state"], "cancelled", "{r}");
        let log = tool_ok(
            &f,
            "root",
            "list_audit_log",
            json!({"action":"query.cancel"}),
        )
        .await;
        assert_eq!(log["entries"][0]["target"], theirs, "{log}");

        // A non-manager may not kill someone else's tracker query.
        let id = f
            .tracker
            .create_query_from("SELECT 1".into(), "pgwire", Some("bob".into()));
        let e = tool_err(&f, "alice", "cancel_query", json!({"id": id.to_string()})).await;
        assert_eq!(e["code"], "forbidden", "{e}");
    }

    #[tokio::test]
    async fn set_admission_limits_round_trip() {
        let f = manage_fixture();
        let r = tool_ok(
            &f,
            "root",
            "set_admission_limits",
            json!({"max_queued_queries": 5}),
        )
        .await;
        assert_eq!(r["changed"], true, "{r}");
        let a = tool_ok(&f, "root", "get_admission", json!({})).await;
        assert_eq!(a["max_queued_queries"], 5);
        assert_eq!(a["max_running_queries"], 8);
        for bad in [json!({"max_running_queries": -1}), json!({})] {
            let e = tool_err(&f, "root", "set_admission_limits", bad).await;
            assert_eq!(e["code"], "invalid_argument");
        }
        // A value of the wrong type is rmcp's text error naming the field,
        // not the `{code}` envelope.
        for bad in [
            json!({"max_running_queries": 1.5}),
            json!({"max_queued_queries": "5"}),
            json!({"max_running_queries": 9_223_372_036_854_775_808u64}),
        ] {
            // serde's message names the offending value and `i64`, not the
            // field.
            let value = bad
                .as_object()
                .unwrap()
                .values()
                .next()
                .unwrap()
                .to_string();
            let value = value.trim_matches('"').to_owned();
            let r = tool(&f, "root", "set_admission_limits", bad).await;
            assert_eq!(r["isError"], json!(true), "{r}");
            assert!(r["structuredContent"]["error"].is_null(), "{r}");
            let text = r["content"][0]["text"].as_str().unwrap_or("");
            assert!(text.contains(&value) && text.contains("i64"), "{text}");
        }
        let a = tool_ok(&f, "root", "get_admission", json!({})).await;
        assert_eq!(a["max_queued_queries"], 5);
    }

    /// An `/api/v2` call as `user` with an optional body; the JSON answer
    /// (status asserted `200`).
    async fn api_send(
        f: &Fixture,
        user: &str,
        method: Method,
        path: &str,
        body: Option<String>,
    ) -> Value {
        let (auth_header, auth_value) = authn(f, user);
        let req = axum::http::Request::builder()
            .method(method)
            .uri(path)
            .header(auth_header, auth_value)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(body.unwrap_or_default()))
            .unwrap();
        let resp = f.router.clone().oneshot(req).await.unwrap();
        let status = resp.status();
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(status, StatusCode::OK, "{path}: {:?}", bytes);
        serde_json::from_slice(&bytes).unwrap()
    }

    #[tokio::test]
    async fn tool_list_has_43_tools() {
        let f = fixture(false);
        let v = rpc_raw(
            &f,
            json!({"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}),
        )
        .await;
        assert_eq!(v["result"]["tools"].as_array().unwrap().len(), 43);
    }

    const RULES: &str =
        r#"{"catalogs":[{"allow":"all"}],"queries":[{"allow":["execute","view"]}]}"#;

    #[tokio::test]
    async fn each_config_tool_matches_its_route() {
        let _threshold = crate::web::settings::THRESHOLD_TEST_LOCK.lock().await;
        let f = manage_fixture();
        let rules: Value = serde_json::from_str(RULES).unwrap();

        // Access rules.
        assert_eq!(
            tool_ok(&f, "root", "get_access_rules", json!({})).await,
            api_get(&f, "root", "/api/v2/access-rules").await
        );
        let dry = tool_ok(
            &f,
            "root",
            "set_access_rules",
            json!({"rules": rules, "dry_run": true}),
        )
        .await;
        assert_eq!(dry["dry_run"], true, "{dry}");
        assert_eq!(
            dry,
            api_send(
                &f,
                "root",
                Method::PUT,
                "/api/v2/access-rules?dry_run=true",
                Some(RULES.to_owned())
            )
            .await
        );
        let set = tool_ok(&f, "root", "set_access_rules", json!({"rules": rules})).await;
        assert_eq!(set["changed"], true, "{set}");
        assert_eq!(
            tool_ok(&f, "root", "get_access_rules", json!({})).await,
            api_get(&f, "root", "/api/v2/access-rules").await
        );
        let e = tool_err(&f, "root", "set_access_rules", json!({"rules": [1]})).await;
        assert_eq!(e["code"], "invalid_argument", "{e}");
        assert_eq!(
            tool_ok(&f, "root", "clear_access_rules", json!({"dry_run": true})).await,
            api_send(
                &f,
                "root",
                Method::DELETE,
                "/api/v2/access-rules?dry_run=true",
                None
            )
            .await
        );

        // Users and their tokens.
        assert_eq!(
            tool_ok(&f, "root", "list_users", json!({})).await,
            api_get(&f, "root", "/api/v2/users").await
        );
        let e = tool_err(&f, "root", "list_users", json!({"limit": 1001})).await;
        assert_eq!(e["code"], "invalid_argument", "{e}");
        let args = json!({"name": "dave", "password": "dave-password", "dry_run": true});
        assert_eq!(
            tool_ok(&f, "root", "create_user", args).await,
            api_send(
                &f,
                "root",
                Method::POST,
                "/api/v2/users?dry_run=true",
                Some(json!({"name":"dave","password":"dave-password"}).to_string())
            )
            .await
        );
        let made = tool_ok(
            &f,
            "root",
            "create_user",
            json!({"name": "dave", "password": "dave-password"}),
        )
        .await;
        assert_eq!(made["result"]["name"], "dave", "{made}");
        assert_eq!(
            tool_ok(&f, "root", "list_users", json!({})).await,
            api_get(&f, "root", "/api/v2/users").await
        );
        assert_eq!(
            tool_ok(
                &f,
                "root",
                "set_user_password",
                json!({"name":"dave","password":"another-password","dry_run":true})
            )
            .await,
            api_send(
                &f,
                "root",
                Method::PATCH,
                "/api/v2/users/dave?dry_run=true",
                Some(json!({"password":"another-password"}).to_string())
            )
            .await
        );
        assert_eq!(
            tool_ok(
                &f,
                "root",
                "delete_user",
                json!({"name":"dave","dry_run":true})
            )
            .await,
            api_send(
                &f,
                "root",
                Method::DELETE,
                "/api/v2/users/dave?dry_run=true",
                None
            )
            .await
        );
        let issued = tool_ok(
            &f,
            "root",
            "issue_user_token",
            json!({"name":"dave","token_name":"ci","expires_in_days":30}),
        )
        .await;
        let id = issued["result"]["id"].as_str().unwrap().to_owned();
        assert!(issued["result"]["token"].is_string(), "{issued}");
        assert_eq!(
            tool_ok(&f, "root", "list_user_tokens", json!({"name":"dave"})).await,
            api_get(&f, "root", "/api/v2/users/dave/tokens").await
        );
        let dry = tool_ok(
            &f,
            "root",
            "issue_user_token",
            json!({"name":"dave","token_name":"ci","dry_run":true}),
        )
        .await;
        assert_eq!(
            dry["result"],
            Value::Null,
            "a dry run issues nothing: {dry}"
        );
        assert_eq!(
            tool_ok(
                &f,
                "root",
                "revoke_user_token",
                json!({"name":"dave","id":id,"dry_run":true})
            )
            .await,
            api_send(
                &f,
                "root",
                Method::DELETE,
                &format!("/api/v2/users/dave/tokens/{id}?dry_run=true"),
                None
            )
            .await
        );
        let r = tool_ok(
            &f,
            "root",
            "revoke_user_token",
            json!({"name":"dave","id":id}),
        )
        .await;
        assert_eq!(r["action"], "token.revoke", "{r}");

        // Catalog definitions.
        let spec = json!({"name":"extra","type":"postgresql",
            "connection_url":"postgresql://u:s3cret-url@127.0.0.1:1/db","user":"u",
            "password":"s3cret-pw"});
        let created = tool_ok(&f, "root", "create_catalog", json!({"spec": spec})).await;
        assert_eq!(created["result"]["name"], "extra", "{created}");
        assert!(!created.to_string().contains("s3cret"), "{created}");
        assert_eq!(
            tool_ok(&f, "root", "list_catalog_configs", json!({})).await,
            api_get(&f, "root", "/api/v2/catalog-configs").await
        );
        let got = tool_ok(&f, "root", "get_catalog_config", json!({"name":"extra"})).await;
        assert_eq!(
            got,
            api_get(&f, "root", "/api/v2/catalog-configs/extra").await
        );
        assert!(!got.to_string().contains("s3cret"), "{got}");
        assert!(got.to_string().contains("<redacted>"), "{got}");
        // The redacted body goes back unchanged.
        let back = tool_ok(
            &f,
            "root",
            "update_catalog",
            json!({"name":"extra","spec":got["spec"],"dry_run":true}),
        )
        .await;
        assert_eq!(
            back,
            api_send(
                &f,
                "root",
                Method::PUT,
                "/api/v2/catalog-configs/extra?dry_run=true",
                Some(got["spec"].to_string())
            )
            .await
        );
        assert_eq!(
            tool_ok(
                &f,
                "root",
                "drop_catalog",
                json!({"name":"extra","dry_run":true})
            )
            .await,
            api_send(
                &f,
                "root",
                Method::DELETE,
                "/api/v2/catalog-configs/extra?dry_run=true",
                None
            )
            .await
        );
        let e = tool_err(&f, "root", "get_catalog_config", json!({"name":"nope"})).await;
        assert_eq!(e["code"], "not_found", "{e}");
        let dropped = tool_ok(&f, "root", "drop_catalog", json!({"name":"extra"})).await;
        assert_eq!(dropped["action"], "catalog.drop", "{dropped}");

        // Settings and re-seed.
        assert_eq!(
            tool_ok(&f, "root", "get_settings", json!({})).await,
            api_get(&f, "root", "/api/v2/settings").await
        );
        assert_eq!(
            tool_ok(
                &f,
                "root",
                "update_settings",
                json!({"slow_query_threshold_ms": 1234, "dry_run": true})
            )
            .await,
            api_send(
                &f,
                "root",
                Method::PATCH,
                "/api/v2/settings?dry_run=true",
                Some(json!({"slow_query_threshold_ms": 1234}).to_string())
            )
            .await
        );
        for bad in [json!({}), json!({"slow_query_threshold_ms": -1})] {
            let e = tool_err(&f, "root", "update_settings", bad).await;
            assert_eq!(e["code"], "invalid_argument", "{e}");
        }
        assert_eq!(
            tool_ok(&f, "root", "reseed_state", json!({"dry_run": true})).await,
            api_send(
                &f,
                "root",
                Method::POST,
                "/api/v2/state/reseed?dry_run=true",
                None
            )
            .await
        );
    }

    #[tokio::test]
    async fn config_tools_refused_for_non_manager() {
        let f = manage_fixture();
        let calls = [
            ("get_access_rules", json!({})),
            ("set_access_rules", json!({"rules": {}})),
            ("clear_access_rules", json!({})),
            ("list_users", json!({})),
            ("create_user", json!({"name":"x","password":"password-1"})),
            (
                "set_user_password",
                json!({"name":"alice","password":"password-1"}),
            ),
            ("delete_user", json!({"name":"alice"})),
            ("list_user_tokens", json!({"name":"alice"})),
            ("issue_user_token", json!({"name":"alice"})),
            (
                "revoke_user_token",
                json!({"name":"alice","id":"0123456789abcdef"}),
            ),
            ("list_catalog_configs", json!({})),
            ("get_catalog_config", json!({"name":"x"})),
            (
                "create_catalog",
                json!({"spec": {"name":"x","type":"hive"}}),
            ),
            (
                "update_catalog",
                json!({"name":"x","spec": {"name":"x","type":"hive"}}),
            ),
            ("drop_catalog", json!({"name":"x"})),
            ("update_settings", json!({"slow_query_threshold_ms": 5})),
            ("reseed_state", json!({})),
        ];
        for (name, args) in &calls {
            let e = tool_err(&f, "bob", name, args.clone()).await;
            assert_eq!(e["code"], "forbidden", "{name}");
            let mut dry = args.clone();
            dry["dry_run"] = json!(true);
            if CONFIG_MANAGE_TOOLS.contains(name) {
                let e = tool_err(&f, "bob", name, dry).await;
                assert_eq!(e["code"], "forbidden", "{name} dry");
            }
        }
        // Nothing changed.
        let users = tool_ok(&f, "root", "list_users", json!({})).await;
        assert_eq!(users["users"].as_array().unwrap().len(), 5, "{users}");
    }

    #[tokio::test]
    async fn create_user_tool_never_echoes_the_password() {
        let f = manage_fixture();
        let pw = "hunter2-not-for-echo";
        let pw2 = "second-secret-pass";
        let mut seen = Vec::new();
        for (tool, args) in [
            (
                "create_user",
                json!({"name":"erin","password":pw,"dry_run":true}),
            ),
            ("create_user", json!({"name":"erin","password":pw})),
            (
                "set_user_password",
                json!({"name":"erin","password":pw2,"dry_run":true}),
            ),
            ("set_user_password", json!({"name":"erin","password":pw2})),
        ] {
            seen.push(tool_raw(&f, "root", tool, args).await.to_string());
        }
        // A refused password (too short) is not echoed either.
        let short = "short";
        seen.push(
            tool_raw(
                &f,
                "root",
                "create_user",
                json!({"name":"frank","password":short}),
            )
            .await
            .to_string(),
        );
        seen.push(
            tool_ok(&f, "root", "list_users", json!({}))
                .await
                .to_string(),
        );
        seen.push(
            tool_ok(&f, "root", "list_audit_log", json!({"limit": 100}))
                .await
                .to_string(),
        );
        for text in &seen {
            for secret in [pw, pw2] {
                assert!(!text.contains(secret), "a password leaked: {text}");
            }
        }
        assert!(seen[4].contains("invalid_argument"), "{}", seen[4]);
        assert!(!seen[4].contains("\"short\""), "{}", seen[4]);
        let audit = tool_ok(
            &f,
            "root",
            "list_audit_log",
            json!({"action":"user.create"}),
        )
        .await;
        assert_eq!(audit["entries"][0]["target"], "erin", "{audit}");
    }
}
