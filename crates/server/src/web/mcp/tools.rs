//! The MCP tools. Each runs as the request's [`Identity`](super::Identity)
//! and makes the same service call, with the same validation, as the
//! matching `/api/v2` route.

use std::time::Duration;

use arneb_protocol::agent::{
    AgentError, AgentLimits, AgentQueryService, ExplainRequest, ListRequest, StatementStatus,
    SubmitRequest,
};
use rmcp::{
    handler::server::wrapper::Parameters,
    model::CallToolResult,
    service::{RequestContext, RoleServer},
    tool, tool_router, ErrorData,
};
use serde::{Deserialize, Serialize};

use super::{identity, ok, tool_error, ArnebMcp};
use crate::web::agent::{service, wait};
use crate::web::api::{self, BoundedParams, BoundedQuery};
use crate::web::auth::AuthVia;
use crate::web::error::ApiError;
use crate::web::v2;

/// Arguments of `list_catalogs`.
#[derive(Deserialize, schemars::JsonSchema)]
struct ListCatalogsArgs {
    /// Keep only names starting with this (case-insensitive).
    prefix: Option<String>,
    /// Most names to return (default 1000, at most 10000).
    limit: Option<usize>,
}

/// Arguments of `list_schemas`.
#[derive(Deserialize, schemars::JsonSchema)]
struct ListSchemasArgs {
    /// The catalog to list.
    catalog: String,
    /// Keep only names starting with this (case-insensitive).
    prefix: Option<String>,
    /// Most names to return (default 1000, at most 10000).
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
    /// Most names to return (default 1000, at most 10000).
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
    /// Rows to return (default 10, at most 100).
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
    /// Rows to keep for fetching (default 1000, at most 10000).
    max_rows: Option<usize>,
    /// Milliseconds to wait for the statement to finish before answering
    /// (default 0, at most 10000).
    wait_ms: Option<u64>,
}

/// Arguments of `get_query_status`.
#[derive(Deserialize, schemars::JsonSchema)]
struct StatusArgs {
    /// The statement id from `submit_query`.
    id: String,
    /// Milliseconds to wait for the statement to finish before answering
    /// (default 0, at most 10000).
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
    /// Most bytes of rows in the page (default 32768, at most 4194304).
    max_bytes: Option<usize>,
}

/// Arguments of `cancel_query`.
#[derive(Deserialize, schemars::JsonSchema)]
struct IdArgs {
    /// The statement id from `submit_query`.
    id: String,
}

/// Arguments of `list_queries`.
#[derive(Deserialize, schemars::JsonSchema)]
struct ListQueriesArgs {
    /// Keep only this state: queued, running, finished, failed or cancelled.
    state: Option<String>,
    /// Keep only queries of this user.
    user: Option<String>,
    /// Keep only queries that took (or have taken) at least this many
    /// milliseconds: the slow-query view.
    min_elapsed_ms: Option<u64>,
    /// Most queries to return (default 50, at most 500).
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

/// A tool result for a service outcome: the body, or the error envelope.
fn respond<T: Serialize>(outcome: Result<T, ApiError>) -> Result<CallToolResult, ErrorData> {
    match outcome {
        Ok(body) => ok(&body),
        Err(e) => Ok(tool_error(e)),
    }
}

impl ArnebMcp {
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

    /// The MCP page budget of inline first pages and omitted `max_bytes`.
    fn page_bytes(&self, svc: &AgentQueryService) -> usize {
        self.state
            .mcp
            .as_ref()
            .map_or(svc.limits().default_page_bytes, |m| m.default_page_bytes)
    }

    /// `status` with its inline `first_page` re-paged to the MCP budget.
    /// A cancel that lands between status and results surfaces as an error result;
    /// that outcome is acceptable and not swallowed.
    fn repage(
        &self,
        svc: &AgentQueryService,
        user: &str,
        mut status: StatementStatus,
    ) -> Result<StatementStatus, AgentError> {
        if status.first_page.is_some() {
            let page = svc.results(user, &status.id, 0, Some(self.page_bytes(svc)))?;
            status.first_page = Some(page);
        }
        Ok(status)
    }
}

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
    /// `list_catalogs`: `GET /api/v2/catalogs`.
    #[tool(
        description = "List the catalogs you can see, sorted, with the total and whether the \
                       list was cut at `limit` (default 1000, at most 10000). Next: \
                       list_schemas.",
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
                       list was cut at `limit` (default 1000, at most 10000). Next: list_tables.",
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
                       list was cut at `limit` (default 1000, at most 10000). Next: \
                       describe_table.",
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
                    .describe_table(&who.user, &a.catalog, &a.schema, &a.table)
                    .await?)
            }
            .await,
        )
    }

    /// `sample_table`: `GET …/tables/{t}/sample`.
    #[tool(
        description = "Read a few rows of a table (default 10, at most 100) and return the \
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
                let status = svc
                    .sample(&who.user, &a.catalog, &a.schema, &a.table, a.rows)
                    .await?;
                Ok(self.repage(svc, &who.user, status)?)
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
        respond(
            async {
                let req = ExplainRequest {
                    sql: a.sql,
                    catalog: a.catalog,
                    schema: a.schema,
                };
                Ok(service(&self.state)?.explain(&who.user, &req).await?)
            }
            .await,
        )
    }

    /// `submit_query`: `POST /api/v2/statements`.
    #[tool(
        description = "Run a SQL statement. Returns its status; with `wait_ms` (at most 10000) \
                       a short query is already finished, with a first page of rows. At most \
                       `max_rows` rows are kept and `truncated` says if more existed. A failed \
                       statement is a status with state `failed` and an error. Next: \
                       get_query_status until finished, then fetch_results.",
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
                let status = svc.submit(&who.user, req, wait).await?;
                Ok(self.repage(svc, &who.user, status)?)
            }
            .await,
        )
    }

    /// `get_query_status`: `GET /api/v2/statements/{id}`.
    #[tool(
        description = "Get the status of a statement you submitted, optionally waiting up to \
                       `wait_ms` (at most 10000) for it to finish. A finished one carries the \
                       columns and a first page of rows. Next: fetch_results while \
                       `next_cursor` is set, or cancel_query to stop it.",
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
                let status = svc.status(&who.user, &a.id, wait).await?;
                Ok(self.repage(svc, &who.user, status)?)
            }
            .await,
        )
    }

    /// `fetch_results`: `GET /api/v2/statements/{id}/results`.
    #[tool(
        description = "Fetch a page of a finished statement's rows from `cursor`, at most \
                       `max_bytes` of rows (default 32768, at most 4194304; long cells are \
                       cut). Repeat with `next_cursor` until it is null.",
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

    /// `cancel_query`: `DELETE /api/v2/statements/{id}`.
    #[tool(
        description = "Cancel a statement you submitted. Safe to repeat; a statement that has \
                       already ended keeps its state. Returns the status after cancelling.",
        annotations(
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn cancel_query(
        &self,
        Parameters(a): Parameters<IdArgs>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let who = identity(&ctx)?;
        respond(async { Ok(service(&self.state)?.cancel(&who.user, &a.id)?) }.await)
    }

    /// `list_queries`: the bounded `GET /api/v2/queries`.
    #[tool(
        description = "List tracked queries (any protocol) you may see, newest first, at most \
                       `limit` (default 50, at most 500); `truncated` says if more matched. \
                       Filter by `state` (queued, running, finished, failed, cancelled), `user`, \
                       or `min_elapsed_ms` for slow queries. Long `sql` is cut and flagged with \
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
        identity(&ctx)?;
        ok(&api::cluster_response(&self.state))
    }

    /// `list_workers`: `GET /api/v2/cluster/workers`.
    #[tool(
        description = "List the alive workers with address, version, heartbeat age and whether \
                       they are active or draining.",
        annotations(read_only_hint = true, open_world_hint = false)
    )]
    async fn list_workers(
        &self,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        identity(&ctx)?;
        ok(&serde_json::json!({"workers": api::workers_response(&self.state)}))
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
        respond(v2::list_tokens_for(&self.state, &who))
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
        respond(v2::issue_token_for(&self.state, &who, &headers, req).await)
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
            v2::revoke_token_for(&self.state, &who, a.id.clone())
                .await
                .map(|()| serde_json::json!({"revoked": a.id})),
        )
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
        let mut registry = ConnectorRegistry::new();
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
        let (cm, registry) = engine_state();
        let mut agent =
            AgentQueryService::new(cm, registry, tracker.clone(), AgentLimits::default());
        if hang {
            agent = agent.with_distributed_executor(Arc::new(HangingExecutor));
        }
        let state = WebState {
            query_tracker: tracker.clone(),
            node_registry: arneb_scheduler::NodeRegistry::new(30),
            start_time: std::time::Instant::now(),
            role: "standalone".into(),
            admission: None,
            max_queued_queries: 8,
            memory_pool: Arc::new(arneb_common::memory_pool::GreedyMemoryPool::new(1 << 20)),
            memory_limit_bytes: None,
            basic: None,
            credentials: None,
            state: None,
            access_rules: None,
            metrics_auth: false,
            sessions: SessionStore::new(),
            agent: Some(agent),
            mcp: Some(McpSettings {
                allowed_hosts: None,
                default_page_bytes: page_bytes,
            }),
        };
        Fixture {
            router: build_router(state),
            tracker,
        }
    }

    /// A `tools/call` as `user`; the whole JSON-RPC response.
    async fn tool_raw(f: &Fixture, user: &str, name: &str, args: Value) -> Value {
        let body = json!({"jsonrpc":"2.0","id":1,"method":"tools/call",
                          "params":{"name":name,"arguments":args}});
        let req = axum::http::Request::builder()
            .method(Method::POST)
            .uri("/mcp")
            .header(header::HOST, "127.0.0.1:6432")
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::ACCEPT, "application/json, text/event-stream")
            .header("mcp-protocol-version", "2025-06-18")
            .header("x-arneb-user", user)
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
        let req = axum::http::Request::builder()
            .method(Method::GET)
            .uri(path)
            .header("x-arneb-user", user)
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
        for name in ["get_query_status", "fetch_results", "cancel_query"] {
            let e = tool_err(&f, "bob", name, json!({"id": id})).await;
            assert_eq!(e["code"], "not_found", "{name}");
        }
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
        assert_eq!(f.tracker.list_queries(None).len(), before);
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
        let r = tool_ok(&f, "alice", "list_queries", json!({"user": "carol"})).await;
        assert!(r["queries"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn slow_queries_filter() {
        let f = history_fixture();
        let slow = f
            .tracker
            .create_query_from("SELECT slow".into(), "pgwire", None);
        tokio::time::sleep(Duration::from_millis(300)).await;
        f.tracker
            .create_query_from("SELECT fast".into(), "pgwire", None);
        let r = tool_ok(&f, "alice", "list_queries", json!({"min_elapsed_ms": 200})).await;
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
        let w = tool_ok(&f, "alice", "list_workers", json!({})).await;
        assert_eq!(
            w["workers"],
            api_get(&f, "alice", "/api/v2/cluster/workers").await
        );
    }
}
