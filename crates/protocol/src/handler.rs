use std::fmt::Debug;
use std::sync::Arc;

use arneb_catalog::{CatalogManager, TablePrivilege};
use arneb_common::error::{ArnebError, ExecutionError};
use arneb_common::stream::collect_stream;
use arneb_common::types::ScalarValue;
use arneb_connectors::ConnectorRegistry;
use arneb_execution::{ExecutionContext, ExecutionPlan};
use arneb_planner::{JoinCondition, LogicalOptimizer, LogicalPlan, PlanExpr, QueryPlanner};
use arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int32Array, Int64Array, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};
use arrow::datatypes::DataType as ArrowDataType;
use async_recursion::async_recursion;
use async_trait::async_trait;
use futures::stream;
use futures::Sink;
use pgwire::api::portal::Portal;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    DescribePortalResponse, DescribeResponse, DescribeStatementResponse, FieldFormat, FieldInfo,
    QueryResponse, Response,
};
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::api::{ClientInfo, ClientPortalStore, NoopHandler, PgWireServerHandlers, Type};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;

use crate::auth::{AuthMethod, AuthStartupHandler};
use crate::encoding::{column_info_to_field_info, encode_record_batches};
use crate::session::{handle_search_path, SearchPath};

/// Trait for distributed query execution. Implemented by QueryCoordinator
/// in the server crate and injected into the protocol handler.
#[async_trait]
pub trait DistributedExecutor: Send + Sync {
    /// Execute a query plan distributedly across workers.
    async fn execute(
        &self,
        plan: LogicalPlan,
        exec_ctx: &ExecutionContext,
    ) -> Result<Vec<arrow::record_batch::RecordBatch>, ArnebError>;

    /// Check if workers are available for distributed execution.
    fn has_workers(&self) -> bool;
}
use crate::error::{arneb_error_to_pg_error, arneb_error_to_pg_error_with_source};
use arneb_common::diagnostic::SourceFile;

fn distribute_scalar_subquery_enabled() -> bool {
    // Default ON: run uncorrelated scalar subqueries through the distributed
    // executor instead of pre-evaluating them single-node on the coordinator.
    // q11's HAVING subquery materialized its 24M partsupp join on the
    // coordinator (3.2 GB peak); distributing it drops the coordinator to
    // ~144 MB and flips q11 both axes. `ARNEB_DISTRIBUTE_SCALAR_SUBQUERY=0`
    // disables. Test builds read the env fresh so per-test overrides don't
    // collide on the cached OnceLock.
    #[cfg(test)]
    {
        std::env::var("ARNEB_DISTRIBUTE_SCALAR_SUBQUERY")
            .map(|v| v != "0")
            .unwrap_or(true)
    }
    #[cfg(not(test))]
    {
        static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
        *ENABLED.get_or_init(|| {
            let enabled = std::env::var("ARNEB_DISTRIBUTE_SCALAR_SUBQUERY")
                .map(|v| v != "0")
                .unwrap_or(true);
            tracing::info!(
                target: "arneb::config",
                distribute_scalar_subquery = enabled,
                "ARNEB_DISTRIBUTE_SCALAR_SUBQUERY effective value (default on; =0 to disable)"
            );
            enabled
        })
    }
}

fn arrow_type_to_pg(dt: &arrow::datatypes::DataType) -> Type {
    use arrow::datatypes::DataType as ADT;
    match dt {
        ADT::Boolean => Type::BOOL,
        ADT::Int64 => Type::INT8,
        ADT::Int32 => Type::INT4,
        ADT::Float64 => Type::FLOAT8,
        ADT::Utf8 => Type::VARCHAR,
        _ => Type::TEXT,
    }
}

/// Factory that creates per-connection handlers with shared state.
pub struct HandlerFactory {
    pub catalog_manager: Arc<CatalogManager>,
    pub connector_registry: Arc<ConnectorRegistry>,
    pub distributed_executor: Option<Arc<dyn DistributedExecutor>>,
    pub memory_pool: Arc<dyn arneb_execution::memory_pool::MemoryPool>,
    /// Per-query memory limit on this node (`query_max_memory_per_node`);
    /// each statement runs under its own `QueryMemoryPool` over
    /// `memory_pool`. `None`: limited only by the node pool.
    pub query_memory_limit: Option<usize>,
    /// Admission control shared with the Trino listener; `None` admits
    /// every statement at once.
    pub admission: Option<arneb_scheduler::AdmissionController>,
    /// Server default `statement_timeout` for sessions that set none.
    pub statement_timeout: Option<std::time::Duration>,
    /// Client authentication mode enforced during connection startup. Covers
    /// both the Simple and Extended Query paths, which share one startup
    /// handler.
    pub auth: AuthMethod,
    /// Reject StartupMessages on connections that did not negotiate TLS
    /// (`[tls] pgwire = "required"`).
    pub tls_required: bool,
    /// Records each statement (Web UI, `system.runtime.queries`, `/metrics`).
    pub query_tracker: Arc<arneb_scheduler::QueryTracker>,
    /// Access rules checked for the connection's user; `None` allows all
    /// (file-based-access-control).
    pub access_rules: Option<Arc<arneb_catalog::AccessRules>>,
}

impl PgWireServerHandlers for HandlerFactory {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        Arc::new(ConnectionHandler {
            distributed_executor: self.distributed_executor.clone(),
            catalog_manager: Arc::clone(&self.catalog_manager),
            connector_registry: Arc::clone(&self.connector_registry),
            memory_pool: Arc::clone(&self.memory_pool),
            query_memory_limit: self.query_memory_limit,
            admission: self.admission.clone(),
            statement_timeout: self.statement_timeout,
            query_tracker: self.query_tracker.clone(),
            access_rules: self.access_rules.clone(),
        })
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        Arc::new(ConnectionHandler {
            distributed_executor: self.distributed_executor.clone(),
            catalog_manager: Arc::clone(&self.catalog_manager),
            connector_registry: Arc::clone(&self.connector_registry),
            memory_pool: Arc::clone(&self.memory_pool),
            query_memory_limit: self.query_memory_limit,
            admission: self.admission.clone(),
            statement_timeout: self.statement_timeout,
            query_tracker: self.query_tracker.clone(),
            access_rules: self.access_rules.clone(),
        })
    }

    fn startup_handler(&self) -> Arc<impl pgwire::api::auth::StartupHandler> {
        // pgwire calls this once per connection, so the SCRAM exchange state
        // inside the handler is never shared between clients.
        Arc::new(AuthStartupHandler::new(
            self.auth.clone(),
            self.tls_required,
        ))
    }

    fn copy_handler(&self) -> Arc<impl pgwire::api::copy::CopyHandler> {
        Arc::new(NoopHandler)
    }
}

/// One pgwire statement's entry in the query tracker: `Queued` at receipt,
/// `Running` once admitted, then finished or failed. Dropped before either
/// (the client went away mid-statement) it is recorded as cancelled.
struct TrackedStatement(Option<Tracked>);

type Tracked = (
    Arc<arneb_scheduler::QueryTracker>,
    arneb_common::identifiers::QueryId,
);

impl TrackedStatement {
    fn start(
        tracker: &Arc<arneb_scheduler::QueryTracker>,
        sql: &str,
        user: Option<String>,
    ) -> Self {
        let id = tracker.create_query_from(sql.to_string(), "pgwire", user);
        Self(Some((Arc::clone(tracker), id)))
    }

    fn running(&self) {
        if let Some((t, id)) = &self.0 {
            let _ = t.advance_to(id, arneb_scheduler::QueryState::Running);
        }
    }

    fn finish(&mut self, rows: u64) {
        if let Some((t, id)) = self.0.take() {
            let _ = t.finish_query(&id, rows);
        }
    }

    fn fail(&mut self, error: String) {
        if let Some((t, id)) = self.0.take() {
            let _ = t.fail_query(&id, error);
        }
    }
}

impl Drop for TrackedStatement {
    fn drop(&mut self) {
        if let Some((t, id)) = self.0.take() {
            let _ = t.cancel_query(&id);
        }
    }
}

/// Per-connection handler that processes queries through the full pipeline.
pub struct ConnectionHandler {
    pub catalog_manager: Arc<CatalogManager>,
    pub connector_registry: Arc<ConnectorRegistry>,
    pub distributed_executor: Option<Arc<dyn DistributedExecutor>>,
    /// Memory pool installed by [`crate::ProtocolServer::with_memory_pool`].
    /// Threaded into every `ExecutionContext` created by this handler so
    /// spillable operators (SemiJoinExec build) honour the configured
    /// per-task budget instead of growing unbounded.
    pub memory_pool: Arc<dyn arneb_execution::memory_pool::MemoryPool>,
    /// Per-query memory limit; see [`HandlerFactory::query_memory_limit`].
    pub query_memory_limit: Option<usize>,
    /// Admission control; see [`HandlerFactory::admission`].
    pub admission: Option<arneb_scheduler::AdmissionController>,
    /// Server default timeout; see [`HandlerFactory::statement_timeout`].
    pub statement_timeout: Option<std::time::Duration>,
    /// See [`HandlerFactory::query_tracker`].
    pub query_tracker: Arc<arneb_scheduler::QueryTracker>,
    /// See [`HandlerFactory::access_rules`].
    pub access_rules: Option<Arc<arneb_catalog::AccessRules>>,
}

impl ConnectionHandler {
    /// The connection's effective `statement_timeout`.
    fn effective_timeout<C: ClientInfo>(&self, client: &C) -> Option<std::time::Duration> {
        client
            .session_extensions()
            .get::<crate::timeout::SessionTimeout>()
            .map_or(self.statement_timeout, |t| {
                t.effective(self.statement_timeout)
            })
    }

    /// Handle `SET` / `SHOW` / `RESET statement_timeout`, and reset it on
    /// `RESET ALL` (which then falls through to `search_path`). An invalid
    /// value is `ERROR 22023`.
    fn timeout_command<C: ClientInfo>(
        &self,
        client: &C,
        sql: &str,
    ) -> Option<PgWireResult<Response>> {
        use crate::timeout::{Command, SessionTimeout};
        if crate::timeout::is_reset_all(sql) {
            client
                .session_extensions()
                .insert(SessionTimeout::default());
            return None;
        }
        let command = match crate::timeout::parse(sql)? {
            Ok(c) => c,
            Err(msg) => {
                return Some(Err(PgWireError::UserError(Box::new(
                    pgwire::error::ErrorInfo::new("ERROR".to_owned(), "22023".to_owned(), msg),
                ))))
            }
        };
        let response = match command.tag() {
            None => crate::timeout::show_result(self.effective_timeout(client)),
            Some(tag) => {
                let explicit = match command {
                    Command::Set(value) => Some(value),
                    _ => None,
                };
                client
                    .session_extensions()
                    .insert(SessionTimeout { explicit });
                Ok(crate::metadata::MetadataResponse::Command(tag.to_owned()))
            }
        };
        Some(
            response
                .map_err(|e| {
                    arneb_error_to_pg_error(&ArnebError::Execution(
                        ExecutionError::InvalidOperation(e),
                    ))
                })
                .and_then(metadata_to_response),
        )
    }

    /// Run `work` (admission wait plus execution) under the connection's
    /// `statement_timeout`, timed from statement receipt. On expiry the
    /// work future is dropped, which cancels the query on its workers, and
    /// the client gets `ERROR 57014`.
    async fn with_timeout<C: ClientInfo, T>(
        &self,
        client: &C,
        work: impl std::future::Future<Output = PgWireResult<T>>,
    ) -> PgWireResult<T> {
        match self.effective_timeout(client) {
            None => work.await,
            Some(limit) => match tokio::time::timeout(limit, work).await {
                Ok(result) => result,
                Err(_) => Err(PgWireError::UserError(Box::new(
                    pgwire::error::ErrorInfo::new(
                        "ERROR".to_owned(),
                        "57014".to_owned(),
                        "canceling statement due to statement timeout".to_owned(),
                    ),
                ))),
            },
        }
    }

    /// Wait for an admission slot when `sql` executes a plan. The permit is
    /// held until the statement's results are produced; a full queue is
    /// `ERROR 53000`.
    async fn admit(&self, sql: &str) -> PgWireResult<Option<arneb_scheduler::AdmissionPermit>> {
        let Some(controller) = &self.admission else {
            return Ok(None);
        };
        if !needs_admission(sql) {
            return Ok(None);
        }
        match controller.admit_with(|| admission_label(sql)) {
            Ok(pending) => Ok(Some(pending.wait().await)),
            Err(e) => Err(PgWireError::UserError(Box::new(
                pgwire::error::ErrorInfo::new(
                    "ERROR".to_owned(),
                    "53000".to_owned(),
                    e.to_string(),
                ),
            ))),
        }
    }

    /// Admit and execute one statement under the session's timeout,
    /// recording it in the query tracker. `system.runtime.queries` reads
    /// run over a snapshot of the tracker instead of the real catalogs.
    async fn run_statement<C: ClientInfo>(
        &self,
        client: &C,
        sql: &str,
        catalog_manager: &Arc<CatalogManager>,
    ) -> PgWireResult<
        Result<
            (
                Arc<dyn ExecutionPlan>,
                Vec<arrow::record_batch::RecordBatch>,
            ),
            ArnebError,
        >,
    > {
        let user = client.metadata().get(pgwire::api::METADATA_USER).cloned();
        let mut tracked = TrackedStatement::start(&self.query_tracker, sql, user);
        let result = self
            .with_timeout(client, async {
                let _admission = self.admit(sql).await?;
                tracked.running();
                // Snapshot after admission, so the reading statement shows
                // itself as RUNNING.
                let runtime = crate::runtime_queries::references_runtime_queries(sql).then(|| {
                    crate::runtime_queries::runtime_catalog(&self.query_tracker, catalog_manager)
                });
                let (cm, registry, executor) = match &runtime {
                    Some((cm, registry)) => (cm, registry, None),
                    None => (
                        catalog_manager.as_ref(),
                        self.connector_registry.as_ref(),
                        self.distributed_executor.as_deref(),
                    ),
                };
                Ok(execute_query_with_limit(
                    sql,
                    cm,
                    registry,
                    executor,
                    &self.memory_pool,
                    self.query_memory_limit,
                )
                .await)
            })
            .await;
        match &result {
            Ok(Ok((_, batches))) => {
                tracked.finish(batches.iter().map(|b| b.num_rows() as u64).sum())
            }
            Ok(Err(e)) => tracked.fail(e.to_string()),
            // Admission refusal or statement timeout.
            Err(e) => tracked.fail(e.to_string()),
        }
        result
    }

    /// The catalog manager resolving unqualified names for this connection:
    /// a session view when `SET search_path` chose a non-default schema,
    /// checked against the access rules for the connection's user. Fails
    /// when the rules do not let the user run statements.
    fn session_catalog<C: ClientInfo>(&self, client: &C) -> PgWireResult<Arc<CatalogManager>> {
        let base = client.session_extensions().get::<SearchPath>().map_or_else(
            || Arc::clone(&self.catalog_manager),
            |p| Arc::clone(&p.catalog_manager),
        );
        let Some(rules) = &self.access_rules else {
            return Ok(base);
        };
        let user = client
            .metadata()
            .get(pgwire::api::METADATA_USER)
            .cloned()
            .unwrap_or_default();
        let view = base.with_access(user, Arc::clone(rules));
        view.check_execute()
            .map_err(|e| arneb_error_to_pg_error(&e.into()))?;
        Ok(Arc::new(view))
    }

    /// Answer a metadata statement: a `pg_catalog` read runs in the engine
    /// over the session's catalog tables (pg-catalog-tables), falling back
    /// to the fixed result when the engine cannot run it; everything else
    /// goes to the fixed handlers.
    async fn metadata<C: ClientInfo>(
        &self,
        client: &C,
        sql: &str,
        catalog_manager: &CatalogManager,
    ) -> Option<crate::metadata::MetadataResult> {
        if crate::pg_catalog::references_pg_catalog(sql) {
            let error = match crate::pg_catalog::run(sql, catalog_manager, &self.memory_pool).await
            {
                Ok(response) => return Some(Ok(response)),
                Err(error) => error,
            };
            let Some(fixed) = crate::metadata::fixed_pg_catalog(sql, catalog_manager).await else {
                return Some(Err(error.to_string()));
            };
            arneb_common::metrics::pg_catalog_fallback();
            tracing::warn!(
                target: "arneb::protocol",
                %error,
                sql,
                "pg_catalog statement answered with the fixed result"
            );
            return Some(fixed);
        }
        let user = client.metadata().get(pgwire::api::METADATA_USER).cloned();
        crate::metadata::try_handle_metadata(sql, catalog_manager, user.as_deref()).await
    }

    /// Handle `SET`/`SHOW`/`RESET search_path` (updating the connection's
    /// state), then fall back to metadata interception.
    async fn intercept<C: ClientInfo + Sync>(
        &self,
        client: &C,
        sql: &str,
        catalog_manager: &CatalogManager,
    ) -> Option<crate::metadata::MetadataResult> {
        let current = client.session_extensions().get::<SearchPath>();
        if let Some((result, new_path)) = handle_search_path(
            sql,
            &self.catalog_manager,
            catalog_manager,
            current.as_deref(),
        )
        .await
        {
            if let Some(path) = new_path {
                client.session_extensions().insert(path);
            }
            return Some(result);
        }
        self.metadata(client, sql, catalog_manager).await
    }

    /// Like [`Self::intercept`] but without updating the connection's state,
    /// for Describe: it must report the same columns Execute will return.
    async fn describe_intercept<C: ClientInfo + Sync>(
        &self,
        client: &C,
        sql: &str,
        catalog_manager: &CatalogManager,
    ) -> Option<crate::metadata::MetadataResult> {
        match crate::timeout::parse(sql) {
            Some(Ok(command)) => {
                return Some(match command.tag() {
                    Some(tag) => Ok(crate::metadata::MetadataResponse::Command(tag.to_owned())),
                    None => crate::timeout::show_result(self.effective_timeout(client)),
                });
            }
            // Execute reports the invalid value; Describe answers `SET`.
            Some(Err(_)) => {
                return Some(Ok(crate::metadata::MetadataResponse::Command(
                    "SET".to_owned(),
                )))
            }
            None => {}
        }
        let current = client.session_extensions().get::<SearchPath>();
        if let Some((result, _)) = handle_search_path(
            sql,
            &self.catalog_manager,
            catalog_manager,
            current.as_deref(),
        )
        .await
        {
            return Some(result);
        }
        self.metadata(client, sql, catalog_manager).await
    }
}

#[async_trait]
impl SimpleQueryHandler for ConnectionHandler {
    async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let trimmed = query.trim();
        if trimmed.is_empty() {
            return Ok(vec![Response::EmptyQuery]);
        }

        tracing::debug!(query = trimmed, "processing simple query");
        if let Some(response) = self.timeout_command(client, trimmed) {
            return response.map(|r| vec![r]);
        }

        // Intercept session commands and metadata queries (pg_catalog,
        // information_schema, version()).
        let catalog_manager = self.session_catalog(client)?;
        if let Some(meta_result) = self.intercept(client, trimmed, &catalog_manager).await {
            return match meta_result {
                Ok(resp) => metadata_to_response(resp).map(|r| vec![r]),
                Err(e) => Err(arneb_error_to_pg_error(&ArnebError::Execution(
                    arneb_common::error::ExecutionError::InvalidOperation(e),
                ))),
            };
        }

        // Capture the submitted SQL as a `SourceFile` so diagnostic
        // rendering can point at the exact token that failed. pgwire
        // error responses carry the rendered text in the message body;
        // SQLSTATE codes are unaffected.
        let source = SourceFile::new("<query>", trimmed);

        let result = self
            .run_statement(client, trimmed, &catalog_manager)
            .await?;

        match result {
            Ok((plan, batches)) => {
                let columns = plan.schema();
                let field_info = column_info_to_field_info(&columns);
                let schema = Arc::new(field_info);

                let (rows, _row_count) = encode_record_batches(&schema, &batches)?;

                let data_row_stream = stream::iter(rows);
                let response = Response::Query(QueryResponse::new(schema, data_row_stream));

                Ok(vec![response])
            }
            Err(err) => Err(arneb_error_to_pg_error_with_source(&err, Some(&source))),
        }
    }
}

// ---------------------------------------------------------------------------
// Extended Query protocol (Parse → Bind → Describe → Execute → Sync)
// ---------------------------------------------------------------------------

/// Substitute `$1`, `$2`, ... placeholders with parameter values, each
/// already a SQL literal (see [`extract_params`]).
fn bind_parameters(sql: &str, params: &[String]) -> String {
    let mut result = sql.to_string();
    // Replace in reverse order ($10 before $1) to avoid partial matches
    for (i, literal) in params.iter().enumerate().rev() {
        result = result.replace(&format!("${}", i + 1), literal);
    }
    result
}

/// A portal's parameters as SQL literals. A parameter is decoded by the type
/// the client declared and the format it chose: psycopg 3 sends integers,
/// floats and booleans in binary, JDBC and tokio-postgres mostly in text.
fn extract_params(portal: &Portal<String>) -> PgWireResult<Vec<String>> {
    (0..portal.parameter_len())
        .map(|i| {
            let declared = portal
                .statement
                .parameter_types
                .get(i)
                .cloned()
                .flatten()
                .unwrap_or(Type::UNKNOWN);
            Ok(param_literal(portal, i, &declared)?.unwrap_or_else(|| "NULL".to_string()))
        })
        .collect()
}

/// One parameter as a SQL literal, `None` for NULL. A binary value of a type
/// Arneb does not decode is an error, never a silent NULL.
fn param_literal(
    portal: &Portal<String>,
    i: usize,
    declared: &Type,
) -> PgWireResult<Option<String>> {
    macro_rules! get {
        ($t:ty, $ty:expr) => {
            portal.parameter::<$t>(i, $ty)?
        };
    }
    let quoted = |v: String| format!("'{}'", v.replace('\'', "''"));
    if portal.parameter_format.is_binary(i) {
        return Ok(match *declared {
            Type::INT2 => get!(i16, declared).map(|v| v.to_string()),
            Type::INT4 => get!(i32, declared).map(|v| v.to_string()),
            Type::INT8 => get!(i64, declared).map(|v| v.to_string()),
            Type::FLOAT4 => get!(f32, declared).map(|v| v.to_string()),
            Type::FLOAT8 => get!(f64, declared).map(|v| v.to_string()),
            Type::BOOL => get!(bool, declared).map(bool_literal),
            Type::DATE => get!(chrono::NaiveDate, declared).map(|d| format!("DATE '{d}'")),
            Type::TIMESTAMP => get!(chrono::NaiveDateTime, declared).map(timestamp_literal),
            Type::TIMESTAMPTZ => get!(chrono::DateTime<chrono::FixedOffset>, declared)
                .map(|t| timestamp_literal(t.naive_utc())),
            Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME | Type::UNKNOWN => {
                get!(String, &Type::TEXT).map(quoted)
            }
            ref other => {
                return Err(PgWireError::UserError(Box::new(
                    pgwire::error::ErrorInfo::new(
                        "ERROR".to_owned(),
                        "0A000".to_owned(),
                        format!(
                            "parameter ${} has binary type {}, which is not supported; \
                             send it in text format",
                            i + 1,
                            other.name()
                        ),
                    ),
                )))
            }
        });
    }
    let Some(value) = get!(String, &Type::TEXT) else {
        return Ok(None);
    };
    Ok(Some(match *declared {
        Type::BOOL => match value.to_ascii_lowercase().as_str() {
            "t" | "true" | "y" | "yes" | "on" | "1" => bool_literal(true),
            "f" | "false" | "n" | "no" | "off" | "0" => bool_literal(false),
            _ => quoted(value),
        },
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME => quoted(value),
        // Declared numeric, or untyped: a number stays a number.
        _ if value.parse::<f64>().is_ok() => value,
        _ => quoted(value),
    }))
}

fn bool_literal(value: bool) -> String {
    if value { "TRUE" } else { "FALSE" }.to_string()
}

fn timestamp_literal(value: chrono::NaiveDateTime) -> String {
    format!("TIMESTAMP '{}'", value.format("%Y-%m-%d %H:%M:%S%.f"))
}

/// Plan a SQL string (without executing) to obtain output column schema.
async fn plan_for_schema(
    sql: &str,
    catalog_manager: &CatalogManager,
) -> Result<Vec<pgwire::api::results::FieldInfo>, PgWireError> {
    let statement = arneb_sql_parser::parse(sql).map_err(|e| arneb_error_to_pg_error(&e.into()))?;
    let planner = QueryPlanner::new(catalog_manager);
    let plan = planner
        .plan_statement(&statement)
        .await
        .map_err(|e| arneb_error_to_pg_error(&e.into()))?;
    Ok(column_info_to_field_info(&plan.schema()))
}

/// Plan a placeholder-bearing SQL string for the describe path and
/// return both the output schema and the parameter-type map inferred
/// by the analyzer. Called by `do_describe_statement` to report
/// concrete `ParameterDescription` OIDs instead of the legacy TEXT
/// fallback.
async fn plan_for_describe(
    sql: &str,
    catalog_manager: &CatalogManager,
) -> Result<
    (
        Vec<pgwire::api::results::FieldInfo>,
        std::collections::HashMap<usize, arneb_common::types::DataType>,
    ),
    PgWireError,
> {
    let statement = arneb_sql_parser::parse(sql).map_err(|e| arneb_error_to_pg_error(&e.into()))?;
    let planner = QueryPlanner::new(catalog_manager);
    let (plan, ctx) = planner
        .plan_statement_with_context(&statement)
        .await
        .map_err(|e| arneb_error_to_pg_error(&e.into()))?;
    Ok((column_info_to_field_info(&plan.schema()), ctx.param_types))
}

/// Map an Arneb `DataType` to a PostgreSQL [`Type`] OID for
/// `ParameterDescription`. Covers the types the analyzer infers
/// today; anything unknown falls back to `Type::TEXT` (matching
/// Postgres' `unknown` → `text` convention).
fn arneb_type_to_pg_param_type(t: &arneb_common::types::DataType) -> Type {
    use arneb_common::types::DataType as DT;
    match t {
        DT::Boolean => Type::BOOL,
        DT::Int32 => Type::INT4,
        DT::Int64 => Type::INT8,
        DT::Float32 => Type::FLOAT4,
        DT::Float64 => Type::FLOAT8,
        DT::Utf8 | DT::LargeUtf8 => Type::VARCHAR,
        DT::Date32 => Type::DATE,
        DT::Timestamp { timezone, .. } => {
            if timezone.is_some() {
                Type::TIMESTAMPTZ
            } else {
                Type::TIMESTAMP
            }
        }
        DT::Binary => Type::BYTEA,
        DT::Decimal128 { .. } => Type::NUMERIC,
        _ => Type::TEXT,
    }
}

#[async_trait]
impl ExtendedQueryHandler for ConnectionHandler {
    type Statement = String;
    type QueryParser = NoopQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        Arc::new(NoopQueryParser)
    }

    async fn do_query<C>(
        &self,
        client: &mut C,
        portal: &Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: pgwire::api::store::PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let sql = &&portal.statement.statement;
        let params = extract_params(portal)?;
        let bound_sql = bind_parameters(sql, &params);
        let trimmed = bound_sql.trim();

        if trimmed.is_empty() {
            return Ok(Response::EmptyQuery);
        }

        tracing::debug!(query = trimmed, "processing extended query");
        if let Some(response) = self.timeout_command(client, trimmed) {
            return response;
        }

        // Intercept session commands and metadata queries
        let catalog_manager = self.session_catalog(client)?;
        if let Some(meta_result) = self.intercept(client, trimmed, &catalog_manager).await {
            return match meta_result {
                Ok(resp) => metadata_to_response(resp),
                Err(e) => Err(arneb_error_to_pg_error(&ArnebError::Execution(
                    arneb_common::error::ExecutionError::InvalidOperation(e),
                ))),
            };
        }

        let source = SourceFile::new("<query>", trimmed);
        let result = self
            .run_statement(client, trimmed, &catalog_manager)
            .await?;

        match result {
            Ok((plan, batches)) => {
                let columns = plan.schema();
                let field_info = column_info_to_field_info(&columns);
                let schema = Arc::new(field_info);
                let (rows, _row_count) = encode_record_batches(&schema, &batches)?;
                let data_row_stream = stream::iter(rows);
                Ok(Response::Query(QueryResponse::new(schema, data_row_stream)))
            }
            Err(err) => Err(arneb_error_to_pg_error_with_source(&err, Some(&source))),
        }
    }

    async fn do_describe_statement<C>(
        &self,
        client: &mut C,
        target: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: pgwire::api::store::PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let sql = &target.statement;
        if sql.trim().is_empty() {
            return Ok(DescribeStatementResponse::no_data());
        }
        let catalog_manager = self.session_catalog(client)?;
        let param_count = count_placeholders(sql);
        let describe = |fields, param_type_map: std::collections::HashMap<_, _>| {
            let param_types: Vec<Type> = (1..=param_count)
                .map(|i| {
                    param_type_map
                        .get(&i)
                        .map(arneb_type_to_pg_param_type)
                        .unwrap_or(Type::TEXT)
                })
                .collect();
            DescribeStatementResponse::new(param_types, fields)
        };

        // A pg_catalog statement is planned over its catalog tables, so its
        // parameters get types (pg-catalog-tables).
        if crate::pg_catalog::references_pg_catalog(sql) {
            let (pg_catalog, _) = crate::pg_catalog::catalog_for(sql, &catalog_manager).await;
            if let Ok((fields, param_type_map)) = plan_for_describe(sql, &pg_catalog).await {
                return Ok(describe(fields, param_type_map));
            }
        }

        // Intercept metadata queries for Describe too
        if let Some(Ok(crate::metadata::MetadataResponse::Query(fields, _))) = self
            .describe_intercept(client, sql.trim(), &catalog_manager)
            .await
        {
            let field_info: Vec<FieldInfo> = fields
                .iter()
                .map(|f| {
                    let pg_type = arrow_type_to_pg(f.data_type());
                    FieldInfo::new(f.name().clone(), None, None, pg_type, FieldFormat::Text)
                })
                .collect();
            return Ok(DescribeStatementResponse::new(vec![], field_info));
        }
        if let Some(Ok(crate::metadata::MetadataResponse::Command(_))) = self
            .describe_intercept(client, sql.trim(), &catalog_manager)
            .await
        {
            return Ok(DescribeStatementResponse::no_data());
        }

        // Count parameter placeholders. The analyzer infers types
        // during planning and returns them in `ctx.param_types` —
        // we translate them to PG OIDs here. Any indices the
        // analyzer didn't see (e.g., because planning failed)
        // fall back to `Type::TEXT` to preserve the legacy behavior
        // for clients that don't care about precise types.
        let (fields, param_type_map) = plan_for_describe(sql, &catalog_manager)
            .await
            .unwrap_or_else(|_| (Vec::new(), std::collections::HashMap::new()));
        Ok(describe(fields, param_type_map))
    }

    async fn do_describe_portal<C>(
        &self,
        client: &mut C,
        target: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: pgwire::api::store::PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let sql = &target.statement.statement;
        let params = extract_params(target)?;
        let bound_sql = bind_parameters(sql, &params);
        let trimmed = bound_sql.trim();

        if trimmed.is_empty() {
            return Ok(DescribePortalResponse::no_data());
        }
        let catalog_manager = self.session_catalog(client)?;

        // Intercept metadata queries
        if let Some(Ok(crate::metadata::MetadataResponse::Query(fields, _))) = self
            .describe_intercept(client, trimmed, &catalog_manager)
            .await
        {
            let field_info: Vec<FieldInfo> = fields
                .iter()
                .map(|f| {
                    let pg_type = arrow_type_to_pg(f.data_type());
                    FieldInfo::new(f.name().clone(), None, None, pg_type, FieldFormat::Text)
                })
                .collect();
            return Ok(DescribePortalResponse::new(field_info));
        }
        if let Some(Ok(crate::metadata::MetadataResponse::Command(_))) = self
            .describe_intercept(client, trimmed, &catalog_manager)
            .await
        {
            return Ok(DescribePortalResponse::no_data());
        }

        let fields = plan_for_schema(trimmed, &catalog_manager)
            .await
            .unwrap_or_default();
        Ok(DescribePortalResponse::new(fields))
    }
}

/// Count `$N` placeholders in SQL.
fn count_placeholders(sql: &str) -> usize {
    let mut max_idx = 0usize;
    let bytes = sql.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'$' {
            i += 1;
            let mut num = 0usize;
            while i < bytes.len() && bytes[i].is_ascii_digit() {
                num = num * 10 + (bytes[i] - b'0') as usize;
                i += 1;
            }
            if num > max_idx {
                max_idx = num;
            }
        } else {
            i += 1;
        }
    }
    max_idx
}

/// A session command's result as a pgwire response.
fn metadata_to_response(resp: crate::metadata::MetadataResponse) -> PgWireResult<Response> {
    match resp {
        crate::metadata::MetadataResponse::Query(fields, batches) => {
            let field_info: Vec<FieldInfo> = fields
                .iter()
                .map(|f| {
                    FieldInfo::new(
                        f.name().clone(),
                        None,
                        None,
                        arrow_type_to_pg(f.data_type()),
                        FieldFormat::Text,
                    )
                })
                .collect();
            let schema = Arc::new(field_info);
            let (rows, _) = encode_record_batches(&schema, &batches)?;
            Ok(Response::Query(QueryResponse::new(
                schema,
                stream::iter(rows),
            )))
        }
        crate::metadata::MetadataResponse::Command(tag) => {
            Ok(Response::Execution(pgwire::api::results::Tag::new(&tag)))
        }
    }
}

/// Whether `sql` executes a plan and so takes an admission slot
/// (admission-control D5): queries, `INSERT`, `DELETE`, CTAS and
/// `EXPLAIN ANALYZE`. Catalog-only DDL and plain `EXPLAIN` never queue.
/// SQL the arneb parser rejects is admitted (fail closed): the caller must
/// resolve statements it handles itself (session, metadata, Trino
/// `EXECUTE`) before asking.
pub fn needs_admission(sql: &str) -> bool {
    use arneb_sql_parser::ast::Statement;
    match arneb_sql_parser::parse(sql) {
        Ok(Statement::Explain { analyze, .. }) => analyze,
        Ok(
            Statement::CreateTable { .. }
            | Statement::DropTable { .. }
            | Statement::CreateView { .. }
            | Statement::DropView { .. },
        ) => false,
        Ok(_) | Err(_) => true,
    }
}

/// A short, single-line form of `sql` for admission logs.
/// Built in one allocation, and only for a query that has to queue.
pub(crate) fn admission_label(sql: &str) -> String {
    const MAX_CHARS: usize = 80;
    let mut label = String::with_capacity(sql.len().min(MAX_CHARS * 4) + 3);
    let flat = sql
        .split_whitespace()
        .enumerate()
        .flat_map(|(i, word)| (i > 0).then_some(' ').into_iter().chain(word.chars()));
    for (i, c) in flat.enumerate() {
        if i == MAX_CHARS {
            label.push('…');
            break;
        }
        label.push(c);
    }
    label
}

/// Execute the full query pipeline asynchronously, with no per-query memory
/// limit beyond `memory_pool` itself.
pub async fn execute_query(
    sql: &str,
    catalog_manager: &CatalogManager,
    connector_registry: &ConnectorRegistry,
    distributed_executor: Option<&dyn DistributedExecutor>,
    memory_pool: &Arc<dyn arneb_execution::memory_pool::MemoryPool>,
) -> Result<
    (
        Arc<dyn ExecutionPlan>,
        Vec<arrow::record_batch::RecordBatch>,
    ),
    ArnebError,
> {
    execute_query_with_limit(
        sql,
        catalog_manager,
        connector_registry,
        distributed_executor,
        memory_pool,
        None,
    )
    .await
}

/// [`execute_query`] under a per-query memory limit: the statement's local
/// execution (a standalone plan, or the coordinator's own fragments) reserves
/// from a `QueryMemoryPool` of `query_memory_limit` bytes over
/// `memory_pool`, dropped when the statement ends.
pub async fn execute_query_with_limit(
    sql: &str,
    catalog_manager: &CatalogManager,
    connector_registry: &ConnectorRegistry,
    distributed_executor: Option<&dyn DistributedExecutor>,
    memory_pool: &Arc<dyn arneb_execution::memory_pool::MemoryPool>,
    query_memory_limit: Option<usize>,
) -> Result<
    (
        Arc<dyn ExecutionPlan>,
        Vec<arrow::record_batch::RecordBatch>,
    ),
    ArnebError,
> {
    execute_query_capped(
        sql,
        catalog_manager,
        connector_registry,
        distributed_executor,
        memory_pool,
        query_memory_limit,
        None,
    )
    .await
}

/// Caps the rows a query plan produces at `row_cap`: an existing root
/// `LIMIT` keeps the smaller of the two, anything else gets a `LIMIT` on top.
/// The cap goes below root projections (which keep the row count), so the
/// physical planner can turn it into a Top-K over a sort or a limit hint over
/// a bare scan.
fn cap_plan_rows(plan: LogicalPlan, row_cap: usize) -> LogicalPlan {
    match plan {
        LogicalPlan::Projection {
            input,
            exprs,
            schema,
        } => LogicalPlan::Projection {
            input: Box::new(cap_plan_rows(*input, row_cap)),
            exprs,
            schema,
        },
        LogicalPlan::Limit {
            input,
            limit,
            offset,
        } => LogicalPlan::Limit {
            input,
            limit: Some(limit.map_or(row_cap, |l| l.min(row_cap))),
            offset,
        },
        plan => LogicalPlan::Limit {
            input: Box::new(plan),
            limit: Some(row_cap),
            offset: None,
        },
    }
}

/// Checks the write privilege a DDL/DML plan needs on its target table
/// (`INSERT`, `DELETE`, or `OWNERSHIP` for create/drop), recursing through
/// `EXPLAIN` because analysis is where access is checked. Reads need
/// nothing here: planning already checked every `SELECT` source.
pub(crate) fn check_write_privileges(
    plan: &LogicalPlan,
    catalogs: &CatalogManager,
) -> Result<(), ArnebError> {
    use TablePrivilege::{Delete, Insert, Ownership};
    let (name, privilege, verb) = match plan {
        LogicalPlan::Explain { input, .. } => return check_write_privileges(input, catalogs),
        LogicalPlan::InsertInto { table, .. } => (table, Insert, "insert into"),
        LogicalPlan::DeleteFrom { table, .. } => (table, Delete, "delete from"),
        LogicalPlan::CreateTable { name, .. } | LogicalPlan::CreateTableAsSelect { name, .. } => {
            (name, Ownership, "create")
        }
        LogicalPlan::DropTable { name, .. } => (name, Ownership, "drop"),
        LogicalPlan::CreateView { name, .. } => (name, Ownership, "create view"),
        LogicalPlan::DropView { name, .. } => (name, Ownership, "drop view"),
        _ => return Ok(()),
    };
    catalogs.check_table_privilege(name, privilege, verb)?;
    Ok(())
}

/// [`execute_query_with_limit`] that, for a `SELECT`, stops producing rows
/// after `row_cap` of them: the plan's root gets a `LIMIT row_cap`, so the
/// query reads no more than it needs to return that many rows. Other
/// statements ignore `row_cap`.
pub(crate) async fn execute_query_capped(
    sql: &str,
    catalog_manager: &CatalogManager,
    connector_registry: &ConnectorRegistry,
    distributed_executor: Option<&dyn DistributedExecutor>,
    memory_pool: &Arc<dyn arneb_execution::memory_pool::MemoryPool>,
    query_memory_limit: Option<usize>,
    row_cap: Option<usize>,
) -> Result<
    (
        Arc<dyn ExecutionPlan>,
        Vec<arrow::record_batch::RecordBatch>,
    ),
    ArnebError,
> {
    let memory_pool: Arc<dyn arneb_execution::memory_pool::MemoryPool> = match query_memory_limit {
        Some(limit) => Arc::new(arneb_execution::memory_pool::QueryMemoryPool::new(
            Arc::clone(memory_pool),
            limit,
        )),
        None => Arc::clone(memory_pool),
    };
    let memory_pool = &memory_pool;
    // Step 1: Parse SQL
    let statement = arneb_sql_parser::parse(sql)?;

    // Step 2: Plan
    //
    // A2.2 (2026-05-28): use `plan_statement_with_context` so the
    // analyzer's `AnalyzerContext.catalog_stats` snapshot survives
    // into the execution context. The fragmenter's broadcast-join
    // decision reads it via `ExecutionContext::catalog_stats()`.
    // `plan_statement(&statement)` discards stats; keeping them
    // costs only an `Arc` clone.
    let planner = QueryPlanner::new(catalog_manager);
    let (logical_plan, analyzer_ctx) = planner.plan_statement_with_context(&statement).await?;
    let logical_plan = match (row_cap, &statement) {
        (Some(cap), arneb_sql_parser::ast::Statement::Query { .. }) => {
            cap_plan_rows(logical_plan, cap)
        }
        _ => logical_plan,
    };

    check_write_privileges(&logical_plan, catalog_manager)?;

    // Step 2.5: Optimize logical plan
    let optimizer = LogicalOptimizer::default_rules();
    let logical_plan = optimizer.optimize(logical_plan)?;

    // Step 3: Create execution context with the active memory pool
    // (Phase 2c: server-side budget wiring). The pool flows through to
    // every spillable operator built off this context — currently the
    // SemiJoinExec build phase. If the pool is `UnboundedMemoryPool`,
    // operators see no budget; if it's `GreedyMemoryPool(N)`, builds
    // will spill to disk when they would exceed N bytes.
    //
    // A2.2: attach the analyzer's `CatalogStats` snapshot so the
    // distributed fragmenter has stats for broadcast eligibility.
    // `broadcast_max_build_bytes` stays `None` (the A2.2-landed
    // default); A2.4 measurement plumbs it through here.
    let mut exec_ctx = ExecutionContext::new()
        .with_memory_pool(memory_pool.clone())
        .with_catalog_stats(Some(analyzer_ctx.catalog_stats.clone()))
        // A2.x broadcast threshold (2026-05-28). Default OFF.
        //
        // A2.1+A2.2+A2.3 v1 infra ships in this commit but is dormant
        // until A2.3 v2 (parallel probe) lands. A2.4 SF10 17q bench at
        // 100 MiB threshold delivered Q09 18% win vs OFF but regressed
        // Q14/Q19 17-40% — A2.3 v1's `FragmentType::Fixed` collapse on
        // every broadcast-eligible join trades probe-stage parallelism
        // for RepartitionExec channel savings. A2.4b re-bench at 5 MiB
        // (only tiny dims qualify) lost most of the Q09 win without
        // recovering Q14/Q19 — confirming the trade-off is structural,
        // not a threshold-tuning problem.
        //
        // Broadcast v2 (2026-06-03): the fragmenter no longer collapses a
        // broadcast-eligible join to Fixed/Single — it keeps the probe
        // N-way and only broadcasts the build, so enabling this is now
        // correct + parallel. Runtime override `ARNEB_BROADCAST_MAX_BUILD_BYTES`
        // (bytes) drives the A/B; defaults to the validated 1 GB cap, with
        // an explicit zero disabling broadcast.
        .with_broadcast_max_build_bytes(match std::env::var("ARNEB_BROADCAST_MAX_BUILD_BYTES") {
            Ok(value) => value.parse::<usize>().ok().filter(|&bytes| bytes > 0),
            Err(_) => Some(1_000_000_000),
        });
    register_data_sources(
        &logical_plan,
        catalog_manager,
        connector_registry,
        &mut exec_ctx,
    )
    .await?;

    // Step 3.5: Resolve scalar subqueries in expressions (pre-evaluate them)
    let logical_plan =
        resolve_plan_subqueries(&exec_ctx, distributed_executor, logical_plan).await?;

    // Step 3.6: Check for distributed execution
    if let Some(executor) = distributed_executor {
        if executor.has_workers() {
            tracing::info!("routing query to distributed executor");
            let batches = executor.execute(logical_plan, &exec_ctx).await?;
            // Create local physical plan just for schema (not executed)
            // Fall through to local if distributed fails
            let local_plan = exec_ctx.create_physical_plan(
                &optimizer.optimize(planner.plan_statement(&statement).await?)?,
            )?;
            return Ok((local_plan, batches));
        }
    }

    // Step 4: Create physical plan (local execution)
    let physical_plan = exec_ctx.create_physical_plan(&logical_plan)?;

    // Step 5: Wrap the root in CoalescePartitionsExec when it would
    // otherwise expose multiple output partitions. Most operators
    // already coalesce their inputs; raw `SELECT * FROM scan_table`
    // does not because the planner leaves the ScanExec exposed for
    // downstream Repartition. The pgwire path drains a single stream
    // — without the coalesce we'd silently drop N-1 partitions.
    let physical_plan: Arc<dyn arneb_execution::ExecutionPlan> =
        if physical_plan.output_partitioning().partition_count() > 1 {
            Arc::new(arneb_execution::CoalescePartitionsExec::new(physical_plan))
        } else {
            physical_plan
        };

    // Step 6: Execute (async). After the wrap above, top-level output
    // is always single-partition.
    let stream = physical_plan.execute(0).await?;
    let batches = collect_stream(stream).await?;

    Ok((physical_plan, batches))
}

/// Walk a logical plan and resolve any scalar subqueries in expressions.
async fn resolve_plan_subqueries(
    ctx: &ExecutionContext,
    distributed_executor: Option<&dyn DistributedExecutor>,
    plan: LogicalPlan,
) -> Result<LogicalPlan, ArnebError> {
    match plan {
        LogicalPlan::Filter { input, predicate } => {
            let input =
                Box::pin(resolve_plan_subqueries(ctx, distributed_executor, *input)).await?;
            let predicate =
                resolve_expr_subqueries_distributed(ctx, distributed_executor, predicate).await?;
            Ok(LogicalPlan::Filter {
                input: Box::new(input),
                predicate,
            })
        }
        LogicalPlan::Projection {
            input,
            exprs,
            schema,
        } => {
            let input =
                Box::pin(resolve_plan_subqueries(ctx, distributed_executor, *input)).await?;
            let mut resolved = Vec::with_capacity(exprs.len());
            for expr in exprs {
                let r =
                    resolve_expr_subqueries_distributed(ctx, distributed_executor, expr).await?;
                resolved.push(r);
            }
            Ok(LogicalPlan::Projection {
                input: Box::new(input),
                exprs: resolved,
                schema,
            })
        }
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggr_exprs,
            schema,
        } => {
            let input =
                Box::pin(resolve_plan_subqueries(ctx, distributed_executor, *input)).await?;
            Ok(LogicalPlan::Aggregate {
                input: Box::new(input),
                group_by,
                aggr_exprs,
                schema,
            })
        }
        LogicalPlan::Sort { input, order_by } => {
            let input =
                Box::pin(resolve_plan_subqueries(ctx, distributed_executor, *input)).await?;
            Ok(LogicalPlan::Sort {
                input: Box::new(input),
                order_by,
            })
        }
        LogicalPlan::Limit {
            input,
            limit,
            offset,
        } => {
            let input =
                Box::pin(resolve_plan_subqueries(ctx, distributed_executor, *input)).await?;
            Ok(LogicalPlan::Limit {
                input: Box::new(input),
                limit,
                offset,
            })
        }
        LogicalPlan::Join {
            left,
            right,
            join_type,
            condition,
            dynamic_filter_ids,
        } => {
            let left = Box::pin(resolve_plan_subqueries(ctx, distributed_executor, *left)).await?;
            let right =
                Box::pin(resolve_plan_subqueries(ctx, distributed_executor, *right)).await?;
            // A1.5 (2026-05-27): preserve cross-fragment DF
            // annotations. Stripping them here (previous behaviour)
            // silently broke A1.5 producers — the worker's
            // HashJoinExec.dynamic_filter_producers came out empty,
            // build phase published nothing, probe-side scans timed
            // out for 10 s before falling back to static filters.
            Ok(LogicalPlan::Join {
                left: Box::new(left),
                right: Box::new(right),
                join_type,
                condition,
                dynamic_filter_ids,
            })
        }
        LogicalPlan::SemiJoin {
            left,
            right,
            left_key,
            right_key,
            residual,
            dynamic_filter_ids,
            ..
        } => {
            // Recurse into BOTH children — without this, scalar
            // subqueries pushed into SemiJoin.left by PredicatePushdown
            // (TPC-H Q22) never reach `resolve_scalar_subqueries`,
            // and FilterExec then panics with "scalar subquery requires
            // pre-evaluation at operator level".
            let left = Box::pin(resolve_plan_subqueries(ctx, distributed_executor, *left)).await?;
            let right =
                Box::pin(resolve_plan_subqueries(ctx, distributed_executor, *right)).await?;
            Ok(LogicalPlan::SemiJoin {
                left: Box::new(left),
                right: Box::new(right),
                left_key,
                right_key,
                residual,
                // A1.5 (2026-05-27): preserve (see Join arm above).
                dynamic_filter_ids,
                probe_side_filter_ids: Vec::new(),
            })
        }
        LogicalPlan::AntiJoin {
            left,
            right,
            left_key,
            right_key,
            residual,
            ..
        } => {
            let left = Box::pin(resolve_plan_subqueries(ctx, distributed_executor, *left)).await?;
            let right =
                Box::pin(resolve_plan_subqueries(ctx, distributed_executor, *right)).await?;
            Ok(LogicalPlan::AntiJoin {
                left: Box::new(left),
                right: Box::new(right),
                left_key,
                right_key,
                residual,
                probe_side_filter_ids: Vec::new(),
            })
        }
        // For other plan types, return as-is
        other => Ok(other),
    }
}

async fn resolve_expr_subqueries_distributed(
    ctx: &ExecutionContext,
    distributed_executor: Option<&dyn DistributedExecutor>,
    expr: PlanExpr,
) -> Result<PlanExpr, ArnebError> {
    let executor = distributed_executor
        .filter(|executor| distribute_scalar_subquery_enabled() && executor.has_workers());
    let resolved = resolve_expr_subqueries_distributed_inner(ctx, executor, expr).await?;
    ctx.resolve_scalar_subqueries(&resolved)
        .await
        .map_err(ArnebError::Execution)
}

/// Resolve the scalar subqueries in `expr`. With an `executor`, uncorrelated
/// ones run on the workers and become literals. Without one, only the
/// subqueries nested *inside* each subquery's plan are resolved (TPC-H Q11's
/// `SUM(..) / (SELECT COUNT(*) ..)`), leaving the outer one for
/// `ExecutionContext::resolve_scalar_subqueries`.
async fn resolve_expr_subqueries_distributed_inner(
    ctx: &ExecutionContext,
    executor: Option<&dyn DistributedExecutor>,
    expr: PlanExpr,
) -> Result<PlanExpr, ArnebError> {
    match expr {
        PlanExpr::ScalarSubquery { subplan, span } => {
            let subplan = Box::pin(resolve_plan_subqueries(ctx, executor, *subplan)).await?;
            let Some(executor) = executor.filter(|_| is_uncorrelated(&subplan)) else {
                return Ok(PlanExpr::ScalarSubquery {
                    subplan: Box::new(subplan),
                    span,
                });
            };
            let batches = executor.execute(subplan.clone(), ctx).await?;
            let value = scalar_value_from_batches(&batches)?;
            Ok(PlanExpr::Literal { value, span })
        }
        PlanExpr::BinaryOp {
            left,
            op,
            right,
            span,
        } => {
            let left = Box::pin(resolve_expr_subqueries_distributed_inner(
                ctx, executor, *left,
            ))
            .await?;
            let right = Box::pin(resolve_expr_subqueries_distributed_inner(
                ctx, executor, *right,
            ))
            .await?;
            Ok(PlanExpr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
                span,
            })
        }
        PlanExpr::UnaryOp { op, expr, span } => {
            let expr = Box::pin(resolve_expr_subqueries_distributed_inner(
                ctx, executor, *expr,
            ))
            .await?;
            Ok(PlanExpr::UnaryOp {
                op,
                expr: Box::new(expr),
                span,
            })
        }
        PlanExpr::Function {
            name,
            args,
            distinct,
            span,
        } => {
            let mut resolved_args = Vec::with_capacity(args.len());
            for arg in args {
                resolved_args.push(
                    Box::pin(resolve_expr_subqueries_distributed_inner(
                        ctx, executor, arg,
                    ))
                    .await?,
                );
            }
            Ok(PlanExpr::Function {
                name,
                args: resolved_args,
                distinct,
                span,
            })
        }
        PlanExpr::IsNull { expr, span } => {
            let expr = Box::pin(resolve_expr_subqueries_distributed_inner(
                ctx, executor, *expr,
            ))
            .await?;
            Ok(PlanExpr::IsNull {
                expr: Box::new(expr),
                span,
            })
        }
        PlanExpr::IsNotNull { expr, span } => {
            let expr = Box::pin(resolve_expr_subqueries_distributed_inner(
                ctx, executor, *expr,
            ))
            .await?;
            Ok(PlanExpr::IsNotNull {
                expr: Box::new(expr),
                span,
            })
        }
        PlanExpr::Between {
            expr,
            negated,
            low,
            high,
            span,
        } => {
            let expr = Box::pin(resolve_expr_subqueries_distributed_inner(
                ctx, executor, *expr,
            ))
            .await?;
            let low = Box::pin(resolve_expr_subqueries_distributed_inner(
                ctx, executor, *low,
            ))
            .await?;
            let high = Box::pin(resolve_expr_subqueries_distributed_inner(
                ctx, executor, *high,
            ))
            .await?;
            Ok(PlanExpr::Between {
                expr: Box::new(expr),
                negated,
                low: Box::new(low),
                high: Box::new(high),
                span,
            })
        }
        PlanExpr::InList {
            expr,
            list,
            negated,
            span,
        } => {
            let expr = Box::pin(resolve_expr_subqueries_distributed_inner(
                ctx, executor, *expr,
            ))
            .await?;
            let mut resolved_list = Vec::with_capacity(list.len());
            for item in list {
                resolved_list.push(
                    Box::pin(resolve_expr_subqueries_distributed_inner(
                        ctx, executor, item,
                    ))
                    .await?,
                );
            }
            Ok(PlanExpr::InList {
                expr: Box::new(expr),
                list: resolved_list,
                negated,
                span,
            })
        }
        PlanExpr::Cast {
            expr,
            data_type,
            span,
        } => {
            let expr = Box::pin(resolve_expr_subqueries_distributed_inner(
                ctx, executor, *expr,
            ))
            .await?;
            Ok(PlanExpr::Cast {
                expr: Box::new(expr),
                data_type,
                span,
            })
        }
        PlanExpr::CaseExpr {
            operand,
            when_clauses,
            else_result,
            span,
        } => {
            let operand = match operand {
                Some(expr) => Some(Box::new(
                    Box::pin(resolve_expr_subqueries_distributed_inner(
                        ctx, executor, *expr,
                    ))
                    .await?,
                )),
                None => None,
            };
            let mut resolved_when_clauses = Vec::with_capacity(when_clauses.len());
            for (condition, result) in when_clauses {
                let condition = Box::pin(resolve_expr_subqueries_distributed_inner(
                    ctx, executor, condition,
                ))
                .await?;
                let result = Box::pin(resolve_expr_subqueries_distributed_inner(
                    ctx, executor, result,
                ))
                .await?;
                resolved_when_clauses.push((condition, result));
            }
            let else_result = match else_result {
                Some(expr) => Some(Box::new(
                    Box::pin(resolve_expr_subqueries_distributed_inner(
                        ctx, executor, *expr,
                    ))
                    .await?,
                )),
                None => None,
            };
            Ok(PlanExpr::CaseExpr {
                operand,
                when_clauses: resolved_when_clauses,
                else_result,
                span,
            })
        }
        PlanExpr::Column { .. }
        | PlanExpr::Literal { .. }
        | PlanExpr::Wildcard
        | PlanExpr::Parameter { .. } => Ok(expr),
    }
}

fn is_uncorrelated(plan: &LogicalPlan) -> bool {
    plan_columns_in_range(plan)
}

fn plan_columns_in_range(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::TableScan { .. } | LogicalPlan::ExchangeNode { .. } | LogicalPlan::OneRow => {
            true
        }
        LogicalPlan::Projection { input, exprs, .. } => {
            let width = input.schema().len();
            plan_columns_in_range(input)
                && exprs.iter().all(|expr| expr_columns_in_range(expr, width))
        }
        LogicalPlan::Filter { input, predicate } => {
            let width = input.schema().len();
            plan_columns_in_range(input) && expr_columns_in_range(predicate, width)
        }
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggr_exprs,
            ..
        }
        | LogicalPlan::PartialAggregate {
            input,
            group_by,
            aggr_exprs,
            ..
        }
        | LogicalPlan::FinalAggregate {
            input,
            group_by,
            aggr_exprs,
            ..
        } => {
            let width = input.schema().len();
            plan_columns_in_range(input)
                && group_by
                    .iter()
                    .all(|expr| expr_columns_in_range(expr, width))
                && aggr_exprs
                    .iter()
                    .all(|expr| expr_columns_in_range(expr, width))
        }
        LogicalPlan::Sort { input, order_by } => {
            let width = input.schema().len();
            plan_columns_in_range(input)
                && order_by
                    .iter()
                    .all(|sort_expr| expr_columns_in_range(&sort_expr.expr, width))
        }
        LogicalPlan::Limit { input, .. }
        | LogicalPlan::Explain { input, .. }
        | LogicalPlan::Distinct { input }
        | LogicalPlan::AssignUniqueId { input, .. } => plan_columns_in_range(input),
        LogicalPlan::Join {
            left,
            right,
            condition,
            ..
        } => {
            let width = left.schema().len() + right.schema().len();
            plan_columns_in_range(left)
                && plan_columns_in_range(right)
                && match condition {
                    JoinCondition::On(expr) => expr_columns_in_range(expr, width),
                    JoinCondition::None => true,
                }
        }
        LogicalPlan::SemiJoin {
            left,
            right,
            left_key,
            right_key,
            residual,
            ..
        }
        | LogicalPlan::AntiJoin {
            left,
            right,
            left_key,
            right_key,
            residual,
            ..
        } => {
            let left_width = left.schema().len();
            let right_width = right.schema().len();
            plan_columns_in_range(left)
                && plan_columns_in_range(right)
                && expr_columns_in_range(left_key, left_width)
                && expr_columns_in_range(right_key, right_width)
                && residual
                    .as_ref()
                    .map(|expr| expr_columns_in_range(expr, left_width + right_width))
                    .unwrap_or(true)
        }
        LogicalPlan::ScalarSubquery { subplan } => plan_columns_in_range(subplan),
        LogicalPlan::UnionAll { inputs } => inputs.iter().all(plan_columns_in_range),
        LogicalPlan::Intersect { left, right } | LogicalPlan::Except { left, right } => {
            plan_columns_in_range(left) && plan_columns_in_range(right)
        }
        LogicalPlan::CreateTableAsSelect { source, .. }
        | LogicalPlan::InsertInto { source, .. } => plan_columns_in_range(source),
        LogicalPlan::CreateView { plan, .. } => plan_columns_in_range(plan),
        LogicalPlan::Window { input, functions } => {
            let width = input.schema().len();
            plan_columns_in_range(input)
                && functions.iter().all(|func| {
                    func.args
                        .iter()
                        .all(|expr| expr_columns_in_range(expr, width))
                        && func
                            .partition_by
                            .iter()
                            .all(|expr| expr_columns_in_range(expr, width))
                        && func
                            .order_by
                            .iter()
                            .all(|sort_expr| expr_columns_in_range(&sort_expr.expr, width))
                })
        }
        LogicalPlan::CreateTable { .. }
        | LogicalPlan::DropTable { .. }
        | LogicalPlan::DeleteFrom { .. }
        | LogicalPlan::DropView { .. } => true,
    }
}

fn expr_columns_in_range(expr: &PlanExpr, input_width: usize) -> bool {
    match expr {
        PlanExpr::Column { index, .. } => *index < input_width,
        PlanExpr::Literal { .. } | PlanExpr::Wildcard | PlanExpr::Parameter { .. } => true,
        PlanExpr::BinaryOp { left, right, .. } => {
            expr_columns_in_range(left, input_width) && expr_columns_in_range(right, input_width)
        }
        PlanExpr::UnaryOp { expr, .. }
        | PlanExpr::IsNull { expr, .. }
        | PlanExpr::IsNotNull { expr, .. }
        | PlanExpr::Cast { expr, .. } => expr_columns_in_range(expr, input_width),
        PlanExpr::Function { args, .. } => args
            .iter()
            .all(|expr| expr_columns_in_range(expr, input_width)),
        PlanExpr::Between {
            expr, low, high, ..
        } => {
            expr_columns_in_range(expr, input_width)
                && expr_columns_in_range(low, input_width)
                && expr_columns_in_range(high, input_width)
        }
        PlanExpr::InList { expr, list, .. } => {
            expr_columns_in_range(expr, input_width)
                && list
                    .iter()
                    .all(|item| expr_columns_in_range(item, input_width))
        }
        PlanExpr::ScalarSubquery { subplan, .. } => plan_columns_in_range(subplan),
        PlanExpr::CaseExpr {
            operand,
            when_clauses,
            else_result,
            ..
        } => {
            operand
                .as_ref()
                .map(|expr| expr_columns_in_range(expr, input_width))
                .unwrap_or(true)
                && when_clauses.iter().all(|(condition, result)| {
                    expr_columns_in_range(condition, input_width)
                        && expr_columns_in_range(result, input_width)
                })
                && else_result
                    .as_ref()
                    .map(|expr| expr_columns_in_range(expr, input_width))
                    .unwrap_or(true)
        }
    }
}

fn scalar_value_from_batches(
    batches: &[arrow::record_batch::RecordBatch],
) -> Result<ScalarValue, ArnebError> {
    let total_rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    if total_rows > 1 {
        return Err(ArnebError::Execution(ExecutionError::InvalidOperation(
            "scalar subquery must return at most one row".to_string(),
        )));
    }
    if total_rows == 0 || batches.is_empty() {
        return Ok(ScalarValue::Null);
    }
    let column = batches[0].column(0);
    if column.is_null(0) {
        return Ok(ScalarValue::Null);
    }
    Ok(arrow_to_scalar_value(column, 0))
}

fn arrow_to_scalar_value(array: &arrow::array::ArrayRef, row: usize) -> ScalarValue {
    match array.data_type() {
        ArrowDataType::Boolean => ScalarValue::Boolean(
            array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("BooleanArray")
                .value(row),
        ),
        ArrowDataType::Int32 => ScalarValue::Int32(
            array
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("Int32Array")
                .value(row),
        ),
        ArrowDataType::Int64 => ScalarValue::Int64(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Int64Array")
                .value(row),
        ),
        ArrowDataType::Float32 => ScalarValue::Float32(
            array
                .as_any()
                .downcast_ref::<Float32Array>()
                .expect("Float32Array")
                .value(row),
        ),
        ArrowDataType::Float64 => ScalarValue::Float64(
            array
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("Float64Array")
                .value(row),
        ),
        ArrowDataType::Utf8 => ScalarValue::Utf8(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("StringArray")
                .value(row)
                .to_string(),
        ),
        ArrowDataType::Binary => ScalarValue::Binary(
            array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .expect("BinaryArray")
                .value(row)
                .to_vec(),
        ),
        ArrowDataType::Decimal128(precision, scale) => ScalarValue::Decimal128 {
            value: array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .expect("Decimal128Array")
                .value(row),
            precision: *precision,
            scale: *scale,
        },
        ArrowDataType::Date32 => ScalarValue::Date32(
            array
                .as_any()
                .downcast_ref::<Date32Array>()
                .expect("Date32Array")
                .value(row),
        ),
        ArrowDataType::Timestamp(unit, timezone) => {
            let value = match unit {
                arrow::datatypes::TimeUnit::Second => array
                    .as_any()
                    .downcast_ref::<TimestampSecondArray>()
                    .expect("TimestampSecondArray")
                    .value(row),
                arrow::datatypes::TimeUnit::Millisecond => array
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .expect("TimestampMillisecondArray")
                    .value(row),
                arrow::datatypes::TimeUnit::Microsecond => array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .expect("TimestampMicrosecondArray")
                    .value(row),
                arrow::datatypes::TimeUnit::Nanosecond => array
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .expect("TimestampNanosecondArray")
                    .value(row),
            };
            ScalarValue::Timestamp {
                value,
                unit: (*unit).into(),
                timezone: timezone.as_ref().map(|tz| tz.to_string()),
            }
        }
        _ => {
            let value = arrow::util::display::array_value_to_string(array, row).unwrap_or_default();
            ScalarValue::Utf8(value)
        }
    }
}

/// The subplans of the scalar subqueries in `expr`, outermost first.
fn expr_subplans(expr: &PlanExpr) -> Vec<&LogicalPlan> {
    fn walk<'a>(expr: &'a PlanExpr, out: &mut Vec<&'a LogicalPlan>) {
        match expr {
            PlanExpr::ScalarSubquery { subplan, .. } => out.push(subplan),
            PlanExpr::Column { .. }
            | PlanExpr::Literal { .. }
            | PlanExpr::Wildcard
            | PlanExpr::Parameter { .. } => {}
            PlanExpr::BinaryOp { left, right, .. } => {
                walk(left, out);
                walk(right, out);
            }
            PlanExpr::UnaryOp { expr, .. }
            | PlanExpr::IsNull { expr, .. }
            | PlanExpr::IsNotNull { expr, .. }
            | PlanExpr::Cast { expr, .. } => walk(expr, out),
            PlanExpr::Function { args, .. } => args.iter().for_each(|a| walk(a, out)),
            PlanExpr::Between {
                expr, low, high, ..
            } => {
                walk(expr, out);
                walk(low, out);
                walk(high, out);
            }
            PlanExpr::InList { expr, list, .. } => {
                walk(expr, out);
                list.iter().for_each(|item| walk(item, out));
            }
            PlanExpr::CaseExpr {
                operand,
                when_clauses,
                else_result,
                ..
            } => {
                if let Some(operand) = operand {
                    walk(operand, out);
                }
                for (condition, result) in when_clauses {
                    walk(condition, out);
                    walk(result, out);
                }
                if let Some(else_result) = else_result {
                    walk(else_result, out);
                }
            }
        }
    }
    let mut out = Vec::new();
    walk(expr, &mut out);
    out
}

/// Walk the logical plan to find all TableScan nodes and register data sources.
#[allow(clippy::double_must_use)] // async_recursion emits a bare #[must_use]
#[async_recursion]
async fn register_data_sources(
    plan: &LogicalPlan,
    catalog_manager: &CatalogManager,
    registry: &ConnectorRegistry,
    ctx: &mut ExecutionContext,
) -> Result<(), ArnebError> {
    match plan {
        LogicalPlan::TableScan {
            table,
            schema,
            properties,
            ..
        } => {
            let key = table.to_string();
            let connector_name = table
                .catalog
                .as_deref()
                .unwrap_or(catalog_manager.default_catalog());

            if let Some(factory) = registry.get(connector_name) {
                // Surface connector errors (e.g. an Iceberg table with
                // unsupported delete files) instead of degrading them to a
                // generic "data source not found" later.
                let ds = factory
                    .create_data_source(table, schema, properties)
                    .await?;
                ctx.register_data_source(key, ds);
            }
        }
        // A scalar subquery inside an expression may read a table nothing
        // else in the plan scans; without workers it runs locally and needs
        // that table's data source too.
        LogicalPlan::Filter { input, predicate } => {
            for subplan in expr_subplans(predicate) {
                register_data_sources(subplan, catalog_manager, registry, ctx).await?;
            }
            register_data_sources(input, catalog_manager, registry, ctx).await?;
        }
        LogicalPlan::Projection { input, exprs, .. } => {
            for subplan in exprs.iter().flat_map(expr_subplans) {
                register_data_sources(subplan, catalog_manager, registry, ctx).await?;
            }
            register_data_sources(input, catalog_manager, registry, ctx).await?;
        }
        LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Explain { input, .. } => {
            register_data_sources(input, catalog_manager, registry, ctx).await?;
        }
        LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::PartialAggregate { input, .. }
        | LogicalPlan::FinalAggregate { input, .. } => {
            register_data_sources(input, catalog_manager, registry, ctx).await?;
        }
        LogicalPlan::Join { left, right, .. } => {
            register_data_sources(left, catalog_manager, registry, ctx).await?;
            register_data_sources(right, catalog_manager, registry, ctx).await?;
        }
        LogicalPlan::ExchangeNode { .. } => {
            // Exchange nodes don't have table scans — they read from other stages.
        }
        LogicalPlan::SemiJoin { left, right, .. } | LogicalPlan::AntiJoin { left, right, .. } => {
            register_data_sources(left, catalog_manager, registry, ctx).await?;
            register_data_sources(right, catalog_manager, registry, ctx).await?;
        }
        LogicalPlan::ScalarSubquery { subplan } => {
            register_data_sources(subplan, catalog_manager, registry, ctx).await?;
        }
        LogicalPlan::UnionAll { inputs } => {
            for input in inputs {
                register_data_sources(input, catalog_manager, registry, ctx).await?;
            }
        }
        LogicalPlan::Distinct { input }
        | LogicalPlan::Window { input, .. }
        | LogicalPlan::AssignUniqueId { input, .. } => {
            register_data_sources(input, catalog_manager, registry, ctx).await?;
        }
        LogicalPlan::Intersect { left, right } | LogicalPlan::Except { left, right } => {
            register_data_sources(left, catalog_manager, registry, ctx).await?;
            register_data_sources(right, catalog_manager, registry, ctx).await?;
        }
        LogicalPlan::CreateTableAsSelect { source, .. }
        | LogicalPlan::InsertInto { source, .. } => {
            register_data_sources(source, catalog_manager, registry, ctx).await?;
        }
        LogicalPlan::CreateView { plan, .. } => {
            register_data_sources(plan, catalog_manager, registry, ctx).await?;
        }
        LogicalPlan::CreateTable { .. }
        | LogicalPlan::DropTable { .. }
        | LogicalPlan::DeleteFrom { .. }
        | LogicalPlan::DropView { .. }
        | LogicalPlan::OneRow => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arneb_common::types::{ColumnInfo, DataType};
    use arneb_sql_parser::ast;
    use arrow::array::Int64Array;
    use arrow::datatypes::{Field, Schema};
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn admission_label_flattens_and_truncates() {
        assert_eq!(admission_label("  SELECT\n  1\t, 2 "), "SELECT 1 , 2");
        let eighty = "x".repeat(80);
        assert_eq!(admission_label(&eighty), eighty);
        assert_eq!(
            admission_label(&format!("{eighty} y")),
            format!("{eighty}…")
        );
        assert_eq!(
            admission_label(&format!("{} é", "x".repeat(79))),
            format!("{} …", "x".repeat(79))
        );
    }

    struct MockDistributedExecutor {
        batches: Vec<arrow::record_batch::RecordBatch>,
        calls: AtomicUsize,
    }

    #[async_trait]
    impl DistributedExecutor for MockDistributedExecutor {
        async fn execute(
            &self,
            _plan: LogicalPlan,
            _exec_ctx: &ExecutionContext,
        ) -> Result<Vec<arrow::record_batch::RecordBatch>, ArnebError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.batches.clone())
        }

        fn has_workers(&self) -> bool {
            true
        }
    }

    fn col(name: &str, data_type: DataType) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            data_type,
            nullable: true,
        }
    }

    fn one_col_plan() -> LogicalPlan {
        LogicalPlan::Projection {
            input: Box::new(LogicalPlan::OneRow),
            exprs: vec![PlanExpr::Literal {
                value: ScalarValue::Int64(1),
                span: None,
            }],
            schema: vec![col("v", DataType::Int64)],
        }
    }

    fn scalar_subquery(subplan: LogicalPlan) -> PlanExpr {
        PlanExpr::ScalarSubquery {
            subplan: Box::new(subplan),
            span: None,
        }
    }

    #[test]
    fn is_uncorrelated_accepts_self_contained_and_rejects_out_of_range_column() {
        let self_contained = LogicalPlan::Filter {
            input: Box::new(one_col_plan()),
            predicate: PlanExpr::Column {
                index: 0,
                name: "v".to_string(),
                span: None,
            },
        };
        assert!(is_uncorrelated(&self_contained));

        let out_of_range = LogicalPlan::Projection {
            input: Box::new(LogicalPlan::OneRow),
            exprs: vec![PlanExpr::Column {
                index: 0,
                name: "outer_ref".to_string(),
                span: None,
            }],
            schema: vec![col("outer_ref", DataType::Int64)],
        };
        assert!(!is_uncorrelated(&out_of_range));
    }

    #[tokio::test]
    async fn distributed_prepass_replaces_filter_scalar_subquery_with_literal() {
        std::env::set_var("ARNEB_DISTRIBUTE_SCALAR_SUBQUERY", "1");

        let schema = Arc::new(Schema::new(vec![Field::new(
            "v",
            arrow::datatypes::DataType::Int64,
            true,
        )]));
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![42]))],
        )
        .unwrap();
        let executor = MockDistributedExecutor {
            batches: vec![batch],
            calls: AtomicUsize::new(0),
        };
        let plan = LogicalPlan::Filter {
            input: Box::new(LogicalPlan::OneRow),
            predicate: PlanExpr::BinaryOp {
                left: Box::new(scalar_subquery(LogicalPlan::OneRow)),
                op: ast::BinaryOp::Eq,
                right: Box::new(PlanExpr::Literal {
                    value: ScalarValue::Int64(42),
                    span: None,
                }),
                span: None,
            },
        };

        let resolved = resolve_plan_subqueries(&ExecutionContext::new(), Some(&executor), plan)
            .await
            .unwrap();

        assert_eq!(executor.calls.load(Ordering::SeqCst), 1);
        let LogicalPlan::Filter { predicate, .. } = resolved else {
            panic!("expected filter");
        };
        let PlanExpr::BinaryOp { left, .. } = predicate else {
            panic!("expected binary predicate");
        };
        assert_eq!(
            *left,
            PlanExpr::Literal {
                value: ScalarValue::Int64(42),
                span: None,
            }
        );
    }

    #[tokio::test]
    async fn correlated_scalar_subquery_is_left_for_local_resolution() {
        let executor = MockDistributedExecutor {
            batches: vec![],
            calls: AtomicUsize::new(0),
        };
        let correlated_subplan = LogicalPlan::Projection {
            input: Box::new(LogicalPlan::OneRow),
            exprs: vec![PlanExpr::Column {
                index: 0,
                name: "outer_ref".to_string(),
                span: None,
            }],
            schema: vec![col("outer_ref", DataType::Int64)],
        };

        let resolved = resolve_expr_subqueries_distributed_inner(
            &ExecutionContext::new(),
            Some(&executor),
            scalar_subquery(correlated_subplan),
        )
        .await
        .unwrap();

        assert_eq!(executor.calls.load(Ordering::SeqCst), 0);
        assert!(matches!(resolved, PlanExpr::ScalarSubquery { .. }));
    }

    /// Regression: COUNT / MIN / MAX over a filter that matches nothing used
    /// to fail with "expected Int64 but found Null".
    #[tokio::test]
    async fn global_aggregate_over_filter_matching_nothing_returns_typed_nulls() {
        use arneb_connectors::memory::{
            MemoryCatalog, MemoryConnectorFactory, MemorySchema, MemoryTable,
        };

        let batch = arrow::record_batch::RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "o_orderkey",
                ArrowDataType::Int64,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let table = MemoryTable::new(vec![col("o_orderkey", DataType::Int64)], vec![batch]);
        let schema = Arc::new(MemorySchema::new());
        schema.register_table("orders", Arc::new(table));
        let catalog = Arc::new(MemoryCatalog::new());
        catalog.register_schema("default", schema);
        let catalog_manager = CatalogManager::new("memory", "default");
        catalog_manager.register_catalog("memory", catalog.clone());
        let mut registry = ConnectorRegistry::new();
        registry.register(
            "memory",
            Arc::new(MemoryConnectorFactory::new(catalog, "default")),
        );
        let pool: Arc<dyn arneb_execution::memory_pool::MemoryPool> =
            Arc::new(arneb_execution::memory_pool::UnboundedMemoryPool::new());

        let (_, batches) = execute_query(
            "SELECT count(*), min(o_orderkey), max(o_orderkey) FROM orders \
             WHERE o_orderkey > 99999999",
            &catalog_manager,
            &registry,
            None,
            &pool,
        )
        .await
        .unwrap();

        let rows: Vec<_> = batches.iter().filter(|b| b.num_rows() > 0).collect();
        assert_eq!(rows.len(), 1);
        let b = rows[0];
        assert_eq!(b.num_rows(), 1);
        let count = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(count.value(0), 0);
        for i in [1, 2] {
            assert_eq!(b.column(i).data_type(), &ArrowDataType::Int64);
            assert!(b.column(i).is_null(0));
        }
    }

    // -- End-to-end three-valued (Kleene) boolean logic -------------------

    /// Runs `sql` against a memory table `t(a INT NULL, b BOOLEAN NULL)` with
    /// rows (1, TRUE), (NULL, FALSE), (3, NULL), (NULL, NULL), (5, FALSE) and
    /// returns the first non-empty batch.
    async fn kleene_query(sql: &str) -> arrow::record_batch::RecordBatch {
        use arneb_connectors::memory::{
            MemoryCatalog, MemoryConnectorFactory, MemorySchema, MemoryTable,
        };
        use arrow::array::{BooleanArray, Int32Array};
        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("a", arrow::datatypes::DataType::Int32, true),
            Field::new("b", arrow::datatypes::DataType::Boolean, true),
        ]));
        let batch = arrow::record_batch::RecordBatch::try_new(
            arrow_schema,
            vec![
                Arc::new(Int32Array::from(vec![
                    Some(1),
                    None,
                    Some(3),
                    None,
                    Some(5),
                ])),
                Arc::new(BooleanArray::from(vec![
                    Some(true),
                    Some(false),
                    None,
                    None,
                    Some(false),
                ])),
            ],
        )
        .unwrap();
        let table = Arc::new(MemoryTable::new(
            vec![col("a", DataType::Int32), col("b", DataType::Boolean)],
            vec![batch],
        ));
        let schema = Arc::new(MemorySchema::new());
        schema.register_table("t", table);
        let catalog = Arc::new(MemoryCatalog::new());
        catalog.register_schema("default", schema);
        let factory = MemoryConnectorFactory::new(catalog.clone(), "default");
        let cm = CatalogManager::new("memory", "default");
        cm.register_catalog("memory", catalog);
        let mut reg = ConnectorRegistry::new();
        reg.register("memory", Arc::new(factory));

        let pool: Arc<dyn arneb_execution::memory_pool::MemoryPool> =
            Arc::new(arneb_execution::memory_pool::UnboundedMemoryPool::new());
        let (_, batches) = execute_query(sql, &cm, &reg, None, &pool)
            .await
            .unwrap_or_else(|e| panic!("query failed: {sql}: {e}"));
        batches
            .into_iter()
            .find(|b| b.num_rows() > 0)
            .unwrap_or_else(|| panic!("no rows: {sql}"))
    }

    #[tokio::test]
    async fn where_is_null_or_uses_kleene_logic() {
        // Rows 1, 2, 4 qualify. Row 4 is `TRUE OR NULL` = TRUE (non-Kleene
        // OR returned NULL and dropped it). Row 3 is `FALSE OR NULL` = NULL.
        let batch = kleene_query("SELECT count(*) FROM t WHERE a IS NULL OR b").await;
        let n = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("count(*) should be Int64")
            .value(0);
        assert_eq!(n, 3);
    }

    #[tokio::test]
    async fn select_null_boolean_literals_use_kleene_logic() {
        use arrow::array::{Array, BooleanArray};
        let batch =
            kleene_query("SELECT NULL OR TRUE, NULL AND FALSE, NULL AND TRUE, NULL OR FALSE").await;
        let cell = |i: usize| -> Option<bool> {
            let c = batch.column(i);
            if c.is_null(0) {
                return None;
            }
            Some(
                c.as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap_or_else(|| panic!("column {i} is {:?}", c.data_type()))
                    .value(0),
            )
        };
        assert_eq!(cell(0), Some(true), "NULL OR TRUE");
        assert_eq!(cell(1), Some(false), "NULL AND FALSE");
        assert_eq!(cell(2), None, "NULL AND TRUE");
        assert_eq!(cell(3), None, "NULL OR FALSE");
    }

    #[tokio::test]
    async fn nested_loop_join_null_condition_does_not_match() {
        // a = (1, NULL, 3, NULL, 5): only (1,3), (1,5), (3,5) satisfy a < a.
        let count = |b: arrow::record_batch::RecordBatch| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0)
        };
        let inner = kleene_query("SELECT count(*) FROM t t1 JOIN t t2 ON t1.a < t2.a").await;
        assert_eq!(count(inner), 3);
        // Plus one NULL-extended row each for 5 and the two NULLs.
        let left = kleene_query("SELECT count(*) FROM t t1 LEFT JOIN t t2 ON t1.a < t2.a").await;
        assert_eq!(count(left), 6);
    }

    // -- End-to-end: comparisons with a NULL literal ----------------------

    /// Memory table `t(a INT NULL)` with rows 1, NULL, 3.
    fn null_cmp_memory_env() -> (CatalogManager, ConnectorRegistry) {
        use arneb_connectors::memory::{
            MemoryCatalog, MemoryConnectorFactory, MemorySchema, MemoryTable,
        };
        let arrow_schema = Arc::new(Schema::new(vec![Field::new(
            "a",
            arrow::datatypes::DataType::Int32,
            true,
        )]));
        let batch = arrow::record_batch::RecordBatch::try_new(
            arrow_schema,
            vec![Arc::new(arrow::array::Int32Array::from(vec![
                Some(1),
                None,
                Some(3),
            ]))],
        )
        .unwrap();
        let table = Arc::new(MemoryTable::new(
            vec![col("a", DataType::Int32)],
            vec![batch],
        ));
        let schema = Arc::new(MemorySchema::new());
        schema.register_table("t", table);
        let catalog = Arc::new(MemoryCatalog::new());
        catalog.register_schema("default", schema);
        let factory = MemoryConnectorFactory::new(catalog.clone(), "default");
        let catalog_manager = CatalogManager::new("memory", "default");
        catalog_manager.register_catalog("memory", catalog);
        let mut registry = ConnectorRegistry::new();
        registry.register("memory", Arc::new(factory));
        (catalog_manager, registry)
    }

    async fn run_sql(
        sql: &str,
        cm: &CatalogManager,
        reg: &ConnectorRegistry,
    ) -> arrow::record_batch::RecordBatch {
        let pool: Arc<dyn arneb_execution::memory_pool::MemoryPool> =
            Arc::new(arneb_execution::memory_pool::UnboundedMemoryPool::new());
        let (_, batches) = execute_query(sql, cm, reg, None, &pool)
            .await
            .unwrap_or_else(|e| panic!("query failed: {sql}: {e}"));
        batches
            .into_iter()
            .find(|b| b.num_rows() > 0)
            .unwrap_or_else(|| panic!("no rows: {sql}"))
    }

    #[tokio::test]
    async fn comparison_with_null_literal_is_unknown_not_folded() {
        use arrow::array::Array;
        let (cm, reg) = null_cmp_memory_env();
        // A comparison with NULL is NULL, so it never passes a WHERE clause.
        for pred in ["NULL = NULL", "NULL <> NULL", "NULL < NULL", "NULL >= NULL"] {
            let sql = format!("SELECT count(*) FROM t WHERE {pred}");
            let batch = run_sql(&sql, &cm, &reg).await;
            let n = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("count(*) should be Int64")
                .value(0);
            assert_eq!(n, 0, "{pred}");
        }
        let batch = run_sql("SELECT NULL = NULL, NULL <> NULL, 1 = 1", &cm, &reg).await;
        assert!(batch.column(0).is_null(0), "NULL = NULL");
        assert!(batch.column(1).is_null(0), "NULL <> NULL");
        assert!(!batch.column(2).is_null(0), "1 = 1");
    }

    fn count_of(batch: &arrow::record_batch::RecordBatch) -> i64 {
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("count(*) should be Int64")
            .value(0)
    }

    #[tokio::test]
    async fn untyped_null_operand_coerces_end_to_end() {
        use arrow::array::Array;
        let (cm, reg) = null_cmp_memory_env();
        let batch = run_sql("SELECT 1 = NULL, NULL < 2, 1 + NULL", &cm, &reg).await;
        for i in 0..3 {
            assert!(batch.column(i).is_null(0), "column {i}");
        }
        // t.a is (1, NULL, 3).
        for (pred, expected) in [
            ("a = NULL", 0),
            ("NULL <> a", 0),
            ("a + NULL > 1", 0),
            ("a IN (1, NULL)", 1),
            ("a BETWEEN 0 AND NULL", 0),
            ("a IS NULL", 1),
        ] {
            let sql = format!("SELECT count(*) FROM t WHERE {pred}");
            assert_eq!(
                count_of(&run_sql(&sql, &cm, &reg).await),
                expected,
                "{pred}"
            );
        }
        let batch = run_sql(
            "SELECT CASE WHEN a = 1 THEN a ELSE NULL END, COALESCE(NULL, a) FROM t",
            &cm,
            &reg,
        )
        .await;
        assert_eq!(batch.column(0).null_count(), 2);
        assert_eq!(batch.column(1).null_count(), 1);
    }

    #[tokio::test]
    async fn overflowing_constant_expression_errors_instead_of_panicking() {
        let (cm, reg) = null_cmp_memory_env();
        let pool: Arc<dyn arneb_execution::memory_pool::MemoryPool> =
            Arc::new(arneb_execution::memory_pool::UnboundedMemoryPool::new());
        for sql in [
            "SELECT 9223372036854775807 + 1",
            "SELECT 9223372036854775807 * 2",
            "SELECT 1 / 0",
        ] {
            let err = execute_query(sql, &cm, &reg, None, &pool)
                .await
                .err()
                .unwrap_or_else(|| panic!("expected an error: {sql}"));
            let msg = err.to_string();
            assert!(
                msg.contains("overflow") || msg.contains("Divide by zero"),
                "{sql}: {msg}"
            );
        }
    }

    // -- End-to-end: aggregate result types (Trino semantics) -------------

    /// Memory table `t(i BIGINT, d DECIMAL(15,2), f DOUBLE, dt DATE,
    /// ts TIMESTAMP)` with three rows over two `dt` / `ts` / `d` groups.
    fn agg_types_env() -> (CatalogManager, ConnectorRegistry) {
        use arneb_common::types::TimeUnit;
        use arneb_connectors::memory::{
            MemoryCatalog, MemoryConnectorFactory, MemorySchema, MemoryTable,
        };
        use arrow::array::{Date32Array, Decimal128Array, Float64Array, TimestampMicrosecondArray};
        let cols = vec![
            col("i", DataType::Int64),
            col(
                "d",
                DataType::Decimal128 {
                    precision: 15,
                    scale: 2,
                },
            ),
            col("f", DataType::Float64),
            col("dt", DataType::Date32),
            col(
                "ts",
                DataType::Timestamp {
                    unit: TimeUnit::Microsecond,
                    timezone: None,
                },
            ),
        ];
        let arrow_schema = Arc::new(Schema::new(
            cols.iter().cloned().map(Field::from).collect::<Vec<_>>(),
        ));
        let batch = arrow::record_batch::RecordBatch::try_new(
            arrow_schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 4])),
                Arc::new(
                    Decimal128Array::from(vec![100, 100, 450])
                        .with_precision_and_scale(15, 2)
                        .unwrap(),
                ),
                Arc::new(Float64Array::from(vec![1.5, 2.5, 4.0])),
                Arc::new(Date32Array::from(vec![10, 10, 20])),
                Arc::new(TimestampMicrosecondArray::from(vec![1_000, 1_000, 2_000])),
            ],
        )
        .unwrap();
        let schema = Arc::new(MemorySchema::new());
        schema.register_table("t", Arc::new(MemoryTable::new(cols, vec![batch])));
        let catalog = Arc::new(MemoryCatalog::new());
        catalog.register_schema("default", schema);
        let factory = MemoryConnectorFactory::new(catalog.clone(), "default");
        let cm = CatalogManager::new("memory", "default");
        cm.register_catalog("memory", catalog);
        let mut reg = ConnectorRegistry::new();
        reg.register("memory", Arc::new(factory));
        (cm, reg)
    }

    fn decimal_cell(batch: &arrow::record_batch::RecordBatch, i: usize) -> (i128, u8, i8) {
        let a = batch
            .column(i)
            .as_any()
            .downcast_ref::<arrow::array::Decimal128Array>()
            .unwrap_or_else(|| panic!("column {i} is {:?}", batch.column(i).data_type()));
        (a.value(0), a.precision(), a.scale())
    }

    fn f64_cell(batch: &arrow::record_batch::RecordBatch, i: usize) -> f64 {
        batch
            .column(i)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap_or_else(|| panic!("column {i} is {:?}", batch.column(i).data_type()))
            .value(0)
    }

    /// AVG(integer) / AVG(double) are DOUBLE; AVG(DECIMAL(p,s)) is an exact
    /// DECIMAL(p,s). Used to fail: planner said Int64 / executor gave Float64.
    #[tokio::test]
    async fn avg_result_types_match_trino() {
        let (cm, reg) = agg_types_env();
        let b = run_sql("SELECT avg(i), avg(d), avg(f) FROM t", &cm, &reg).await;
        assert!((f64_cell(&b, 0) - 7.0 / 3.0).abs() < 1e-12);
        // (1.00 + 1.00 + 4.50) / 3 = 2.1666.. -> 2.17
        assert_eq!(decimal_cell(&b, 1), (217, 15, 2));
        assert!((f64_cell(&b, 2) - 8.0 / 3.0).abs() < 1e-12);
    }

    /// SUM(DECIMAL(p,s)) is DECIMAL(38,s) on both the planner and executor.
    #[tokio::test]
    async fn sum_decimal_is_decimal_38() {
        let (cm, reg) = agg_types_env();
        let b = run_sql(
            "SELECT sum(i), sum(d), sum(CAST(i AS DECIMAL(15,2))), sum(f) FROM t",
            &cm,
            &reg,
        )
        .await;
        let i = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(i.value(0), 7);
        assert_eq!(decimal_cell(&b, 1), (650, 38, 2));
        assert_eq!(decimal_cell(&b, 2), (700, 38, 2));
        assert_eq!(f64_cell(&b, 3), 8.0);
    }

    /// DISTINCT aggregates grouped by DECIMAL and TIMESTAMP keys.
    #[tokio::test]
    async fn distinct_aggregate_groups_by_decimal_and_timestamp() {
        let (cm, reg) = agg_types_env();
        let b = run_sql(
            "SELECT d, ts, count(DISTINCT i) FROM t GROUP BY d, ts ORDER BY d",
            &cm,
            &reg,
        )
        .await;
        assert_eq!(b.num_rows(), 2);
        let counts = b.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(counts.values(), &[2, 1]);
    }

    // -- End-to-end: decimal arithmetic result types (Trino semantics) ----

    /// Memory table `t(p, disc, tax, qty DECIMAL(15,2), i BIGINT)`, the
    /// TPC-H lineitem types, with rows (1000.00, 0.05, 0.08, 17.00, 1) and
    /// (2500.50, 0.10, 0.02, 36.00, 2).
    fn decimal_arith_env() -> (CatalogManager, ConnectorRegistry) {
        use arneb_connectors::memory::{
            MemoryCatalog, MemoryConnectorFactory, MemorySchema, MemoryTable,
        };
        use arrow::array::Decimal128Array;
        let dec = DataType::Decimal128 {
            precision: 15,
            scale: 2,
        };
        let cols = vec![
            col("p", dec.clone()),
            col("disc", dec.clone()),
            col("tax", dec.clone()),
            col("qty", dec),
            col("i", DataType::Int64),
        ];
        let arrow_schema = Arc::new(Schema::new(
            cols.iter().cloned().map(Field::from).collect::<Vec<_>>(),
        ));
        let d = |v: Vec<i128>| -> arrow::array::ArrayRef {
            Arc::new(
                Decimal128Array::from(v)
                    .with_precision_and_scale(15, 2)
                    .unwrap(),
            )
        };
        let batch = arrow::record_batch::RecordBatch::try_new(
            arrow_schema,
            vec![
                d(vec![100_000, 250_050]),
                d(vec![5, 10]),
                d(vec![8, 2]),
                d(vec![1_700, 3_600]),
                Arc::new(Int64Array::from(vec![1, 2])),
            ],
        )
        .unwrap();
        let schema = Arc::new(MemorySchema::new());
        schema.register_table("t", Arc::new(MemoryTable::new(cols, vec![batch])));
        // `u` is read only from subqueries in the scalar-subquery tests.
        let u_cols = vec![col("k", DataType::Int64)];
        let u_batch = arrow::record_batch::RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::from(u_cols[0].clone())])),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4]))],
        )
        .unwrap();
        schema.register_table("u", Arc::new(MemoryTable::new(u_cols, vec![u_batch])));
        let catalog = Arc::new(MemoryCatalog::new());
        catalog.register_schema("default", schema);
        let factory = MemoryConnectorFactory::new(catalog.clone(), "default");
        let cm = CatalogManager::new("memory", "default");
        cm.register_catalog("memory", catalog);
        let mut reg = ConnectorRegistry::new();
        reg.register("memory", Arc::new(factory));
        (cm, reg)
    }

    /// TPC-H Q1 expressions on DECIMAL(15,2). Used to fail with
    /// "arithmetic_op received mismatched types Decimal128(21, 2) vs
    /// Decimal128(22, 2)".
    #[tokio::test]
    async fn decimal_q1_expressions_have_trino_types() {
        let (cm, reg) = decimal_arith_env();
        let b = run_sql(
            "SELECT p * (1 - disc), p * (1 - disc) * (1 + tax) FROM t WHERE i = 1",
            &cm,
            &reg,
        )
        .await;
        assert_eq!(decimal_cell(&b, 0), (9_500_000, 31, 4)); // 950.0000
        assert_eq!(decimal_cell(&b, 1), (1_026_000_000, 38, 6)); // 1026.000000

        let b = run_sql(
            "SELECT sum(p * (1 - disc)), avg(p * (1 - disc)), \
             sum(p * (1 - disc) * (1 + tax)) FROM t",
            &cm,
            &reg,
        )
        .await;
        assert_eq!(decimal_cell(&b, 0), (32_004_500, 38, 4)); // 950 + 2250.45
        assert_eq!(decimal_cell(&b, 1), (16_002_250, 31, 4));
        assert_eq!(decimal_cell(&b, 2), (3_321_459_000, 38, 6)); // 1026 + 2295.459
    }

    #[tokio::test]
    async fn decimal_with_integer_and_division() {
        let (cm, reg) = decimal_arith_env();
        let b = run_sql(
            "SELECT qty + i, qty + 1, qty * 2, p / qty, disc / 3, p % 7 FROM t WHERE i = 1",
            &cm,
            &reg,
        )
        .await;
        assert_eq!(decimal_cell(&b, 0), (1_800, 22, 2)); // BIGINT -> DECIMAL(19,0)
        assert_eq!(decimal_cell(&b, 1), (1_800, 16, 2)); // literal -> DECIMAL(10,0)
        assert_eq!(decimal_cell(&b, 2), (3_400, 25, 2));
        // 58.823529411764705882|35 (Trino: DECIMAL(33,18))
        assert_eq!(decimal_cell(&b, 3), (58_823_529_411_764_705_882, 33, 18));
        // 0.0166666666666|66 rounds half up (Trino: DECIMAL(26,13))
        assert_eq!(decimal_cell(&b, 4), (166_666_666_667, 26, 13));
        assert_eq!(decimal_cell(&b, 5), (600, 12, 2)); // 1000.00 % 7
    }

    /// TPC-H Q22 shape: a DECIMAL scalar subquery used to come back as a
    /// Utf8 literal ("compare_op received mismatched types Decimal128 vs Utf8").
    #[tokio::test]
    async fn decimal_scalar_subquery_stays_decimal() {
        let (cm, reg) = decimal_arith_env();
        let b = run_sql(
            "SELECT count(*) FROM t WHERE p > (SELECT avg(p) FROM t)",
            &cm,
            &reg,
        )
        .await;
        let n = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(n.value(0), 1);
    }

    /// Standalone (no workers): a scalar subquery over a table the outer
    /// query does not scan still gets that table's data source.
    #[tokio::test]
    async fn scalar_subquery_over_a_table_only_it_reads() {
        let (cm, reg) = decimal_arith_env();
        let b = run_sql(
            "SELECT count(*) FROM t WHERE i < (SELECT count(*) FROM u) - 2",
            &cm,
            &reg,
        )
        .await;
        let n = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(n.value(0), 1);
    }

    /// TPC-H Q11 shape: a scalar subquery nested inside another one's
    /// expression, resolved without workers.
    #[tokio::test]
    async fn nested_scalar_subquery_is_resolved_locally() {
        let (cm, reg) = decimal_arith_env();
        let b = run_sql(
            "SELECT count(*) FROM t \
             WHERE i >= (SELECT max(k) / (SELECT count(*) FROM u) FROM u) + 1",
            &cm,
            &reg,
        )
        .await;
        let n = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(n.value(0), 1);
    }

    /// The full TPC-H Q11 shape: the nested subquery sits under a HAVING
    /// threshold built from an aggregate.
    #[tokio::test]
    async fn q11_shaped_having_with_nested_scalar_subquery() {
        let (cm, reg) = decimal_arith_env();
        let b = run_sql(
            "SELECT i, sum(p * qty) AS v FROM t GROUP BY i \
             HAVING sum(p * qty) > (SELECT sum(p * qty) / (SELECT count(*) FROM u) FROM t) \
             ORDER BY v DESC",
            &cm,
            &reg,
        )
        .await;
        let i = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(i.values(), &[2]);
    }

    /// TPC-H Q15 shape: the CTE self-MAX is rewritten to a window MAX, which
    /// used to read every DECIMAL as 0.0 and so kept every row.
    #[tokio::test]
    async fn decimal_cte_max_rewrite_keeps_only_the_max_row() {
        let (cm, reg) = decimal_arith_env();
        let b = run_sql(
            "WITH r AS (SELECT i, p FROM t) \
             SELECT i FROM r WHERE p >= (SELECT MAX(p) - 0.01 FROM r)",
            &cm,
            &reg,
        )
        .await;
        let i = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(i.values(), &[2]);
    }

    // -- End-to-end: decimal literals are DECIMAL(p,s) (Trino) -----------

    #[tokio::test]
    async fn decimal_literal_is_decimal() {
        let (cm, reg) = decimal_arith_env();
        let b = run_sql("SELECT 1.5, 0.05, 1e3, -0.05", &cm, &reg).await;
        assert_eq!(decimal_cell(&b, 0), (15, 2, 1));
        assert_eq!(decimal_cell(&b, 1), (5, 2, 2));
        assert_eq!(f64_cell(&b, 2), 1000.0);
        assert_eq!(decimal_cell(&b, 3), (-5, 2, 2));
    }

    /// TPC-H Q14 (`100.00 * sum(..)`) and Q17 (`sum(..) / 7.0`) shapes are
    /// exact on DECIMAL data, with Trino's result types.
    #[tokio::test]
    async fn decimal_literal_q14_q17_shapes_are_exact() {
        let (cm, reg) = decimal_arith_env();
        let b = run_sql(
            "SELECT 100.00 * sum(p * (1 - disc)), sum(p) / 7.0, \
             100.00 * sum(p * (1 - disc)) / sum(p) FROM t",
            &cm,
            &reg,
        )
        .await;
        // 100.00 * 3200.4500 = 320045.000000
        assert_eq!(decimal_cell(&b, 0), (320_045_000_000, 38, 6));
        // 3500.50 / 7.0 = 500.07142857.. (Trino: DECIMAL(38,6))
        assert_eq!(decimal_cell(&b, 1), (500_071_429, 38, 6));
        // 320045.000000 / 3500.50 = 91.428367376...
        let (v, _, s) = decimal_cell(&b, 2);
        assert_eq!((v, s), (91_428_367, 6));
    }

    /// DECIMAL op DOUBLE is DOUBLE (Trino), so a decimal literal against a
    /// DOUBLE column keeps DOUBLE semantics; comparisons work on both types.
    #[tokio::test]
    async fn decimal_literal_with_double_and_decimal_columns() {
        let (cm, reg) = agg_types_env();
        let b = run_sql("SELECT f * 0.5, f + 0.25 FROM t WHERE i = 1", &cm, &reg).await;
        assert_eq!(f64_cell(&b, 0), 0.75);
        assert_eq!(f64_cell(&b, 1), 1.75);
        for (pred, want) in [
            ("f BETWEEN 1.5 AND 2.5", 2),
            ("f > 2.49", 2),
            ("d BETWEEN 1.00 AND 1.0", 2),
            ("d BETWEEN 0.5 AND 4.495", 2),
            ("d = 4.5", 1),
            ("d > 1", 1),
        ] {
            let sql = format!("SELECT count(*) FROM t WHERE {pred}");
            assert_eq!(count_of(&run_sql(&sql, &cm, &reg).await), want, "{pred}");
        }
        let b = run_sql(
            "SELECT CAST('1.5' AS DECIMAL(3,1)), CAST('1.5' AS DOUBLE)",
            &cm,
            &reg,
        )
        .await;
        assert_eq!(decimal_cell(&b, 0), (15, 3, 1));
        assert_eq!(f64_cell(&b, 1), 1.5);
    }

    #[tokio::test]
    async fn decimal_overflow_is_an_error() {
        let (cm, reg) = decimal_arith_env();
        let pool: Arc<dyn arneb_execution::memory_pool::MemoryPool> =
            Arc::new(arneb_execution::memory_pool::UnboundedMemoryPool::new());
        let sql =
            "SELECT CAST('999999999999999999999999999999999999.99' AS DECIMAL(38,2)) + p FROM t";
        let err = execute_query(sql, &cm, &reg, None, &pool)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("too large"), "{err}");
    }

    // -- End-to-end: SQL window functions (issue #104) --------------------

    /// Memory table `w(id BIGINT, grp VARCHAR NULL, v BIGINT NULL)`:
    /// (1,a,10) (2,a,20) (3,a,20) (4,b,5) (5,b,NULL) (6,NULL,7) — ties on
    /// `v` within `a`, a NULL `v`, and a NULL partition key.
    fn window_env() -> (CatalogManager, ConnectorRegistry) {
        use arneb_connectors::memory::{
            MemoryCatalog, MemoryConnectorFactory, MemorySchema, MemoryTable,
        };
        use arrow::array::StringArray;
        let batch = arrow::record_batch::RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", ArrowDataType::Int64, false),
                Field::new("grp", ArrowDataType::Utf8, true),
                Field::new("v", ArrowDataType::Int64, true),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6])),
                Arc::new(StringArray::from(vec![
                    Some("a"),
                    Some("a"),
                    Some("a"),
                    Some("b"),
                    Some("b"),
                    None,
                ])),
                Arc::new(Int64Array::from(vec![
                    Some(10),
                    Some(20),
                    Some(20),
                    Some(5),
                    None,
                    Some(7),
                ])),
            ],
        )
        .unwrap();
        let table = MemoryTable::new(
            vec![
                col("id", DataType::Int64),
                col("grp", DataType::Utf8),
                col("v", DataType::Int64),
            ],
            vec![batch],
        );
        let schema = Arc::new(MemorySchema::new());
        schema.register_table("w", Arc::new(table));
        let catalog = Arc::new(MemoryCatalog::new());
        catalog.register_schema("default", schema);
        let cm = CatalogManager::new("memory", "default");
        cm.register_catalog("memory", catalog.clone());
        let mut reg = ConnectorRegistry::new();
        reg.register(
            "memory",
            Arc::new(MemoryConnectorFactory::new(catalog, "default")),
        );
        (cm, reg)
    }

    /// Runs `sql` against [`window_env`] and renders every cell as a string
    /// (`NULL` for nulls), checking the declared plan schema agrees with the
    /// produced batch types.
    async fn window_rows(sql: &str) -> Vec<Vec<String>> {
        use arrow::array::Array;
        use arrow::util::display::array_value_to_string;
        let (cm, reg) = window_env();
        let pool: Arc<dyn arneb_execution::memory_pool::MemoryPool> =
            Arc::new(arneb_execution::memory_pool::UnboundedMemoryPool::new());
        let (plan, batches) = execute_query(sql, &cm, &reg, None, &pool)
            .await
            .unwrap_or_else(|e| panic!("query failed: {sql}: {e}"));
        let declared: Vec<ArrowDataType> = plan
            .schema()
            .iter()
            .map(|c| c.data_type.clone().into())
            .collect();
        let mut rows = Vec::new();
        for b in batches.iter().filter(|b| b.num_rows() > 0) {
            let produced: Vec<ArrowDataType> =
                b.columns().iter().map(|c| c.data_type().clone()).collect();
            assert_eq!(declared, produced, "declared vs produced types: {sql}");
            for r in 0..b.num_rows() {
                rows.push(
                    b.columns()
                        .iter()
                        .map(|c| {
                            if c.is_null(r) {
                                "NULL".to_string()
                            } else {
                                array_value_to_string(c, r).unwrap()
                            }
                        })
                        .collect(),
                );
            }
        }
        rows
    }

    fn rows(expected: &[&[&str]]) -> Vec<Vec<String>> {
        expected
            .iter()
            .map(|r| r.iter().map(|c| c.to_string()).collect())
            .collect()
    }

    #[tokio::test]
    async fn window_ranking_functions_without_partition() {
        // v ascending, NULLS LAST: 5(id4) 7(id6) 10(id1) 20(id2) 20(id3) NULL(id5).
        let got = window_rows(
            "SELECT id, row_number() OVER (ORDER BY v, id), rank() OVER (ORDER BY v), \
             dense_rank() OVER (ORDER BY v) FROM w ORDER BY id",
        )
        .await;
        assert_eq!(
            got,
            rows(&[
                &["1", "3", "3", "3"],
                &["2", "4", "4", "4"],
                &["3", "5", "4", "4"],
                &["4", "1", "1", "1"],
                &["5", "6", "6", "5"],
                &["6", "2", "2", "2"],
            ])
        );
    }

    #[tokio::test]
    async fn window_ranking_functions_with_partition() {
        let got = window_rows(
            "SELECT id, row_number() OVER (PARTITION BY grp ORDER BY v, id) AS rn, \
             rank() OVER (PARTITION BY grp ORDER BY v) AS r, \
             dense_rank() OVER (PARTITION BY grp ORDER BY v DESC) AS dr \
             FROM w ORDER BY id",
        )
        .await;
        assert_eq!(
            got,
            rows(&[
                &["1", "1", "1", "2"],
                &["2", "2", "2", "1"],
                &["3", "3", "2", "1"],
                &["4", "1", "1", "1"],
                &["5", "2", "2", "2"],
                &["6", "1", "1", "1"],
            ])
        );
    }

    #[tokio::test]
    async fn window_running_aggregates_include_peers() {
        // Default frame with ORDER BY is RANGE UNBOUNDED PRECEDING .. CURRENT
        // ROW: the tied v=20 rows (and the NULL peer group) share one value.
        // sum(bigint) is BIGINT, as in Trino and the GROUP BY form.
        let got = window_rows(
            "SELECT id, sum(v) OVER (ORDER BY v), count(v) OVER (ORDER BY v), \
             count(*) OVER (ORDER BY v) FROM w ORDER BY id",
        )
        .await;
        assert_eq!(
            got,
            rows(&[
                &["1", "22", "3", "3"],
                &["2", "62", "5", "5"],
                &["3", "62", "5", "5"],
                &["4", "5", "1", "1"],
                &["5", "62", "5", "6"],
                &["6", "12", "2", "2"],
            ])
        );
    }

    #[tokio::test]
    async fn window_aggregates_without_order_cover_whole_partition() {
        let got = window_rows(
            "SELECT id, sum(v) OVER (PARTITION BY grp), max(v) OVER (PARTITION BY grp), \
             count(*) OVER () FROM w ORDER BY id",
        )
        .await;
        assert_eq!(
            got,
            rows(&[
                &["1", "50", "20", "6"],
                &["2", "50", "20", "6"],
                &["3", "50", "20", "6"],
                &["4", "5", "5", "6"],
                &["5", "5", "5", "6"],
                &["6", "7", "7", "6"],
            ])
        );
    }

    #[tokio::test]
    async fn window_two_specs_and_expression_over_window() {
        let got = window_rows(
            "SELECT id, row_number() OVER (ORDER BY id DESC) * 10 AS x, \
             sum(v) OVER (PARTITION BY grp) AS s FROM w ORDER BY id",
        )
        .await;
        assert_eq!(
            got,
            rows(&[
                &["1", "60", "50"],
                &["2", "50", "50"],
                &["3", "40", "50"],
                &["4", "30", "5"],
                &["5", "20", "5"],
                &["6", "10", "7"],
            ])
        );
    }

    #[tokio::test]
    async fn window_with_where_order_by_and_limit() {
        // WHERE runs before the window: ids 1..5, v DESC NULLS LAST =
        // 20(id2) 20(id3) 10(id1) 5(id4) NULL(id5).
        let got = window_rows(
            "SELECT id, rank() OVER (ORDER BY v DESC) AS r FROM w WHERE id <= 5 \
             ORDER BY r, id LIMIT 3",
        )
        .await;
        assert_eq!(got, rows(&[&["2", "1"], &["3", "1"], &["1", "3"]]));

        // ORDER BY the window call itself (unaliased in the select list).
        let got = window_rows(
            "SELECT id, rank() OVER (ORDER BY v DESC) FROM w WHERE id <= 5 \
             ORDER BY rank() OVER (ORDER BY v DESC) DESC, id LIMIT 2",
        )
        .await;
        assert_eq!(got, rows(&[&["5", "5"], &["4", "4"]]));
    }

    #[tokio::test]
    async fn window_over_aggregate_with_group_by() {
        // Group sums: a=50, b=5, NULL=7.
        let got = window_rows(
            "SELECT grp, sum(v) AS s, rank() OVER (ORDER BY sum(v) DESC) AS r \
             FROM w GROUP BY grp ORDER BY grp",
        )
        .await;
        assert_eq!(
            got,
            rows(&[&["a", "50", "1"], &["b", "5", "3"], &["NULL", "7", "2"]])
        );
    }

    #[tokio::test]
    async fn window_filtered_in_outer_query() {
        let got = window_rows(
            "SELECT id FROM (SELECT id, row_number() OVER (PARTITION BY grp ORDER BY id DESC) \
             AS rn FROM w) t WHERE rn = 1 ORDER BY id",
        )
        .await;
        assert_eq!(got, rows(&[&["3"], &["5"], &["6"]]));
    }

    #[tokio::test]
    async fn window_with_wildcard_expands_only_table_columns() {
        let got = window_rows(
            "SELECT *, row_number() OVER (PARTITION BY grp ORDER BY id) AS rn FROM w \
             WHERE id >= 4 ORDER BY id",
        )
        .await;
        assert_eq!(
            got,
            rows(&[
                &["4", "b", "5", "1"],
                &["5", "b", "NULL", "2"],
                &["6", "NULL", "7", "1"],
            ])
        );
    }

    #[tokio::test]
    async fn window_unsupported_shapes_error_clearly() {
        let (cm, reg) = window_env();
        let pool: Arc<dyn arneb_execution::memory_pool::MemoryPool> =
            Arc::new(arneb_execution::memory_pool::UnboundedMemoryPool::new());
        for (sql, needle) in [
            (
                "SELECT sum(v) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM w",
                "window frame",
            ),
            ("SELECT lag(v) OVER (ORDER BY id) FROM w", "not supported"),
            ("SELECT id FROM w WHERE row_number() OVER () > 1", "window"),
        ] {
            let err = execute_query(sql, &cm, &reg, None, &pool)
                .await
                .err()
                .unwrap_or_else(|| panic!("expected an error: {sql}"));
            let msg = err.to_string();
            assert!(msg.contains(needle), "{sql}: {msg}");
        }
    }

    // -- End-to-end: SELECT DISTINCT and ORDER BY / GROUP BY ordinals -----

    /// `nation(n_nationkey, n_name, n_regionkey)`: 10 rows split over two
    /// batches in non-sorted order. Rows per region: 0→2, 1→3, 2→2, 3→2, 4→1.
    fn nation_env() -> (CatalogManager, ConnectorRegistry) {
        use arneb_connectors::memory::{
            MemoryCatalog, MemoryConnectorFactory, MemorySchema, MemoryTable,
        };
        use arrow::array::StringArray;
        let rows: [(i64, &str, i64); 10] = [
            (9, "INDONESIA", 2),
            (3, "CANADA", 1),
            (0, "ALGERIA", 0),
            (6, "FRANCE", 3),
            (1, "ARGENTINA", 1),
            (8, "INDIA", 2),
            (4, "EGYPT", 4),
            (2, "BRAZIL", 1),
            (7, "GERMANY", 3),
            (5, "ETHIOPIA", 0),
        ];
        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("n_nationkey", ArrowDataType::Int64, false),
            Field::new("n_name", ArrowDataType::Utf8, false),
            Field::new("n_regionkey", ArrowDataType::Int64, false),
        ]));
        let batches = rows
            .chunks(5)
            .map(|chunk| {
                arrow::record_batch::RecordBatch::try_new(
                    arrow_schema.clone(),
                    vec![
                        Arc::new(Int64Array::from_iter_values(chunk.iter().map(|r| r.0))),
                        Arc::new(StringArray::from_iter_values(chunk.iter().map(|r| r.1))),
                        Arc::new(Int64Array::from_iter_values(chunk.iter().map(|r| r.2))),
                    ],
                )
                .unwrap()
            })
            .collect();
        let table = Arc::new(MemoryTable::new(
            vec![
                col("n_nationkey", DataType::Int64),
                col("n_name", DataType::Utf8),
                col("n_regionkey", DataType::Int64),
            ],
            batches,
        ));
        let schema = Arc::new(MemorySchema::new());
        schema.register_table("nation", table);
        let catalog = Arc::new(MemoryCatalog::new());
        catalog.register_schema("default", schema);
        let factory = MemoryConnectorFactory::new(catalog.clone(), "default");
        let cm = CatalogManager::new("memory", "default");
        cm.register_catalog("memory", catalog);
        let mut reg = ConnectorRegistry::new();
        reg.register("memory", Arc::new(factory));
        (cm, reg)
    }

    /// Runs `sql` against [`nation_env`]; each row is its cells joined by `|`.
    async fn nation_rows(sql: &str) -> Result<Vec<String>, String> {
        use arrow::util::display::ArrayFormatter;
        let (cm, reg) = nation_env();
        let pool: Arc<dyn arneb_execution::memory_pool::MemoryPool> =
            Arc::new(arneb_execution::memory_pool::UnboundedMemoryPool::new());
        let (_, batches) = execute_query(sql, &cm, &reg, None, &pool)
            .await
            .map_err(|e| e.to_string())?;
        let opts = arrow::util::display::FormatOptions::default().with_null("NULL");
        let mut out = Vec::new();
        for b in &batches {
            let fmts: Vec<_> = b
                .columns()
                .iter()
                .map(|c| ArrayFormatter::try_new(c.as_ref(), &opts).unwrap())
                .collect();
            for r in 0..b.num_rows() {
                let cells: Vec<String> = fmts.iter().map(|f| f.value(r).to_string()).collect();
                out.push(cells.join("|"));
            }
        }
        Ok(out)
    }

    async fn assert_rows(sql: &str, expected: &[&str]) {
        let rows = nation_rows(sql)
            .await
            .unwrap_or_else(|e| panic!("query failed: {sql}: {e}"));
        assert_eq!(rows, expected, "{sql}");
    }

    async fn assert_query_error(sql: &str, needle: &str) {
        let err = nation_rows(sql)
            .await
            .err()
            .unwrap_or_else(|| panic!("expected an error: {sql}"));
        assert!(err.contains(needle), "{sql}: {err}");
    }

    #[tokio::test]
    async fn select_distinct_removes_duplicates() {
        let mut rows = nation_rows("SELECT DISTINCT n_regionkey FROM nation")
            .await
            .unwrap();
        rows.sort();
        assert_eq!(rows, ["0", "1", "2", "3", "4"]);
        assert_rows(
            "SELECT count(*) FROM (SELECT DISTINCT n_regionkey FROM nation) t",
            &["5"],
        )
        .await;
        assert_rows(
            "SELECT DISTINCT n_regionkey, n_regionkey * 10 AS r10 FROM nation ORDER BY r10",
            &["0|0", "1|10", "2|20", "3|30", "4|40"],
        )
        .await;
        assert_rows(
            "SELECT DISTINCT n_regionkey FROM nation ORDER BY n_regionkey DESC LIMIT 2",
            &["4", "3"],
        )
        .await;
        assert_rows(
            "SELECT DISTINCT NULLIF(n_regionkey, 0) AS r FROM nation ORDER BY 1 DESC NULLS FIRST",
            &["NULL", "4", "3", "2", "1"],
        )
        .await;
        // DISTINCT over a GROUP BY result.
        assert_rows(
            "SELECT DISTINCT count(*) FROM nation GROUP BY n_regionkey ORDER BY 1",
            &["1", "2", "3"],
        )
        .await;
    }

    #[tokio::test]
    async fn select_distinct_rejects_order_by_outside_select_list_and_distinct_on() {
        assert_query_error(
            "SELECT DISTINCT n_regionkey FROM nation ORDER BY n_name",
            "ORDER BY expressions must appear in select list",
        )
        .await;
        assert_query_error(
            "SELECT DISTINCT ON (n_regionkey) n_name FROM nation",
            "DISTINCT ON",
        )
        .await;
    }

    #[tokio::test]
    async fn union_distinct_dedups_and_sorts_by_ordinal() {
        assert_rows(
            "SELECT n_regionkey FROM nation UNION SELECT n_regionkey FROM nation ORDER BY 1",
            &["0", "1", "2", "3", "4"],
        )
        .await;
        assert_rows(
            "SELECT n_name FROM nation WHERE n_regionkey = 1 \
             UNION ALL SELECT n_name FROM nation WHERE n_regionkey = 0 ORDER BY 1 DESC",
            &["ETHIOPIA", "CANADA", "BRAZIL", "ARGENTINA", "ALGERIA"],
        )
        .await;
    }

    #[tokio::test]
    async fn order_by_ordinal_sorts_by_select_list_position() {
        assert_rows(
            "SELECT n_name FROM nation ORDER BY 1 LIMIT 3",
            &["ALGERIA", "ARGENTINA", "BRAZIL"],
        )
        .await;
        assert_rows(
            "SELECT n_name, n_regionkey FROM nation ORDER BY 2 DESC, n_name LIMIT 4",
            &["EGYPT|4", "FRANCE|3", "GERMANY|3", "INDIA|2"],
        )
        .await;
        assert_rows(
            "SELECT n_regionkey, count(*) AS c FROM nation GROUP BY n_regionkey \
             ORDER BY 2 DESC, 1 LIMIT 3",
            &["1|3", "0|2", "2|2"],
        )
        .await;
        for sql in [
            "SELECT n_name FROM nation ORDER BY 0",
            "SELECT n_name FROM nation ORDER BY 2",
        ] {
            assert_query_error(sql, "ORDER BY position").await;
        }
    }

    #[tokio::test]
    async fn group_by_ordinal_groups_by_select_list_position() {
        assert_rows(
            "SELECT n_regionkey, count(*) FROM nation GROUP BY 1 ORDER BY 1",
            &["0|2", "1|3", "2|2", "3|2", "4|1"],
        )
        .await;
        assert_rows(
            "SELECT n_regionkey * 10 AS r, count(*) FROM nation GROUP BY 1 ORDER BY r DESC LIMIT 2",
            &["40|1", "30|2"],
        )
        .await;
        assert_query_error(
            "SELECT n_regionkey FROM nation GROUP BY 2",
            "GROUP BY position",
        )
        .await;
    }
}
