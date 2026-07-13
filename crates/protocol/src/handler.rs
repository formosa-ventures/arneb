use std::fmt::Debug;
use std::sync::Arc;

use arneb_catalog::CatalogManager;
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
use pgwire::api::auth::{
    finish_authentication, save_startup_parameters_to_metadata, DefaultServerParameterProvider,
    StartupHandler,
};
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
use pgwire::messages::PgWireFrontendMessage;

use crate::encoding::{column_info_to_field_info, encode_record_batches};

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
}

impl PgWireServerHandlers for HandlerFactory {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        Arc::new(ConnectionHandler {
            distributed_executor: self.distributed_executor.clone(),
            catalog_manager: Arc::clone(&self.catalog_manager),
            connector_registry: Arc::clone(&self.connector_registry),
            memory_pool: Arc::clone(&self.memory_pool),
        })
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        Arc::new(ConnectionHandler {
            distributed_executor: self.distributed_executor.clone(),
            catalog_manager: Arc::clone(&self.catalog_manager),
            connector_registry: Arc::clone(&self.connector_registry),
            memory_pool: Arc::clone(&self.memory_pool),
        })
    }

    fn startup_handler(&self) -> Arc<impl pgwire::api::auth::StartupHandler> {
        Arc::new(ConnectionHandler {
            distributed_executor: self.distributed_executor.clone(),
            catalog_manager: Arc::clone(&self.catalog_manager),
            connector_registry: Arc::clone(&self.connector_registry),
            memory_pool: Arc::clone(&self.memory_pool),
        })
    }

    fn copy_handler(&self) -> Arc<impl pgwire::api::copy::CopyHandler> {
        Arc::new(NoopHandler)
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
}

#[async_trait]
impl StartupHandler for ConnectionHandler {
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        if let PgWireFrontendMessage::Startup(ref startup) = message {
            save_startup_parameters_to_metadata(client, startup);
            finish_authentication(client, &DefaultServerParameterProvider::default()).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl SimpleQueryHandler for ConnectionHandler {
    async fn do_query<C>(&self, _client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
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

        // Intercept metadata queries (pg_catalog, information_schema, version())
        if let Some(meta_result) =
            crate::metadata::try_handle_metadata(trimmed, &self.catalog_manager).await
        {
            return match meta_result {
                Ok(crate::metadata::MetadataResponse::Query(fields, batches)) => {
                    let field_info: Vec<FieldInfo> = fields
                        .iter()
                        .map(|f| {
                            let pg_type = arrow_type_to_pg(f.data_type());
                            FieldInfo::new(f.name().clone(), None, None, pg_type, FieldFormat::Text)
                        })
                        .collect();
                    let schema = Arc::new(field_info);
                    let (rows, _) = encode_record_batches(&schema, &batches)?;
                    let data_row_stream = stream::iter(rows);
                    Ok(vec![Response::Query(QueryResponse::new(
                        schema,
                        data_row_stream,
                    ))])
                }
                Ok(crate::metadata::MetadataResponse::Command(tag)) => {
                    Ok(vec![Response::Execution(pgwire::api::results::Tag::new(
                        &tag,
                    ))])
                }
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

        let result = execute_query(
            trimmed,
            &self.catalog_manager,
            &self.connector_registry,
            self.distributed_executor.as_deref(),
            &self.memory_pool,
        )
        .await;

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

/// Substitute `$1`, `$2`, ... placeholders with parameter values.
fn bind_parameters(sql: &str, params: &[Option<String>]) -> String {
    let mut result = sql.to_string();
    // Replace in reverse order ($10 before $1) to avoid partial matches
    for (i, param) in params.iter().enumerate().rev() {
        let placeholder = format!("${}", i + 1);
        let replacement = match param {
            Some(val) => {
                // Check if it looks like a number
                if val.parse::<f64>().is_ok() {
                    val.clone()
                } else {
                    format!("'{}'", val.replace('\'', "''"))
                }
            }
            None => "NULL".to_string(),
        };
        result = result.replace(&placeholder, &replacement);
    }
    result
}

/// Extract parameter values from a portal as `Vec<Option<String>>`.
fn extract_params(portal: &Portal<String>) -> Vec<Option<String>> {
    let len = portal.parameter_len();
    (0..len)
        .map(|i| portal.parameter::<String>(i, &Type::TEXT).ok().flatten())
        .collect()
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
        _client: &mut C,
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
        let params = extract_params(portal);
        let bound_sql = bind_parameters(sql, &params);
        let trimmed = bound_sql.trim();

        if trimmed.is_empty() {
            return Ok(Response::EmptyQuery);
        }

        tracing::debug!(query = trimmed, "processing extended query");

        // Intercept metadata queries
        if let Some(meta_result) =
            crate::metadata::try_handle_metadata(trimmed, &self.catalog_manager).await
        {
            return match meta_result {
                Ok(crate::metadata::MetadataResponse::Query(fields, batches)) => {
                    let field_info: Vec<FieldInfo> = fields
                        .iter()
                        .map(|f| {
                            let pg_type = arrow_type_to_pg(f.data_type());
                            FieldInfo::new(f.name().clone(), None, None, pg_type, FieldFormat::Text)
                        })
                        .collect();
                    let schema = Arc::new(field_info);
                    let (rows, _) = encode_record_batches(&schema, &batches)?;
                    let data_row_stream = stream::iter(rows);
                    Ok(Response::Query(QueryResponse::new(schema, data_row_stream)))
                }
                Ok(crate::metadata::MetadataResponse::Command(tag)) => {
                    Ok(Response::Execution(pgwire::api::results::Tag::new(&tag)))
                }
                Err(e) => Err(arneb_error_to_pg_error(&ArnebError::Execution(
                    arneb_common::error::ExecutionError::InvalidOperation(e),
                ))),
            };
        }

        let source = SourceFile::new("<query>", trimmed);
        let result = execute_query(
            trimmed,
            &self.catalog_manager,
            &self.connector_registry,
            self.distributed_executor.as_deref(),
            &self.memory_pool,
        )
        .await;

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
        _client: &mut C,
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

        // Intercept metadata queries for Describe too
        if let Some(Ok(crate::metadata::MetadataResponse::Query(fields, _))) =
            crate::metadata::try_handle_metadata(sql.trim(), &self.catalog_manager).await
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
        if let Some(Ok(crate::metadata::MetadataResponse::Command(_))) =
            crate::metadata::try_handle_metadata(sql.trim(), &self.catalog_manager).await
        {
            return Ok(DescribeStatementResponse::no_data());
        }

        // Count parameter placeholders. The analyzer infers types
        // during planning and returns them in `ctx.param_types` —
        // we translate them to PG OIDs here. Any indices the
        // analyzer didn't see (e.g., because planning failed)
        // fall back to `Type::TEXT` to preserve the legacy behavior
        // for clients that don't care about precise types.
        let param_count = count_placeholders(sql);
        let (fields, param_type_map) = plan_for_describe(sql, &self.catalog_manager)
            .await
            .unwrap_or_else(|_| (Vec::new(), std::collections::HashMap::new()));
        let param_types: Vec<Type> = (1..=param_count)
            .map(|i| {
                param_type_map
                    .get(&i)
                    .map(arneb_type_to_pg_param_type)
                    .unwrap_or(Type::TEXT)
            })
            .collect();

        Ok(DescribeStatementResponse::new(param_types, fields))
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        target: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: pgwire::api::store::PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let sql = &target.statement.statement;
        let params = extract_params(target);
        let bound_sql = bind_parameters(sql, &params);
        let trimmed = bound_sql.trim();

        if trimmed.is_empty() {
            return Ok(DescribePortalResponse::no_data());
        }

        // Intercept metadata queries
        if let Some(Ok(crate::metadata::MetadataResponse::Query(fields, _))) =
            crate::metadata::try_handle_metadata(trimmed, &self.catalog_manager).await
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
        if let Some(Ok(crate::metadata::MetadataResponse::Command(_))) =
            crate::metadata::try_handle_metadata(trimmed, &self.catalog_manager).await
        {
            return Ok(DescribePortalResponse::no_data());
        }

        let fields = plan_for_schema(trimmed, &self.catalog_manager)
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

/// Execute the full query pipeline asynchronously.
async fn execute_query(
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
        // (bytes) drives the A/B; default None (OFF) until measured.
        .with_broadcast_max_build_bytes(
            std::env::var("ARNEB_BROADCAST_MAX_BUILD_BYTES")
                .ok()
                .and_then(|s| s.parse::<usize>().ok()),
        );
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
            })
        }
        LogicalPlan::AntiJoin {
            left,
            right,
            left_key,
            right_key,
            residual,
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
    let Some(executor) = distributed_executor else {
        return ctx
            .resolve_scalar_subqueries(&expr)
            .await
            .map_err(ArnebError::Execution);
    };
    if !distribute_scalar_subquery_enabled() || !executor.has_workers() {
        return ctx
            .resolve_scalar_subqueries(&expr)
            .await
            .map_err(ArnebError::Execution);
    }

    let resolved = resolve_expr_subqueries_distributed_inner(ctx, executor, expr).await?;
    ctx.resolve_scalar_subqueries(&resolved)
        .await
        .map_err(ArnebError::Execution)
}

async fn resolve_expr_subqueries_distributed_inner(
    ctx: &ExecutionContext,
    executor: &dyn DistributedExecutor,
    expr: PlanExpr,
) -> Result<PlanExpr, ArnebError> {
    match expr {
        PlanExpr::ScalarSubquery { subplan, span } => {
            let subplan = Box::pin(resolve_plan_subqueries(ctx, Some(executor), *subplan)).await?;
            if !is_uncorrelated(&subplan) {
                return Ok(PlanExpr::ScalarSubquery {
                    subplan: Box::new(subplan),
                    span,
                });
            }
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

/// Walk the logical plan to find all TableScan nodes and register data sources.
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
                if let Ok(ds) = factory.create_data_source(table, schema, properties).await {
                    ctx.register_data_source(key, ds);
                }
            }
        }
        LogicalPlan::Projection { input, .. }
        | LogicalPlan::Filter { input, .. }
        | LogicalPlan::Sort { input, .. }
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
            &executor,
            scalar_subquery(correlated_subplan),
        )
        .await
        .unwrap();

        assert_eq!(executor.calls.load(Ordering::SeqCst), 0);
        assert!(matches!(resolved, PlanExpr::ScalarSubquery { .. }));
    }
}
