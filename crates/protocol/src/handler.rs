use std::fmt::Debug;
use std::sync::Arc;

use arneb_catalog::CatalogManager;
use arneb_common::error::ArnebError;
use arneb_common::stream::collect_stream;
use arneb_connectors::ConnectorRegistry;
use arneb_execution::{ExecutionContext, ExecutionPlan};
use arneb_planner::{LogicalOptimizer, LogicalPlan, QueryPlanner};
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
use crate::error::arneb_error_to_pg_error;

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
}

impl PgWireServerHandlers for HandlerFactory {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        Arc::new(ConnectionHandler {
            distributed_executor: self.distributed_executor.clone(),
            catalog_manager: Arc::clone(&self.catalog_manager),
            connector_registry: Arc::clone(&self.connector_registry),
        })
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        Arc::new(ConnectionHandler {
            distributed_executor: self.distributed_executor.clone(),
            catalog_manager: Arc::clone(&self.catalog_manager),
            connector_registry: Arc::clone(&self.connector_registry),
        })
    }

    fn startup_handler(&self) -> Arc<impl pgwire::api::auth::StartupHandler> {
        Arc::new(ConnectionHandler {
            distributed_executor: self.distributed_executor.clone(),
            catalog_manager: Arc::clone(&self.catalog_manager),
            connector_registry: Arc::clone(&self.connector_registry),
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

        let result = execute_query(
            trimmed,
            &self.catalog_manager,
            &self.connector_registry,
            self.distributed_executor.as_deref(),
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
            Err(err) => Err(arneb_error_to_pg_error(&err)),
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

        let result = execute_query(
            trimmed,
            &self.catalog_manager,
            &self.connector_registry,
            self.distributed_executor.as_deref(),
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
            Err(err) => Err(arneb_error_to_pg_error(&err)),
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

        // Count parameter placeholders to report parameter types
        let param_count = count_placeholders(sql);
        let param_types = vec![Type::TEXT; param_count];

        // Try to plan for schema (may fail if params are needed for planning)
        let fields = plan_for_schema(sql, &self.catalog_manager)
            .await
            .unwrap_or_default();

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
    let planner = QueryPlanner::new(catalog_manager);
    let logical_plan = planner.plan_statement(&statement).await?;

    // Step 2.5: Optimize logical plan
    let optimizer = LogicalOptimizer::default_rules();
    let logical_plan = optimizer.optimize(logical_plan)?;

    // Step 3: Create execution context and register data sources
    let mut exec_ctx = ExecutionContext::new();
    register_data_sources(
        &logical_plan,
        catalog_manager,
        connector_registry,
        &mut exec_ctx,
    )?;

    // Step 3.5: Resolve scalar subqueries in expressions (pre-evaluate them)
    let logical_plan = resolve_plan_subqueries(&exec_ctx, logical_plan).await?;

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

    // Step 5: Execute (async)
    let stream = physical_plan.execute().await?;
    let batches = collect_stream(stream).await?;

    Ok((physical_plan, batches))
}

/// Walk a logical plan and resolve any scalar subqueries in expressions.
async fn resolve_plan_subqueries(
    ctx: &ExecutionContext,
    plan: LogicalPlan,
) -> Result<LogicalPlan, ArnebError> {
    match plan {
        LogicalPlan::Filter { input, predicate } => {
            let input = Box::pin(resolve_plan_subqueries(ctx, *input)).await?;
            let predicate = ctx
                .resolve_scalar_subqueries(&predicate)
                .await
                .map_err(ArnebError::Execution)?;
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
            let input = Box::pin(resolve_plan_subqueries(ctx, *input)).await?;
            let mut resolved = Vec::with_capacity(exprs.len());
            for expr in &exprs {
                let r = ctx
                    .resolve_scalar_subqueries(expr)
                    .await
                    .map_err(ArnebError::Execution)?;
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
            let input = Box::pin(resolve_plan_subqueries(ctx, *input)).await?;
            Ok(LogicalPlan::Aggregate {
                input: Box::new(input),
                group_by,
                aggr_exprs,
                schema,
            })
        }
        LogicalPlan::Sort { input, order_by } => {
            let input = Box::pin(resolve_plan_subqueries(ctx, *input)).await?;
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
            let input = Box::pin(resolve_plan_subqueries(ctx, *input)).await?;
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
        } => {
            let left = Box::pin(resolve_plan_subqueries(ctx, *left)).await?;
            let right = Box::pin(resolve_plan_subqueries(ctx, *right)).await?;
            Ok(LogicalPlan::Join {
                left: Box::new(left),
                right: Box::new(right),
                join_type,
                condition,
            })
        }
        // For other plan types, return as-is
        other => Ok(other),
    }
}

/// Walk the logical plan to find all TableScan nodes and register data sources.
fn register_data_sources(
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
                if let Ok(ds) = factory.create_data_source(table, schema, properties) {
                    ctx.register_data_source(key, ds);
                }
            }
        }
        LogicalPlan::Projection { input, .. }
        | LogicalPlan::Filter { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Explain { input } => {
            register_data_sources(input, catalog_manager, registry, ctx)?;
        }
        LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::PartialAggregate { input, .. }
        | LogicalPlan::FinalAggregate { input, .. } => {
            register_data_sources(input, catalog_manager, registry, ctx)?;
        }
        LogicalPlan::Join { left, right, .. } => {
            register_data_sources(left, catalog_manager, registry, ctx)?;
            register_data_sources(right, catalog_manager, registry, ctx)?;
        }
        LogicalPlan::ExchangeNode { .. } => {
            // Exchange nodes don't have table scans — they read from other stages.
        }
        LogicalPlan::SemiJoin { left, right, .. } | LogicalPlan::AntiJoin { left, right, .. } => {
            register_data_sources(left, catalog_manager, registry, ctx)?;
            register_data_sources(right, catalog_manager, registry, ctx)?;
        }
        LogicalPlan::ScalarSubquery { subplan } => {
            register_data_sources(subplan, catalog_manager, registry, ctx)?;
        }
        LogicalPlan::UnionAll { inputs } => {
            for input in inputs {
                register_data_sources(input, catalog_manager, registry, ctx)?;
            }
        }
        LogicalPlan::Distinct { input } | LogicalPlan::Window { input, .. } => {
            register_data_sources(input, catalog_manager, registry, ctx)?;
        }
        LogicalPlan::Intersect { left, right } | LogicalPlan::Except { left, right } => {
            register_data_sources(left, catalog_manager, registry, ctx)?;
            register_data_sources(right, catalog_manager, registry, ctx)?;
        }
        LogicalPlan::CreateTableAsSelect { source, .. }
        | LogicalPlan::InsertInto { source, .. } => {
            register_data_sources(source, catalog_manager, registry, ctx)?;
        }
        LogicalPlan::CreateView { plan, .. } => {
            register_data_sources(plan, catalog_manager, registry, ctx)?;
        }
        LogicalPlan::CreateTable { .. }
        | LogicalPlan::DropTable { .. }
        | LogicalPlan::DeleteFrom { .. }
        | LogicalPlan::DropView { .. } => {}
    }
    Ok(())
}
