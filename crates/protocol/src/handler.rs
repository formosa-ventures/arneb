use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream;
use futures::Sink;
use pgwire::api::auth::{
    finish_authentication, save_startup_parameters_to_metadata, DefaultServerParameterProvider,
    StartupHandler,
};
use pgwire::api::copy::NoopCopyHandler;
use pgwire::api::query::{PlaceholderExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{QueryResponse, Response};
use pgwire::api::{ClientInfo, PgWireHandlerFactory};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;
use pgwire::messages::PgWireFrontendMessage;
use trino_catalog::CatalogManager;
use trino_common::error::TrinoError;
use trino_connectors::ConnectorRegistry;
use trino_execution::{ExecutionContext, ExecutionPlan};
use trino_planner::{LogicalPlan, QueryPlanner};

use crate::encoding::{column_info_to_field_info, encode_record_batches};
use crate::error::trino_error_to_pg_error;

/// Factory that creates per-connection handlers with shared state.
pub struct HandlerFactory {
    pub catalog_manager: Arc<CatalogManager>,
    pub connector_registry: Arc<ConnectorRegistry>,
}

impl PgWireHandlerFactory for HandlerFactory {
    type StartupHandler = ConnectionHandler;
    type SimpleQueryHandler = ConnectionHandler;
    type ExtendedQueryHandler = PlaceholderExtendedQueryHandler;
    type CopyHandler = NoopCopyHandler;

    fn simple_query_handler(&self) -> Arc<Self::SimpleQueryHandler> {
        Arc::new(ConnectionHandler {
            catalog_manager: Arc::clone(&self.catalog_manager),
            connector_registry: Arc::clone(&self.connector_registry),
        })
    }

    fn extended_query_handler(&self) -> Arc<Self::ExtendedQueryHandler> {
        Arc::new(PlaceholderExtendedQueryHandler)
    }

    fn startup_handler(&self) -> Arc<Self::StartupHandler> {
        Arc::new(ConnectionHandler {
            catalog_manager: Arc::clone(&self.catalog_manager),
            connector_registry: Arc::clone(&self.connector_registry),
        })
    }

    fn copy_handler(&self) -> Arc<Self::CopyHandler> {
        Arc::new(NoopCopyHandler)
    }
}

/// Per-connection handler that processes queries through the full pipeline.
pub struct ConnectionHandler {
    pub catalog_manager: Arc<CatalogManager>,
    pub connector_registry: Arc<ConnectorRegistry>,
}

#[async_trait]
impl StartupHandler for ConnectionHandler {
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        if let PgWireFrontendMessage::Startup(ref startup) = message {
            save_startup_parameters_to_metadata(client, startup);
            finish_authentication(client, &DefaultServerParameterProvider::default()).await;
        }
        Ok(())
    }
}

#[async_trait]
impl SimpleQueryHandler for ConnectionHandler {
    async fn do_query<'a, 'b: 'a, C>(
        &'b self,
        _client: &mut C,
        query: &'a str,
    ) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let trimmed = query.trim();
        if trimmed.is_empty() {
            return Ok(vec![Response::EmptyQuery]);
        }

        tracing::debug!(query = trimmed, "processing simple query");

        let catalog_manager = Arc::clone(&self.catalog_manager);
        let connector_registry = Arc::clone(&self.connector_registry);
        let sql = trimmed.to_string();

        let result = tokio::task::spawn_blocking(move || {
            execute_query(&sql, &catalog_manager, &connector_registry)
        })
        .await
        .map_err(|e| {
            PgWireError::ApiError(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("task join error: {e}"),
            )))
        })?;

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
            Err(err) => Err(trino_error_to_pg_error(&err)),
        }
    }
}

/// Execute the full query pipeline synchronously.
fn execute_query(
    sql: &str,
    catalog_manager: &CatalogManager,
    connector_registry: &ConnectorRegistry,
) -> Result<
    (
        Arc<dyn ExecutionPlan>,
        Vec<arrow::record_batch::RecordBatch>,
    ),
    TrinoError,
> {
    // Step 1: Parse SQL
    let statement = trino_sql_parser::parse(sql)?;

    // Step 2: Plan
    let planner = QueryPlanner::new(catalog_manager);
    let logical_plan = planner.plan_statement(&statement)?;

    // Step 3: Create execution context and register data sources
    let mut exec_ctx = ExecutionContext::new();
    register_data_sources(
        &logical_plan,
        catalog_manager,
        connector_registry,
        &mut exec_ctx,
    )?;

    // Step 4: Create physical plan
    let physical_plan = exec_ctx.create_physical_plan(&logical_plan)?;

    // Step 5: Execute
    let batches = physical_plan.execute()?;

    Ok((physical_plan, batches))
}

/// Walk the logical plan to find all TableScan nodes and register data sources.
fn register_data_sources(
    plan: &LogicalPlan,
    catalog_manager: &CatalogManager,
    registry: &ConnectorRegistry,
    ctx: &mut ExecutionContext,
) -> Result<(), TrinoError> {
    match plan {
        LogicalPlan::TableScan { table, schema, .. } => {
            let key = table.to_string();
            // Determine the connector name from the catalog name
            let connector_name = table
                .catalog
                .as_deref()
                .unwrap_or(catalog_manager.default_catalog());

            if let Some(factory) = registry.get(connector_name) {
                if let Ok(ds) = factory.create_data_source(table, schema) {
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
        LogicalPlan::Aggregate { input, .. } => {
            register_data_sources(input, catalog_manager, registry, ctx)?;
        }
        LogicalPlan::Join { left, right, .. } => {
            register_data_sources(left, catalog_manager, registry, ctx)?;
            register_data_sources(right, catalog_manager, registry, ctx)?;
        }
    }
    Ok(())
}
