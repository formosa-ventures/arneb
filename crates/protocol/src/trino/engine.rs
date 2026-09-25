//! Statement execution for the Trino protocol: control statements are
//! answered here, everything else runs through the same
//! parse → plan → optimize → execute pipeline the pgwire handler uses.

use std::sync::Arc;

use arneb_catalog::CatalogManager;
use arneb_common::diagnostic::SourceFile;
use arneb_connectors::ConnectorRegistry;
use arneb_execution::memory_pool::MemoryPool;
use arrow::array::RecordBatch;

use super::error::TrinoError;
use super::metadata::{self, Cell, TableBuilder};
use super::session::{url_encode, ClientSession};
use super::statements::{bind_placeholders, like_match, parse_control, ControlStatement};
use super::types::{trino_type_of, OutputColumn, TrinoType};
use crate::handler::{execute_query, DistributedExecutor};

/// The result of one statement, before paging.
#[derive(Debug, Default)]
pub(crate) struct QueryOutput {
    /// `None` for statements without a result set (`USE`, `SET SESSION`, …).
    pub(crate) columns: Option<Vec<OutputColumn>>,
    pub(crate) batches: Vec<RecordBatch>,
    /// Trino `updateType` (e.g. `SET SESSION`).
    pub(crate) update_type: Option<String>,
    /// Session-mutation response headers as `(suffix, value)`, e.g.
    /// `("Set-Catalog", "hive")` → `X-Trino-Set-Catalog: hive`.
    pub(crate) headers: Vec<(&'static str, String)>,
}

impl QueryOutput {
    fn update(kind: &str) -> Self {
        Self {
            update_type: Some(kind.to_string()),
            ..Self::default()
        }
    }

    fn with_header(mut self, suffix: &'static str, value: String) -> Self {
        self.headers.push((suffix, value));
        self
    }

    fn table(builder: TableBuilder) -> Self {
        let batch = builder.batch();
        let columns = batch
            .schema()
            .fields()
            .iter()
            .map(|f| OutputColumn::from_field(f))
            .collect();
        Self {
            columns: Some(columns),
            batches: vec![batch],
            ..Self::default()
        }
    }
}

/// Shared engine handles (the same ones the pgwire server holds).
pub(crate) struct Engine {
    pub(crate) catalog_manager: Arc<CatalogManager>,
    pub(crate) connector_registry: Arc<ConnectorRegistry>,
    pub(crate) distributed_executor: Option<Arc<dyn DistributedExecutor>>,
    pub(crate) memory_pool: Arc<dyn MemoryPool>,
}

fn filter_like(names: Vec<String>, like: &Option<String>) -> Vec<String> {
    match like {
        Some(p) => names.into_iter().filter(|n| like_match(p, n)).collect(),
        None => names,
    }
}

fn single_column(name: &'static str, values: Vec<String>) -> QueryOutput {
    let mut b = TableBuilder::new(&[name]);
    for v in values {
        b.push(vec![Cell::Str(Some(v))]);
    }
    QueryOutput::table(b)
}

impl Engine {
    /// Session catalog/schema, falling back to the server defaults.
    fn session_defaults(&self, session: &ClientSession) -> (String, String) {
        let root = &self.catalog_manager;
        let catalog = session
            .catalog
            .clone()
            .unwrap_or_else(|| root.default_catalog().to_string());
        let schema = session.schema.clone().unwrap_or_else(|| {
            if catalog == root.default_catalog() {
                root.default_schema().to_string()
            } else {
                "default".to_string()
            }
        });
        (catalog, schema)
    }

    /// Runs one client statement to completion.
    pub(crate) async fn run_statement(
        &self,
        session: &ClientSession,
        sql: &str,
    ) -> Result<QueryOutput, TrinoError> {
        self.run(session, sql, 0).await
    }

    async fn run(
        &self,
        session: &ClientSession,
        sql: &str,
        depth: u8,
    ) -> Result<QueryOutput, TrinoError> {
        let sql = sql.trim().trim_end_matches(';').trim();
        if sql.is_empty() {
            return Err(TrinoError::user(
                1,
                "SYNTAX_ERROR",
                "SQL statement is empty",
            ));
        }
        if let Some(stmt) = parse_control(sql) {
            return Box::pin(self.control(stmt, session, depth)).await;
        }

        let (catalog, schema) = self.session_defaults(session);
        let root = &self.catalog_manager;
        if metadata::references_virtual_metadata(sql) {
            let (cm, registry) = metadata::build_virtual_catalogs(root, &catalog, sql).await;
            return self.run_sql(sql, &cm, &registry, None).await;
        }
        if catalog == root.default_catalog() && schema == root.default_schema() {
            self.run_sql(
                sql,
                root,
                &self.connector_registry,
                self.distributed_executor.as_deref(),
            )
            .await
        } else {
            let view = root.with_session_defaults(catalog, schema);
            self.run_sql(
                sql,
                &view,
                &self.connector_registry,
                self.distributed_executor.as_deref(),
            )
            .await
        }
    }

    async fn run_sql(
        &self,
        sql: &str,
        catalog_manager: &CatalogManager,
        registry: &ConnectorRegistry,
        distributed: Option<&dyn DistributedExecutor>,
    ) -> Result<QueryOutput, TrinoError> {
        let (plan, batches) = execute_query(
            sql,
            catalog_manager,
            registry,
            distributed,
            &self.memory_pool,
        )
        .await
        .map_err(|e| TrinoError::from_arneb(&e, Some(&SourceFile::new("<query>", sql))))?;
        let infos = plan.schema();
        // Prefer the physical Arrow type of the produced data (it is what
        // gets encoded); fall back to the planned type for empty results.
        let physical = batches
            .first()
            .filter(|b| b.num_columns() == infos.len())
            .map(|b| b.schema());
        let columns = infos
            .iter()
            .enumerate()
            .map(|(i, info)| OutputColumn {
                name: info.name.clone(),
                ty: physical
                    .as_ref()
                    .map(|s| TrinoType::from_arrow(s.field(i).data_type()))
                    .unwrap_or_else(|| trino_type_of(&info.data_type)),
            })
            .collect();
        Ok(QueryOutput {
            columns: Some(columns),
            batches,
            ..QueryOutput::default()
        })
    }

    async fn control(
        &self,
        stmt: ControlStatement,
        session: &ClientSession,
        depth: u8,
    ) -> Result<QueryOutput, TrinoError> {
        let root = &self.catalog_manager;
        let (session_catalog, session_schema) = self.session_defaults(session);
        match stmt {
            ControlStatement::ShowCatalogs { like } => Ok(single_column(
                "Catalog",
                filter_like(metadata::catalog_names(root), &like),
            )),
            ControlStatement::ShowSchemas { catalog, like } => {
                let catalog = catalog.unwrap_or(session_catalog);
                let names = metadata::schema_names(root, &catalog)
                    .await
                    .ok_or_else(|| catalog_not_found(&catalog))?;
                Ok(single_column("Schema", filter_like(names, &like)))
            }
            ControlStatement::ShowTables { schema, like } => {
                let (catalog, schema) = match schema.as_deref() {
                    None => (session_catalog, session_schema),
                    Some([s]) => (session_catalog, s.clone()),
                    Some([c, s]) => (c.clone(), s.clone()),
                    Some(_) => {
                        return Err(TrinoError::user(
                            0,
                            "GENERIC_USER_ERROR",
                            "Too many dots in schema name",
                        ))
                    }
                };
                let names = metadata::table_names(root, &catalog, &schema)
                    .await
                    .ok_or_else(|| schema_not_found(&catalog, &schema))?;
                Ok(single_column("Table", filter_like(names, &like)))
            }
            ControlStatement::ShowColumns { table } => {
                let (catalog, schema, table) = match table.as_slice() {
                    [t] => (session_catalog, session_schema, t.clone()),
                    [s, t] => (session_catalog, s.clone(), t.clone()),
                    [c, s, t] => (c.clone(), s.clone(), t.clone()),
                    _ => {
                        return Err(TrinoError::user(
                            0,
                            "GENERIC_USER_ERROR",
                            "Too many dots in table name",
                        ))
                    }
                };
                let columns = if schema == "information_schema"
                    || catalog == metadata::SYSTEM_CATALOG
                {
                    let probe = format!("{catalog}.information_schema.columns system.jdbc");
                    let (cm, _) = metadata::build_virtual_catalogs(root, &catalog, &probe).await;
                    metadata::table_columns(&cm, &catalog, &schema, &table).await
                } else {
                    metadata::table_columns(root, &catalog, &schema, &table).await
                };
                let columns = columns.ok_or_else(|| {
                    TrinoError::user(
                        46,
                        "TABLE_NOT_FOUND",
                        format!("Table '{catalog}.{schema}.{table}' does not exist"),
                    )
                })?;
                let mut b = TableBuilder::new(&["Column", "Type", "Extra", "Comment"]);
                for col in columns {
                    b.push(vec![
                        Cell::Str(Some(col.name)),
                        Cell::Str(Some(trino_type_of(&col.data_type).display())),
                        Cell::Str(Some(String::new())),
                        Cell::Str(Some(String::new())),
                    ]);
                }
                Ok(QueryOutput::table(b))
            }
            ControlStatement::ShowSession => {
                let mut b = TableBuilder::new(&["Name", "Value", "Default", "Type", "Description"]);
                for (name, value) in &session.properties {
                    b.push(vec![
                        Cell::Str(Some(name.clone())),
                        Cell::Str(Some(value.clone())),
                        Cell::Str(Some(String::new())),
                        Cell::Str(Some("varchar".to_string())),
                        Cell::Str(Some(
                            "Accepted by Arneb for client compatibility; not interpreted"
                                .to_string(),
                        )),
                    ]);
                }
                Ok(QueryOutput::table(b))
            }
            ControlStatement::Use { catalog, schema } => {
                let target_catalog = catalog.clone().unwrap_or(session_catalog);
                let schemas = metadata::schema_names(root, &target_catalog)
                    .await
                    .ok_or_else(|| catalog_not_found(&target_catalog))?;
                if !schemas.contains(&schema) {
                    return Err(schema_not_found(&target_catalog, &schema));
                }
                let mut out = QueryOutput::update("USE");
                if let Some(c) = catalog {
                    out = out.with_header("Set-Catalog", c);
                }
                Ok(out.with_header("Set-Schema", schema))
            }
            ControlStatement::SetSession { name, value } => Ok(QueryOutput::update("SET SESSION")
                .with_header("Set-Session", format!("{name}={}", url_encode(&value)))),
            ControlStatement::ResetSession { name } => {
                Ok(QueryOutput::update("RESET SESSION").with_header("Clear-Session", name))
            }
            ControlStatement::NoOp(kind) => Ok(QueryOutput::update(kind)),
            ControlStatement::Prepare { name, sql } => Ok(QueryOutput::update("PREPARE")
                .with_header("Added-Prepare", format!("{name}={}", url_encode(&sql)))),
            ControlStatement::Deallocate { name } => {
                Ok(QueryOutput::update("DEALLOCATE").with_header("Deallocated-Prepare", name))
            }
            ControlStatement::Execute { name, params } => {
                let sql = session.prepared_statement(&name).ok_or_else(|| {
                    TrinoError::user(
                        5,
                        "NOT_FOUND",
                        format!("Prepared statement not found: {name}"),
                    )
                })?;
                self.run_bound(session, sql, &params, depth).await
            }
            ControlStatement::ExecuteImmediate { sql, params } => {
                self.run_bound(session, &sql, &params, depth).await
            }
        }
    }

    async fn run_bound(
        &self,
        session: &ClientSession,
        sql: &str,
        params: &[String],
        depth: u8,
    ) -> Result<QueryOutput, TrinoError> {
        if depth > 0 {
            return Err(TrinoError::not_supported(
                "Nested EXECUTE of a prepared statement is not supported",
            ));
        }
        let bound = bind_placeholders(sql, params)
            .map_err(|m| TrinoError::user(0, "GENERIC_USER_ERROR", m))?;
        Box::pin(self.run(session, &bound, depth + 1)).await
    }
}

fn catalog_not_found(catalog: &str) -> TrinoError {
    TrinoError::user(
        44,
        "CATALOG_NOT_FOUND",
        format!("Catalog '{catalog}' does not exist"),
    )
}

fn schema_not_found(catalog: &str, schema: &str) -> TrinoError {
    TrinoError::user(
        45,
        "SCHEMA_NOT_FOUND",
        format!("Schema '{catalog}.{schema}' does not exist"),
    )
}
