//! Catalog side: schema/table discovery and coordinator-side split planning.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arneb_catalog::{CatalogProvider, SchemaProvider, TableProvider, TableStatistics};
use arneb_common::error::ConnectorError;
use arneb_common::types::ColumnInfo;
use arneb_rdbms::{plan_splits, RemoteColumn, RemoteTable, SplitPlan};
use async_trait::async_trait;
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod, Runtime};
use tokio_postgres::NoTls;

use crate::config::PostgresConfig;
use crate::source::PostgresConnectorFactory;
use crate::types::map_type;
use crate::{pg_error, PostgresDialect};

/// Shared state of one mounted PostgreSQL catalog.
pub(crate) struct CatalogState {
    pub(crate) name: String,
    pub(crate) pool: Pool,
    pub(crate) config: PostgresConfig,
}

impl std::fmt::Debug for CatalogState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CatalogState")
            .field("name", &self.name)
            .field("config", &self.config)
            .finish()
    }
}

/// A PostgreSQL database mounted as an Arneb catalog.
#[derive(Debug, Clone)]
pub struct PostgresCatalog {
    state: Arc<CatalogState>,
}

impl PostgresCatalog {
    /// Build the connection pool and check that the database answers.
    pub async fn connect(name: &str, config: PostgresConfig) -> Result<Self, ConnectorError> {
        let driver = config.driver_config().map_err(|e| {
            ConnectorError::ConnectionFailed(format!("postgresql catalog '{name}': {e}"))
        })?;
        let manager = Manager::from_config(
            driver,
            NoTls,
            ManagerConfig {
                recycling_method: RecyclingMethod::Fast,
            },
        );
        // Without these a full pool or an unresponsive host makes a query
        // (or planning, which needs a connection for metadata) wait forever.
        let pool = Pool::builder(manager)
            .max_size(config.max_connections.max(1))
            .runtime(Runtime::Tokio1)
            .wait_timeout(Some(Duration::from_secs(config.pool_wait_timeout_secs)))
            .create_timeout(Some(Duration::from_secs(config.connect_timeout_secs)))
            .build()
            .map_err(|e| {
                ConnectorError::ConnectionFailed(format!("postgresql catalog '{name}': {e}"))
            })?;
        let state = Arc::new(CatalogState {
            name: name.to_string(),
            pool,
            config,
        });
        let client = state.pool.get().await.map_err(|e| {
            ConnectorError::ConnectionFailed(format!(
                "postgresql catalog '{name}' ({}): {}",
                state.config.display_target(),
                pool_error(&e)
            ))
        })?;
        client.simple_query("SELECT 1").await.map_err(|e| {
            ConnectorError::ConnectionFailed(format!(
                "postgresql catalog '{name}': {}",
                pg_error(&e)
            ))
        })?;
        Ok(Self { state })
    }

    /// The connector factory reading this catalog's tables.
    pub fn connector_factory(&self) -> PostgresConnectorFactory {
        PostgresConnectorFactory::new(self.state.clone())
    }

    /// The configuration the catalog runs with.
    pub fn config(&self) -> &PostgresConfig {
        &self.state.config
    }
}

/// SQL for a column's type name as `map_type` reads it: the bare `typname`
/// for built-in types, schema-qualified otherwise, so a user type that
/// shares a built-in's name (`public.date`) is unsupported, not misread.
const TYPE_NAME: &str = "CASE WHEN t.typnamespace = 'pg_catalog'::regnamespace \
     THEN t.typname::text ELSE t.typnamespace::regnamespace::text || '.' || t.typname END";

pub(crate) fn pool_error(e: &deadpool_postgres::PoolError) -> String {
    match e {
        deadpool_postgres::PoolError::Backend(e) => pg_error(e),
        deadpool_postgres::PoolError::Timeout(deadpool_postgres::TimeoutType::Wait) => {
            "no free connection within pool_wait_timeout_secs (all max_connections busy)"
                .to_string()
        }
        deadpool_postgres::PoolError::Timeout(deadpool_postgres::TimeoutType::Create) => {
            "connecting took longer than connect_timeout_secs".to_string()
        }
        other => other.to_string(),
    }
}

#[async_trait]
impl CatalogProvider for PostgresCatalog {
    async fn schema_names(&self) -> Vec<String> {
        let sql = "SELECT nspname FROM pg_namespace \
                   WHERE nspname NOT IN ('pg_catalog', 'information_schema') \
                   AND nspname NOT LIKE 'pg\\_toast%' AND nspname NOT LIKE 'pg\\_temp\\_%' \
                   ORDER BY 1";
        match query_strings(&self.state, sql, &[]).await {
            Ok(names) => names,
            Err(e) => {
                tracing::warn!(catalog = %self.state.name, error = %e, "failed to list schemas");
                Vec::new()
            }
        }
    }

    async fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        let names = self.schema_names().await;
        let resolved = resolve_name(&names, name)?;
        Some(Arc::new(PostgresSchema {
            state: self.state.clone(),
            name: resolved,
        }))
    }
}

/// One remote schema.
#[derive(Debug)]
pub struct PostgresSchema {
    state: Arc<CatalogState>,
    name: String,
}

/// Relation kinds read as tables: ordinary, view, materialized view,
/// foreign, partitioned.
const RELKINDS: &str = "('r', 'v', 'm', 'f', 'p')";

#[async_trait]
impl SchemaProvider for PostgresSchema {
    async fn table_names(&self) -> Vec<String> {
        let sql = format!(
            "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = $1 AND c.relkind IN {RELKINDS} ORDER BY 1"
        );
        match query_strings(&self.state, &sql, &[&self.name]).await {
            Ok(names) => names,
            Err(e) => {
                tracing::warn!(catalog = %self.state.name, schema = %self.name, error = %e, "failed to list tables");
                Vec::new()
            }
        }
    }

    async fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        match load_table(&self.state, &self.name, name).await {
            Ok(Some(t)) => Some(Arc::new(t)),
            Ok(None) => None,
            Err(e) => {
                tracing::warn!(catalog = %self.state.name, schema = %self.name, table = name, error = %e, "failed to load table");
                None
            }
        }
    }
}

/// A remote table, resolved at planning time on the coordinator.
#[derive(Debug)]
pub struct PostgresTable {
    columns: Vec<ColumnInfo>,
    remote: RemoteTable,
    split: SplitPlan,
    row_count: Option<u64>,
}

impl TableProvider for PostgresTable {
    fn schema(&self) -> Vec<ColumnInfo> {
        self.columns.clone()
    }

    fn properties(&self) -> HashMap<String, String> {
        let mut props = HashMap::new();
        self.remote.write_properties(&mut props);
        self.split.write_properties(&mut props);
        props
    }

    fn statistics(&self) -> Option<TableStatistics> {
        self.row_count.map(|rows| TableStatistics {
            row_count: Some(rows),
            ..Default::default()
        })
    }
}

/// Exact match first, then a unique case-insensitive one (unquoted SQL
/// identifiers may arrive folded).
fn resolve_name(names: &[String], wanted: &str) -> Option<String> {
    if names.iter().any(|n| n == wanted) {
        return Some(wanted.to_string());
    }
    let mut matches = names.iter().filter(|n| n.eq_ignore_ascii_case(wanted));
    let first = matches.next()?;
    matches.next().is_none().then(|| first.clone())
}

async fn query_strings(
    state: &CatalogState,
    sql: &str,
    params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
) -> Result<Vec<String>, String> {
    let client = state.pool.get().await.map_err(|e| pool_error(&e))?;
    let rows = client.query(sql, params).await.map_err(|e| pg_error(&e))?;
    Ok(rows.iter().map(|r| r.get::<_, String>(0)).collect())
}

async fn load_table(
    state: &CatalogState,
    schema: &str,
    name: &str,
) -> Result<Option<PostgresTable>, String> {
    let client = state.pool.get().await.map_err(|e| pool_error(&e))?;
    let rel_sql = format!(
        "SELECT c.oid, c.relname, c.reltuples::float8 FROM pg_class c \
         JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = $1 AND c.relkind IN {RELKINDS} AND lower(c.relname) = lower($2)"
    );
    let rels = client
        .query(&rel_sql, &[&schema, &name])
        .await
        .map_err(|e| pg_error(&e))?;
    let names: Vec<String> = rels.iter().map(|r| r.get(1)).collect();
    let Some(resolved) = resolve_name(&names, name) else {
        return Ok(None);
    };
    let rel = rels
        .iter()
        .find(|r| r.get::<_, String>(1) == resolved)
        .expect("resolved name comes from rels");
    let oid: u32 = rel.get(0);
    let reltuples: f64 = rel.get(2);

    let cols = client
        .query(
            &format!(
                "SELECT a.attname, {TYPE_NAME}, a.atttypmod, a.attnotnull FROM pg_attribute a \
             JOIN pg_type t ON t.oid = a.atttypid \
             WHERE a.attrelid = $1 AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum"
            ),
            &[&oid],
        )
        .await
        .map_err(|e| pg_error(&e))?;
    let mut columns = Vec::with_capacity(cols.len());
    let mut remote_columns = Vec::with_capacity(cols.len());
    for row in &cols {
        let col_name: String = row.get(0);
        let typname: String = row.get(1);
        let typmod: i32 = row.get(2);
        let not_null: bool = row.get(3);
        let (data_type, remote_type) =
            map_type(&typname, typmod, state.config.unconstrained_numeric_scale);
        columns.push(ColumnInfo {
            name: col_name.clone(),
            data_type,
            nullable: !not_null,
        });
        remote_columns.push(RemoteColumn {
            name: col_name,
            remote_type,
        });
    }
    let remote = RemoteTable {
        schema: schema.to_string(),
        name: resolved,
        columns: remote_columns,
    };

    // reltuples is -1 for a never-analyzed table (PostgreSQL 14+).
    let row_count = (reltuples >= 0.0).then_some(reltuples as u64);
    // Splitting is an optimisation: if its key or min/max lookup fails (a
    // permission on the key, a transient error), read the table as one split
    // rather than report it missing.
    let split = match plan_table_splits(state, &client, oid, &remote, row_count.unwrap_or(0)).await
    {
        Ok(split) => split,
        Err(e) => {
            tracing::warn!(
                catalog = %state.name,
                schema = %schema,
                table = %name,
                error = %e,
                "split planning failed; reading the table as one split"
            );
            SplitPlan::single()
        }
    };
    tracing::debug!(
        target: "arneb::rdbms",
        catalog = %state.name,
        table = %format!("{}.{}", remote.schema, remote.name),
        estimated_rows = ?row_count,
        partitions = split.partition_count(),
        "resolved postgresql table"
    );
    Ok(Some(PostgresTable {
        columns,
        remote,
        split,
        row_count,
    }))
}

/// Primary-key range splits for large tables with a single integer key.
async fn plan_table_splits(
    state: &CatalogState,
    client: &tokio_postgres::Client,
    oid: u32,
    remote: &RemoteTable,
    estimated_rows: u64,
) -> Result<SplitPlan, String> {
    let config = &state.config;
    if config.max_splits <= 1 || estimated_rows < config.split_min_rows {
        return Ok(SplitPlan::single());
    }
    let keys = client
        .query(
            &format!(
                "SELECT a.attname, {TYPE_NAME} FROM pg_index i \
             JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) \
             JOIN pg_type t ON t.oid = a.atttypid \
             WHERE i.indrelid = $1 AND i.indisprimary"
            ),
            &[&oid],
        )
        .await
        .map_err(|e| pg_error(&e))?;
    let [key] = keys.as_slice() else {
        return Ok(SplitPlan::single());
    };
    let column: String = key.get(0);
    let typname: String = key.get(1);
    if !matches!(typname.as_str(), "int2" | "int4" | "int8") {
        return Ok(SplitPlan::single());
    }
    let q = |s: &str| arneb_rdbms::Dialect::quote_identifier(&PostgresDialect, s);
    let sql = format!(
        "SELECT min({c})::int8, max({c})::int8 FROM {}.{}",
        q(&remote.schema),
        q(&remote.name),
        c = q(&column)
    );
    let row = client
        .query_one(&sql, &[])
        .await
        .map_err(|e| pg_error(&e))?;
    let (Some(min), Some(max)) = (row.get::<_, Option<i64>>(0), row.get::<_, Option<i64>>(1))
    else {
        return Ok(SplitPlan::single());
    };
    let bounds = plan_splits(
        min,
        max,
        estimated_rows,
        config.split_min_rows,
        config.max_splits,
    );
    Ok(SplitPlan::new(column, bounds))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name_resolution_prefers_exact_then_unique_case_insensitive() {
        let names = vec!["Nation".to_string(), "orders".to_string()];
        assert_eq!(resolve_name(&names, "Nation").as_deref(), Some("Nation"));
        assert_eq!(resolve_name(&names, "nation").as_deref(), Some("Nation"));
        assert_eq!(resolve_name(&names, "missing"), None);
        let ambiguous = vec!["T".to_string(), "t2".to_string(), "t".to_string()];
        assert_eq!(resolve_name(&ambiguous, "t").as_deref(), Some("t"));
        let ambiguous = vec!["Ab".to_string(), "aB".to_string()];
        assert_eq!(resolve_name(&ambiguous, "ab"), None);
    }
}
