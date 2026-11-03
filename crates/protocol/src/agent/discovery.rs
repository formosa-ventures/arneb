//! Cheap discovery for agents: bounded listings of catalogs, schemas and
//! tables, a table description with statistics, and a bounded sample. Names
//! come from the same access-scoped view `SHOW` uses, so an object the user
//! may not see answers exactly like one that does not exist.

use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use arneb_catalog::{CatalogManager, TableProvider};
use arneb_common::suggest::nearest;
use arneb_common::TableReference;

use super::error::{AgentError, AgentErrorCode};
use super::service::AgentQueryService;
use super::statement::{StatementStatus, SubmitRequest};
use crate::trino::{
    build_virtual_catalogs, catalog_names, schema_names, table_columns, table_names, trino_type_of,
    INFORMATION_SCHEMA, SYSTEM_CATALOG,
};

/// Rows a sample returns when `rows` is not given.
pub const DEFAULT_SAMPLE_ROWS: usize = 10;
/// Most rows a sample may ask for.
pub const MAX_SAMPLE_ROWS: usize = 100;
/// Nearest names offered in a not-found error.
const MAX_CANDIDATES: usize = 3;

/// Query parameters of a listing.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ListRequest {
    /// Keep only names starting with this (case-insensitive).
    pub prefix: Option<String>,
    /// Most names to return (default `default_max_rows`).
    pub limit: Option<usize>,
}

/// One named object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Named {
    /// The object's name.
    pub name: String,
}

/// A bounded, name-sorted listing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Listing {
    /// Up to `limit` matching names, sorted.
    pub items: Vec<Named>,
    /// Matches before the limit was applied.
    pub total: usize,
    /// Whether `total` exceeds the items returned.
    pub truncated: bool,
}

/// One column of a described table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ColumnDescription {
    /// Column name.
    pub name: String,
    /// Trino type, e.g. `bigint` or `decimal(12,2)`.
    #[serde(rename = "type")]
    pub type_name: String,
    /// Whether the column may be NULL.
    pub nullable: bool,
}

/// Table statistics; null where the connector does not know.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TableStats {
    /// Approximate row count.
    pub row_count: Option<u64>,
    /// Size in bytes (on disk where applicable).
    pub size_bytes: Option<u64>,
}

/// A table's columns and statistics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TableDescription {
    /// Catalog name.
    pub catalog: String,
    /// Schema name.
    pub schema: String,
    /// Table name.
    pub table: String,
    /// Columns in table order, at most the request's `limit`.
    pub columns: Vec<ColumnDescription>,
    /// Whether columns beyond `limit` were dropped.
    pub truncated: bool,
    /// Row count and size, when known.
    pub statistics: TableStats,
}

fn not_found(
    code: AgentErrorCode,
    what: &str,
    path: &str,
    target: &str,
    visible: &[String],
    list_path: &str,
) -> AgentError {
    let mut err = AgentError::new(code, format!("{what} '{path}' does not exist"))
        .with_hint(format!("list {} with GET /api/v2{list_path}", plural(what)));
    err.candidates = nearest(target, visible, MAX_CANDIDATES);
    err
}

fn plural(what: &str) -> &'static str {
    match what {
        "Catalog" => "catalogs",
        "Schema" => "schemas",
        _ => "tables",
    }
}

fn catalog_missing(catalog: &str, visible: &[String]) -> AgentError {
    not_found(
        AgentErrorCode::CatalogNotFound,
        "Catalog",
        catalog,
        catalog,
        visible,
        "/catalogs",
    )
}

fn schema_missing(catalog: &str, schema: &str, visible: &[String]) -> AgentError {
    not_found(
        AgentErrorCode::SchemaNotFound,
        "Schema",
        &format!("{catalog}.{schema}"),
        schema,
        visible,
        &format!("/catalogs/{catalog}/schemas"),
    )
}

fn table_missing(catalog: &str, schema: &str, table: &str, visible: &[String]) -> AgentError {
    not_found(
        AgentErrorCode::TableNotFound,
        "Table",
        &format!("{catalog}.{schema}.{table}"),
        table,
        visible,
        &format!("/catalogs/{catalog}/schemas/{schema}/tables"),
    )
}

/// Double-quotes an identifier, doubling embedded quotes.
fn quote(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

impl AgentQueryService {
    /// A caller's `limit`: `default_max_rows` when omitted, refused outside
    /// `1..=max_rows`.
    fn limit(&self, limit: Option<usize>) -> Result<usize, AgentError> {
        match limit {
            None => Ok(self.limits.default_max_rows),
            Some(n) if (1..=self.limits.max_rows).contains(&n) => Ok(n),
            Some(n) => Err(AgentError::new(
                AgentErrorCode::InvalidArgument,
                format!("limit {n} is out of range"),
            )
            .with_hint(format!("limit must be in 1..={}", self.limits.max_rows))),
        }
    }

    /// Applies `req` to `names`: prefix filter, sort, limit.
    fn bound(&self, mut names: Vec<String>, req: &ListRequest) -> Result<Listing, AgentError> {
        let limit = self.limit(req.limit)?;
        if let Some(prefix) = &req.prefix {
            let prefix = prefix.to_lowercase();
            names.retain(|n| n.to_lowercase().starts_with(&prefix));
        }
        names.sort();
        let total = names.len();
        names.truncate(limit);
        Ok(Listing {
            items: names.into_iter().map(|name| Named { name }).collect(),
            total,
            truncated: total > limit,
        })
    }

    /// The catalogs `user` can see, as `SHOW CATALOGS` lists them.
    pub async fn list_catalogs(
        &self,
        user: &str,
        req: &ListRequest,
    ) -> Result<Listing, AgentError> {
        let root = self.scoped_root(user)?;
        self.bound(catalog_names(&root), req)
    }

    /// The schemas of `catalog` that `user` can see.
    pub async fn list_schemas(
        &self,
        user: &str,
        catalog: &str,
        req: &ListRequest,
    ) -> Result<Listing, AgentError> {
        let root = self.scoped_root(user)?;
        let names = schemas_of(&root, catalog).await?;
        self.bound(names, req)
    }

    /// The tables of `catalog.schema` that `user` can see.
    pub async fn list_tables(
        &self,
        user: &str,
        catalog: &str,
        schema: &str,
        req: &ListRequest,
    ) -> Result<Listing, AgentError> {
        let root = self.scoped_root(user)?;
        let names = tables_of(&root, catalog, schema).await?;
        self.bound(names, req)
    }

    /// The columns (in order, with Trino types, at most `limit`: default
    /// `default_max_rows`, ceiling `max_rows`) and statistics of a table.
    pub async fn describe_table(
        &self,
        user: &str,
        catalog: &str,
        schema: &str,
        table: &str,
        limit: Option<usize>,
    ) -> Result<TableDescription, AgentError> {
        let limit = self.limit(limit)?;
        let root = self.scoped_root(user)?;
        // Virtual tables resolve against per-query metadata catalogs, as
        // SHOW COLUMNS does; they have no statistics.
        let (columns, statistics) = if is_virtual(catalog, schema) {
            let visible = tables_of(&root, catalog, schema).await?;
            let missing = || table_missing(catalog, schema, table, &visible);
            if !visible.iter().any(|t| t == table) {
                return Err(missing());
            }
            let probe = format!("{catalog}.information_schema.columns system.jdbc");
            let (cm, _) = build_virtual_catalogs(&root, catalog, &probe).await;
            let columns = table_columns(&cm, catalog, schema, table)
                .await
                .ok_or_else(missing)?;
            (columns, None)
        } else {
            let provider = resolve_visible(&root, catalog, schema, table).await?;
            (provider.schema(), provider.statistics())
        };
        let truncated = columns.len() > limit;
        Ok(TableDescription {
            catalog: catalog.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
            columns: columns
                .into_iter()
                .take(limit)
                .map(|c| ColumnDescription {
                    type_name: trino_type_of(&c.data_type).display(),
                    name: c.name,
                    nullable: c.nullable,
                })
                .collect(),
            truncated,
            statistics: TableStats {
                row_count: statistics.as_ref().and_then(|s| s.row_count),
                size_bytes: statistics.as_ref().and_then(|s| s.size_bytes),
            },
        })
    }

    /// Up to `rows` (default 10, at most 100) rows of a table, run as a
    /// tracked statement and waited for up to `max_wait`. The row count is
    /// further capped at the server's `max_rows`, so a small configured
    /// ceiling shortens the sample instead of failing it. The first page
    /// holds at most `page_bytes` (default `default_page_bytes`) of rows. A
    /// table that does not exist (or that `user` may not see) answers
    /// not-found without running, registering or queueing anything.
    pub async fn sample(
        &self,
        user: &str,
        catalog: &str,
        schema: &str,
        table: &str,
        rows: Option<usize>,
        page_bytes: Option<usize>,
    ) -> Result<StatementStatus, AgentError> {
        self.page_budget("page_bytes", page_bytes)?;
        let requested = match rows {
            None => DEFAULT_SAMPLE_ROWS,
            Some(n) if (1..=MAX_SAMPLE_ROWS).contains(&n) => n,
            Some(n) => {
                return Err(AgentError::new(
                    AgentErrorCode::InvalidArgument,
                    format!("rows {n} is out of range"),
                )
                .with_hint(format!("rows must be in 1..={MAX_SAMPLE_ROWS}")))
            }
        };
        let rows = requested.min(self.limits.max_rows);
        let root = self.scoped_root(user)?;
        if is_virtual(catalog, schema) {
            let visible = tables_of(&root, catalog, schema).await?;
            if !visible.iter().any(|t| t == table) {
                return Err(table_missing(catalog, schema, table, &visible));
            }
        } else if !names_visible(&root, catalog, schema, table) {
            return Err(missing(&root, catalog, schema, table).await);
        } else if let Some(error) = listed_miss(&root, catalog, schema, table, false).await {
            return Err(error);
        }
        let sql = format!(
            "SELECT * FROM {}.{}.{} LIMIT {rows}",
            quote(catalog),
            quote(schema),
            quote(table)
        );
        let wait: Duration = self.limits.max_wait;
        let status = self
            .submit(
                user,
                SubmitRequest {
                    sql,
                    catalog: None,
                    schema: None,
                    max_rows: Some(rows),
                },
                wait,
                page_bytes,
            )
            .await?;
        // The query resolves the table itself (once); when the listings now
        // show a sure miss for the names as the query resolved them, answer
        // with the same not-found error (and candidates) as describe, at the
        // level that is missing. Any other not-found (a view over a missing
        // table, a failed listing, ...) stays the query's own error.
        let not_found = status.error.as_ref().is_some_and(|e| {
            matches!(
                e.code,
                AgentErrorCode::CatalogNotFound
                    | AgentErrorCode::SchemaNotFound
                    | AgentErrorCode::TableNotFound
            )
        });
        if not_found && !is_virtual(catalog, schema) {
            if let Some(error) = listed_miss(&root, catalog, schema, table, true).await {
                self.forget(user, &status.id);
                return Err(error);
            }
        }
        Ok(status)
    }
}

/// The not-found error for `catalog.schema.table` when the visible name
/// listings show it surely cannot resolve; `None` (let the query decide, or
/// keep its own error) whenever it might. A level is a sure miss only when
/// its listing has a real name (one besides the virtual `system` and
/// `information_schema` the listings always add; a failed listing reads as
/// empty) and none matches the requested name ignoring case, since the
/// query folds case and connectors may resolve more loosely (PostgreSQL
/// falls back to a unique case-insensitive match). Only names are listed:
/// no table metadata is loaded.
///
/// Before the query (`after_query` false), a non-exact catalog or schema
/// that matches ignoring case ends the check: the query decides. After a
/// not-found from the query, such a level is resolved the way the query
/// would have resolved it ([`resolve_like_query`]), so the error names the
/// level that is really missing.
async fn listed_miss(
    root: &CatalogManager,
    catalog: &str,
    schema: &str,
    table: &str,
    after_query: bool,
) -> Option<AgentError> {
    /// The name `name` stands for in `names`: itself, else its lowercase
    /// form, else the unique case-insensitive match; `Err(sure miss)` when
    /// nothing matches and the listing has a real name, `Ok(None)` when it
    /// cannot tell.
    fn level<'a>(name: &str, names: &'a [String]) -> Result<Option<&'a str>, ()> {
        if let Some(n) = resolve_like_query(name, names) {
            return Ok(Some(n));
        }
        let lower = name.to_lowercase();
        let real = names
            .iter()
            .any(|n| n != SYSTEM_CATALOG && n != INFORMATION_SCHEMA);
        if real && !names.iter().any(|n| n.to_lowercase() == lower) {
            Err(())
        } else {
            Ok(None)
        }
    }
    let catalogs = catalog_names(root);
    let catalog = match level(catalog, &catalogs) {
        Err(()) => return Some(catalog_missing(catalog, &catalogs)),
        Ok(Some(c)) if c == catalog || after_query => c.to_string(),
        Ok(_) => return None,
    };
    let schemas = schema_names(root, &catalog).await?;
    let schema = match level(schema, &schemas) {
        Err(()) => return Some(schema_missing(&catalog, schema, &schemas)),
        Ok(Some(s)) if s == schema || after_query => s.to_string(),
        Ok(_) => return None,
    };
    // An unreachable source is not a sure miss: the query decides.
    let tables = table_names(root, &catalog, &schema).await.ok()??;
    match level(table, &tables) {
        Err(()) => Some(table_missing(&catalog, &schema, table, &tables)),
        Ok(_) => None,
    }
}

/// The name in `names` that `name` resolves to the way the query and the
/// connectors resolve it: `name` itself, else its lowercase form (the
/// query's fold), else the unique case-insensitive match (PostgreSQL).
fn resolve_like_query<'a>(name: &str, names: &'a [String]) -> Option<&'a str> {
    let lower = name.to_lowercase();
    if let Some(n) = names.iter().find(|n| *n == name) {
        return Some(n);
    }
    if let Some(n) = names.iter().find(|n| **n == lower) {
        return Some(n);
    }
    let mut ci = names.iter().filter(|n| n.to_lowercase() == lower);
    match (ci.next(), ci.next()) {
        (Some(n), None) => Some(n),
        _ => None,
    }
}

/// Whether `catalog.schema` is one of the virtual metadata schemas.
fn is_virtual(catalog: &str, schema: &str) -> bool {
    schema == "information_schema" || catalog == SYSTEM_CATALOG
}

/// The provider of a real table the root's user may see, resolved directly.
/// Only on a miss are the schemas and tables listed, to build the not-found
/// error (at the right level, with candidates) exactly as a listing would.
async fn resolve_visible(
    root: &CatalogManager,
    catalog: &str,
    schema: &str,
    table: &str,
) -> Result<Arc<dyn TableProvider>, AgentError> {
    if names_visible(root, catalog, schema, table) {
        let reference = TableReference {
            catalog: Some(catalog.to_string()),
            schema: Some(schema.to_string()),
            table: table.to_string(),
        };
        if let Ok(provider) = root.resolve_table(&reference).await {
            return Ok(provider);
        }
    }
    Err(missing(root, catalog, schema, table).await)
}

/// Whether the access rules let the root's user see `catalog.schema.table`
/// (it may still not exist). No metadata is loaded.
fn names_visible(root: &CatalogManager, catalog: &str, schema: &str, table: &str) -> bool {
    root.catalog_visible(catalog)
        && root.schema_visible(catalog, schema)
        && root.table_visible(catalog, schema, table)
}

/// The not-found error for a table that is missing or hidden, at the right
/// level and with candidates, exactly as a listing would report it.
async fn missing(root: &CatalogManager, catalog: &str, schema: &str, table: &str) -> AgentError {
    match tables_of(root, catalog, schema).await {
        Ok(visible) => table_missing(catalog, schema, table, &visible),
        Err(err) => err,
    }
}

/// Visible schemas of `catalog`, or catalog_not_found.
async fn schemas_of(root: &CatalogManager, catalog: &str) -> Result<Vec<String>, AgentError> {
    match schema_names(root, catalog).await {
        Some(names) => Ok(names),
        None => Err(catalog_missing(catalog, &catalog_names(root))),
    }
}

/// Visible tables of `catalog.schema`, or the not-found error one level up.
async fn tables_of(
    root: &CatalogManager,
    catalog: &str,
    schema: &str,
) -> Result<Vec<String>, AgentError> {
    let schemas = schemas_of(root, catalog).await?;
    match table_names(root, catalog, schema).await {
        Ok(Some(names)) if schemas.iter().any(|s| s == schema) => Ok(names),
        Err(e) => Err(AgentError::new(
            AgentErrorCode::QueryFailed,
            format!("cannot list the tables of {catalog}.{schema}: {e}"),
        )
        .with_hint("the source is unreachable; retry later")),
        _ => Err(schema_missing(catalog, schema, &schemas)),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use arrow::array::{ArrayRef, Int64Array};
    use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use async_trait::async_trait;

    use arneb_catalog::{
        AccessRules, CatalogManager, CatalogProvider, SchemaProvider, TableProvider,
        TableStatistics,
    };
    use arneb_common::types::{ColumnInfo, DataType};
    use arneb_connectors::memory::{
        MemoryCatalog, MemoryConnectorFactory, MemorySchema, MemoryTable,
    };
    use arneb_connectors::ConnectorRegistry;
    use arneb_scheduler::QueryTracker;

    use super::*;
    use crate::agent::statement::StatementState;
    use crate::agent::{AgentErrorCode, AgentLimits, AgentQueryService};

    fn int_table(rows: i64) -> Arc<MemoryTable> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("l_orderkey", ArrowDataType::Int64, false),
            Field::new("l_comment", ArrowDataType::Int64, true),
        ]));
        let infos = schema
            .fields()
            .iter()
            .map(|f| ColumnInfo {
                name: f.name().clone(),
                data_type: DataType::try_from(f.data_type().clone()).unwrap(),
                nullable: f.is_nullable(),
            })
            .collect();
        let cols: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from_iter_values(0..rows)),
            Arc::new(Int64Array::from_iter_values(0..rows)),
        ];
        let batch = RecordBatch::try_new(schema, cols).unwrap();
        Arc::new(MemoryTable::new(infos, vec![batch]))
    }

    /// A table that reports statistics.
    #[derive(Debug)]
    struct StatsTable;

    impl TableProvider for StatsTable {
        fn schema(&self) -> Vec<ColumnInfo> {
            vec![ColumnInfo {
                name: "id".into(),
                data_type: DataType::Int64,
                nullable: false,
            }]
        }
        fn canonical_name(&self) -> Option<(String, String)> {
            None
        }
        fn statistics(&self) -> Option<TableStatistics> {
            Some(TableStatistics {
                row_count: Some(42),
                size_bytes: Some(1024),
                columns: Default::default(),
            })
        }
    }

    #[derive(Debug)]
    struct StatsSchema;

    #[async_trait]
    impl SchemaProvider for StatsSchema {
        async fn table_names(&self) -> Vec<String> {
            vec!["stat".into()]
        }
        async fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
            (name == "stat").then(|| Arc::new(StatsTable) as Arc<dyn TableProvider>)
        }
        fn canonical_name(&self) -> Option<String> {
            None
        }
    }

    #[derive(Debug)]
    struct StatsCatalog;

    #[async_trait]
    impl CatalogProvider for StatsCatalog {
        async fn schema_names(&self) -> Vec<String> {
            vec!["s".into()]
        }
        async fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
            (name == "s").then(|| Arc::new(StatsSchema) as Arc<dyn SchemaProvider>)
        }
    }

    /// `memory.default`: lineitem (10 rows), orders, `we"ird`, t01..t25.
    fn service(rules: Option<&str>) -> AgentQueryService {
        service_limited(rules, AgentLimits::default())
    }

    /// [`service`] under `limits`.
    fn service_limited(rules: Option<&str>, limits: AgentLimits) -> AgentQueryService {
        let schema = Arc::new(MemorySchema::new());
        schema.register_table("lineitem", int_table(10));
        schema.register_table("orders", int_table(3));
        schema.register_table("we\"ird", int_table(3));
        for i in 1..=25 {
            schema.register_table(format!("t{i:02}"), int_table(1));
        }
        let memory = Arc::new(MemoryCatalog::new());
        memory.register_schema("default", schema);
        let secret = Arc::new(MemorySchema::new());
        secret.register_table("vault", int_table(1));
        memory.register_schema("secret", secret);
        let cm = CatalogManager::new("memory", "default");
        cm.register_catalog("memory", memory.clone());
        cm.register_catalog("stats", Arc::new(StatsCatalog));
        let mut registry = ConnectorRegistry::new();
        registry.register(
            "memory",
            Arc::new(MemoryConnectorFactory::new(memory, "default")),
        );
        let svc = AgentQueryService::new(
            Arc::new(cm),
            Arc::new(registry),
            Arc::new(QueryTracker::new()),
            limits,
        );
        match rules {
            Some(r) => svc.with_access_rules(Some(Arc::new(AccessRules::from_json(r).unwrap()))),
            None => svc,
        }
    }

    fn all() -> ListRequest {
        ListRequest {
            prefix: None,
            limit: None,
        }
    }

    fn names(l: &Listing) -> Vec<&str> {
        l.items.iter().map(|n| n.name.as_str()).collect()
    }

    #[tokio::test]
    async fn list_catalogs_matches_show_catalogs() {
        let svc = service(None);
        let l = svc.list_catalogs("alice", &all()).await.unwrap();
        assert_eq!(names(&l), ["memory", "stats", "system"]);
        assert_eq!(l.total, 3);
        assert!(!l.truncated);
        let show = svc
            .submit(
                "alice",
                crate::agent::SubmitRequest {
                    sql: "SHOW CATALOGS".into(),
                    catalog: None,
                    schema: None,
                    max_rows: None,
                },
                Duration::from_secs(30),
                None,
            )
            .await
            .unwrap();
        assert_eq!(show.total_rows, Some(3));
    }

    #[tokio::test]
    async fn listing_honors_prefix_limit_and_reports_total() {
        let svc = service(None);
        let req = ListRequest {
            prefix: Some("T".into()),
            limit: Some(10),
        };
        let l = svc
            .list_tables("alice", "memory", "default", &req)
            .await
            .unwrap();
        assert_eq!(l.items.len(), 10);
        assert_eq!(l.total, 25);
        assert!(l.truncated);
        assert_eq!(l.items[0].name, "t01");

        for bad in [0, AgentLimits::default().max_rows + 1] {
            let req = ListRequest {
                prefix: None,
                limit: Some(bad),
            };
            let err = svc.list_catalogs("alice", &req).await.unwrap_err();
            assert_eq!(err.code, AgentErrorCode::InvalidArgument);
        }
    }

    #[tokio::test]
    async fn listing_missing_levels_is_not_found_with_candidates() {
        let svc = service(None);
        let err = svc
            .list_schemas("alice", "memry", &all())
            .await
            .unwrap_err();
        assert_eq!(err.code, AgentErrorCode::CatalogNotFound);
        assert_eq!(err.candidates, ["memory"]);
        let err = svc
            .list_tables("alice", "memory", "defalt", &all())
            .await
            .unwrap_err();
        assert_eq!(err.code, AgentErrorCode::SchemaNotFound);
        assert_eq!(err.candidates, ["default"]);
        assert!(err
            .hint
            .unwrap()
            .contains("/api/v2/catalogs/memory/schemas"));
    }

    const HIDE_ORDERS: &str = r#"{"tables": [
        {"user": "bob", "table": "orders", "privileges": []},
        {"privileges": ["SELECT"]}
    ]}"#;

    #[tokio::test]
    async fn hidden_table_is_indistinguishable_from_missing() {
        let svc = service(Some(HIDE_ORDERS));
        let hidden = svc
            .describe_table("bob", "memory", "default", "orders", None)
            .await
            .unwrap_err();
        let missing = svc
            .describe_table("bob", "memory", "default", "nosuch", None)
            .await
            .unwrap_err();
        assert_eq!(hidden.code, AgentErrorCode::TableNotFound);
        assert_eq!(hidden.code, missing.code);
        assert_eq!(
            hidden.message.replace("orders", "X"),
            missing.message.replace("nosuch", "X")
        );
        assert!(!hidden.candidates.contains(&"orders".to_string()));
        assert!(!missing.candidates.contains(&"orders".to_string()));
        let l = svc
            .list_tables("bob", "memory", "default", &all())
            .await
            .unwrap();
        assert!(!names(&l).contains(&"orders"));
        // alice is not restricted and still sees it
        assert!(svc
            .describe_table("alice", "memory", "default", "orders", None)
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn describe_returns_columns_types_and_stats() {
        let svc = service(None);
        let d = svc
            .describe_table("alice", "memory", "default", "lineitem", None)
            .await
            .unwrap();
        let cols: Vec<_> = d
            .columns
            .iter()
            .map(|c| (c.name.as_str(), c.type_name.as_str(), c.nullable))
            .collect();
        assert_eq!(
            cols,
            [
                ("l_orderkey", "bigint", false),
                ("l_comment", "bigint", true)
            ]
        );
        assert_eq!(d.statistics.row_count, None);
        assert_eq!(d.statistics.size_bytes, None);
        let json = serde_json::to_value(&d).unwrap();
        assert_eq!(json["columns"][0]["type"], "bigint");

        let d = svc
            .describe_table("alice", "stats", "s", "stat", None)
            .await
            .unwrap();
        assert_eq!(d.statistics.row_count, Some(42));
        assert_eq!(d.statistics.size_bytes, Some(1024));
    }

    /// Listings and table resolutions of a [`CountingSchema`].
    #[derive(Debug, Default)]
    struct Counts {
        listings: std::sync::atomic::AtomicUsize,
        resolutions: std::sync::atomic::AtomicUsize,
    }

    /// A schema `s` with table `stat` that counts its listings and
    /// resolutions.
    #[derive(Debug)]
    struct CountingSchema(Arc<Counts>);

    #[async_trait]
    impl SchemaProvider for CountingSchema {
        async fn table_names(&self) -> Vec<String> {
            self.0
                .listings
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            vec!["stat".into()]
        }
        async fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
            self.0
                .resolutions
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            StatsSchema.table(name).await
        }
        fn canonical_name(&self) -> Option<String> {
            None
        }
    }

    #[derive(Debug)]
    struct CountingCatalog(Arc<Counts>);

    #[async_trait]
    impl CatalogProvider for CountingCatalog {
        async fn schema_names(&self) -> Vec<String> {
            vec!["s".into()]
        }
        async fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
            (name == "s")
                .then(|| Arc::new(CountingSchema(Arc::clone(&self.0))) as Arc<dyn SchemaProvider>)
        }
    }

    #[tokio::test]
    async fn describe_resolves_without_listing_and_sample_resolves_once() {
        let counts = Arc::new(Counts::default());
        let cm = CatalogManager::new("counted", "s");
        cm.register_catalog("counted", Arc::new(CountingCatalog(Arc::clone(&counts))));
        let svc = AgentQueryService::new(
            Arc::new(cm),
            Arc::new(ConnectorRegistry::new()),
            Arc::new(QueryTracker::new()),
            AgentLimits::default(),
        );
        let d = svc
            .describe_table("alice", "counted", "s", "stat", None)
            .await
            .unwrap();
        assert_eq!(d.statistics.row_count, Some(42));
        let resolutions = || counts.resolutions.load(std::sync::atomic::Ordering::SeqCst);
        // What the sample's own query costs, run as a plain statement. It
        // may fail (no connector); only how often the table is resolved
        // matters here.
        let before = resolutions();
        let _ = svc
            .submit(
                "alice",
                SubmitRequest {
                    sql: "SELECT * FROM \"counted\".\"s\".\"stat\" LIMIT 1".into(),
                    catalog: None,
                    schema: None,
                    max_rows: Some(1),
                },
                Duration::from_secs(5),
                None,
            )
            .await;
        let query = resolutions() - before;
        let before = resolutions();
        let _ = svc
            .sample("alice", "counted", "s", "stat", Some(1), None)
            .await;
        assert_eq!(
            resolutions() - before,
            query,
            "the sample must not resolve the table beyond its own query"
        );
        let count = || counts.listings.load(std::sync::atomic::Ordering::SeqCst);
        // The sample checks the name against the schema's table names
        // (which load no table metadata) before running anything.
        assert_eq!(count(), 1, "a sample lists the schema once");
        let d = svc
            .describe_table("alice", "counted", "s", "stat", None)
            .await;
        assert!(d.is_ok());
        assert_eq!(count(), 1, "a describe hit must not list the schema");
        let err = svc
            .describe_table("alice", "counted", "s", "stta", None)
            .await
            .unwrap_err();
        assert_eq!(err.code, AgentErrorCode::TableNotFound);
        assert_eq!(err.candidates, vec!["stat".to_string()]);
        assert_eq!(count(), 2, "a miss lists once for the candidates");
    }

    /// A schema `public` that lists `listed` and resolves a table as
    /// PostgreSQL does: exactly, else by a unique case-insensitive match
    /// against `tables`.
    #[derive(Debug)]
    struct LooseSchema {
        listed: Vec<String>,
        tables: Vec<String>,
    }

    #[async_trait]
    impl SchemaProvider for LooseSchema {
        async fn table_names(&self) -> Vec<String> {
            self.listed.clone()
        }
        async fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
            let lower = name.to_lowercase();
            let exact = self.tables.iter().find(|t| *t == name);
            let mut ci = self.tables.iter().filter(|t| t.to_lowercase() == lower);
            let stored = match (exact, ci.next(), ci.next()) {
                (Some(t), _, _) | (None, Some(t), None) => t.clone(),
                _ => return None,
            };
            Some(Arc::new(StoredTable(stored)) as Arc<dyn TableProvider>)
        }
        fn canonical_name(&self) -> Option<String> {
            Some("public".into())
        }
    }

    /// A [`StatsTable`] that reports the stored name it was resolved to.
    #[derive(Debug)]
    struct StoredTable(String);

    impl TableProvider for StoredTable {
        fn schema(&self) -> Vec<ColumnInfo> {
            StatsTable.schema()
        }
        fn canonical_name(&self) -> Option<(String, String)> {
            Some(("public".into(), self.0.clone()))
        }
    }

    /// A catalog with schema `public` (resolved case-insensitively, as
    /// PostgreSQL does), listing `public` unless `schemas_fail`.
    #[derive(Debug)]
    struct LooseCatalog(Vec<String>, Vec<String>, bool);

    #[async_trait]
    impl CatalogProvider for LooseCatalog {
        async fn schema_names(&self) -> Vec<String> {
            if self.2 {
                Vec::new() // a failed listing, as connectors report one
            } else {
                vec!["public".into()]
            }
        }
        async fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
            (name.to_lowercase() == "public").then(|| {
                Arc::new(LooseSchema {
                    listed: self.0.clone(),
                    tables: self.1.clone(),
                }) as Arc<dyn SchemaProvider>
            })
        }
    }

    /// A service over catalog `pg` ([`LooseCatalog`]) and its tracker.
    fn loose(listed: &[&str], tables: &[&str]) -> (AgentQueryService, Arc<QueryTracker>) {
        loose_with(false, listed, tables)
    }

    /// [`loose`], with the schema listing failing when `schemas_fail`.
    fn loose_with(
        schemas_fail: bool,
        listed: &[&str],
        tables: &[&str],
    ) -> (AgentQueryService, Arc<QueryTracker>) {
        let names = |v: &[&str]| v.iter().map(|s| s.to_string()).collect();
        let cm = CatalogManager::new("pg", "public");
        let catalog = LooseCatalog(names(listed), names(tables), schemas_fail);
        cm.register_catalog("pg", Arc::new(catalog));
        let tracker = Arc::new(QueryTracker::new());
        let svc = AgentQueryService::new(
            Arc::new(cm),
            Arc::new(ConnectorRegistry::new()),
            Arc::clone(&tracker),
            AgentLimits::default(),
        );
        (svc, tracker)
    }

    #[tokio::test]
    async fn sample_never_refuses_what_the_query_might_resolve() {
        // The query folds an unmatched name to lowercase: these sampled at
        // 6f7e56a, and still do.
        let svc = service(None);
        for (catalog, schema, table) in [
            ("memory", "default", "LINEITEM"),
            ("MEMORY", "DEFAULT", "LineItem"),
            ("memory", "INFORMATION_SCHEMA", "TABLES"),
        ] {
            let s = svc
                .sample("alice", catalog, schema, table, Some(1), None)
                .await
                .unwrap();
            assert_eq!(s.state, StatementState::Finished, "{:?}", s.error);
        }
        // PostgreSQL resolves "Orders" for orders or ORDERS (a unique
        // case-insensitive match): the query runs and resolves it. With no
        // connector behind the stub it then fails, but not as not-found
        // (that would have come back as a table_not_found error).
        let (svc, tracker) = loose(&["Orders"], &["Orders"]);
        for name in ["orders", "ORDERS", "Orders"] {
            let s = svc
                .sample("alice", "pg", "public", name, Some(1), None)
                .await;
            assert!(s.is_ok(), "{name}: {:?}", s.unwrap_err());
        }
        assert_eq!(tracker.list_queries(None).len(), 3);
        // A failed listing reads as empty: never a sure miss, the query
        // decides.
        let (svc, tracker) = loose(&[], &["orders"]);
        assert!(svc
            .sample("alice", "pg", "public", "orders", None, None)
            .await
            .is_ok());
        assert_eq!(tracker.list_queries(None).len(), 1);
        // No case-insensitive match: refused before running.
        let (svc, tracker) = loose(&["Orders"], &["Orders"]);
        let err = svc
            .sample("alice", "pg", "public", "ordrs", None, None)
            .await
            .unwrap_err();
        assert_eq!(err.code, AgentErrorCode::TableNotFound);
        assert_eq!(err.candidates, ["Orders"]);
        assert!(tracker.list_queries(None).is_empty());
        // A failed schema listing (only the virtual information_schema
        // left in it) is not a sure miss either.
        let (svc, tracker) = loose_with(true, &["orders"], &["orders"]);
        let s = svc
            .sample("alice", "pg", "public", "orders", None, None)
            .await;
        assert!(s.is_ok(), "{:?}", s.unwrap_err());
        assert_eq!(tracker.list_queries(None).len(), 1);
        // A hidden table is never matched: bob cannot see orders.
        let svc = service(Some(HIDE_ORDERS));
        let err = svc
            .sample("bob", "memory", "default", "ORDERS", None, None)
            .await
            .unwrap_err();
        assert_eq!(err.code, AgentErrorCode::TableNotFound);
        assert!(!err.candidates.contains(&"orders".to_string()));
    }

    #[tokio::test]
    async fn a_missing_table_under_a_case_folded_schema_is_table_not_found() {
        // The schema matches only ignoring case, so the query runs; its
        // not-found is reported at the table level, under the schema the
        // query resolved, not as a missing schema.
        let (svc, _) = loose(&["Orders"], &["Orders"]);
        let err = svc
            .sample("alice", "pg", "PUBLIC", "ordrs", None, None)
            .await
            .unwrap_err();
        assert_eq!(err.code, AgentErrorCode::TableNotFound, "{err:?}");
        assert_eq!(err.candidates, ["Orders"]);
        assert!(err.message.contains("pg.public.ordrs"), "{}", err.message);
        let svc = service(None);
        let err = svc
            .sample(
                "alice",
                "memory",
                "INFORMATION_SCHEMA",
                "TABLEZ",
                None,
                None,
            )
            .await
            .unwrap_err();
        assert_eq!(err.code, AgentErrorCode::TableNotFound, "{err:?}");
        let err = svc
            .sample("alice", "memory", "DEFAULT", "LINEITM", None, None)
            .await
            .unwrap_err();
        assert_eq!(err.code, AgentErrorCode::TableNotFound, "{err:?}");
        assert_eq!(err.candidates[0], "lineitem");
    }

    #[tokio::test]
    async fn sampling_a_missing_table_runs_nothing() {
        let tracker = Arc::new(QueryTracker::new());
        let cm = CatalogManager::new("memory", "default");
        let schema = Arc::new(MemorySchema::new());
        schema.register_table("lineitem", int_table(10));
        let memory = Arc::new(MemoryCatalog::new());
        memory.register_schema("default", schema);
        cm.register_catalog("memory", memory);
        let admission = arneb_scheduler::AdmissionController::new(1, 10);
        let svc = AgentQueryService::new(
            Arc::new(cm),
            Arc::new(ConnectorRegistry::new()),
            Arc::clone(&tracker),
            AgentLimits::default(),
        )
        .with_admission(Some(admission.clone()));
        // Every slot is taken: a sample that queued would wait max_wait.
        let _held = admission.admit("held").unwrap().wait().await;
        let start = std::time::Instant::now();
        let err = svc
            .sample("alice", "memory", "default", "lineitm", Some(5), None)
            .await
            .unwrap_err();
        assert!(start.elapsed() < Duration::from_secs(1));
        assert_eq!(err.code, AgentErrorCode::TableNotFound);
        assert_eq!(err.candidates[0], "lineitem");
        assert!(tracker.list_queries(None).is_empty(), "no statement ran");
        let snap = admission.snapshot();
        assert_eq!((snap.running, snap.queued), (1, 0), "no slot was asked for");
        for (catalog, schema) in [("memry", "default"), ("memory", "defalt")] {
            let err = svc
                .sample("alice", catalog, schema, "lineitem", None, None)
                .await
                .unwrap_err();
            assert!(matches!(
                err.code,
                AgentErrorCode::CatalogNotFound | AgentErrorCode::SchemaNotFound
            ));
        }
        assert!(tracker.list_queries(None).is_empty());
    }

    #[tokio::test]
    async fn describe_bounds_columns_by_limit() {
        let infos: Vec<ColumnInfo> = (0..30)
            .map(|i| ColumnInfo {
                name: format!("c{i:02}"),
                data_type: DataType::Int64,
                nullable: true,
            })
            .collect();
        let schema = Arc::new(MemorySchema::new());
        schema.register_table("wide", Arc::new(MemoryTable::new(infos, vec![])));
        let memory = Arc::new(MemoryCatalog::new());
        memory.register_schema("default", schema);
        let cm = CatalogManager::new("memory", "default");
        cm.register_catalog("memory", memory);
        let svc = AgentQueryService::new(
            Arc::new(cm),
            Arc::new(ConnectorRegistry::new()),
            Arc::new(QueryTracker::new()),
            AgentLimits::default(),
        );
        let d = svc
            .describe_table("alice", "memory", "default", "wide", Some(10))
            .await
            .unwrap();
        let names: Vec<_> = d.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(
            names,
            (0..10).map(|i| format!("c{i:02}")).collect::<Vec<_>>()
        );
        assert!(d.truncated);
        let d = svc
            .describe_table("alice", "memory", "default", "wide", None)
            .await
            .unwrap();
        assert_eq!((d.columns.len(), d.truncated), (30, false));
        let d = svc
            .describe_table("alice", "memory", "default", "wide", Some(30))
            .await
            .unwrap();
        assert_eq!((d.columns.len(), d.truncated), (30, false));
        for bad in [0, AgentLimits::default().max_rows + 1] {
            let err = svc
                .describe_table("alice", "memory", "default", "wide", Some(bad))
                .await
                .unwrap_err();
            assert_eq!(err.code, AgentErrorCode::InvalidArgument, "{bad}");
            assert_eq!(err.hint.as_deref(), Some("limit must be in 1..=10000"));
        }
    }

    #[tokio::test]
    async fn table_not_found_suggests_visible_neighbours() {
        let svc = service(None);
        let err = svc
            .describe_table("alice", "memory", "default", "lineitm", None)
            .await
            .unwrap_err();
        assert_eq!(err.code, AgentErrorCode::TableNotFound);
        assert_eq!(err.candidates[0], "lineitem");
        assert!(err
            .hint
            .unwrap()
            .contains("/api/v2/catalogs/memory/schemas/default/tables"));
        let err = svc
            .describe_table("alice", "memry", "default", "lineitem", None)
            .await
            .unwrap_err();
        assert_eq!(err.code, AgentErrorCode::CatalogNotFound);
        let err = svc
            .describe_table("alice", "memory", "defalt", "lineitem", None)
            .await
            .unwrap_err();
        assert_eq!(err.code, AgentErrorCode::SchemaNotFound);
    }

    #[tokio::test]
    async fn sample_returns_bounded_rows_inline() {
        let svc = service(None);
        let s = svc
            .sample("alice", "memory", "default", "lineitem", Some(5), None)
            .await
            .unwrap();
        assert_eq!(s.state, StatementState::Finished, "{:?}", s.error);
        assert_eq!(s.first_page.unwrap().rows.len(), 5);
        let s = svc
            .sample("alice", "memory", "default", "lineitem", None, None)
            .await
            .unwrap();
        assert_eq!(s.first_page.unwrap().rows.len(), 10);
        for bad in [0, 101] {
            let err = svc
                .sample("alice", "memory", "default", "lineitem", Some(bad), None)
                .await
                .unwrap_err();
            assert_eq!(err.code, AgentErrorCode::InvalidArgument);
        }
        let err = svc
            .sample("alice", "memory", "default", "nosuch", Some(5), None)
            .await
            .unwrap_err();
        assert_eq!(err.code, AgentErrorCode::TableNotFound);
    }

    #[tokio::test]
    async fn sample_is_capped_at_a_small_server_max_rows() {
        let limits = AgentLimits {
            default_max_rows: 5,
            max_rows: 5,
            ..AgentLimits::default()
        };
        limits.validate().unwrap();
        let svc = service_limited(None, limits);
        for rows in [None, Some(3), Some(50)] {
            let s = svc
                .sample("alice", "memory", "default", "lineitem", rows, None)
                .await
                .unwrap();
            assert_eq!(s.state, StatementState::Finished, "{rows:?}: {:?}", s.error);
            let want = rows.unwrap_or(10).min(5);
            assert_eq!(s.first_page.unwrap().rows.len(), want, "{rows:?}");
            assert_eq!(s.max_rows, want);
        }
        // The caller's own ceiling still applies.
        let err = svc
            .sample("alice", "memory", "default", "lineitem", Some(101), None)
            .await
            .unwrap_err();
        assert!(err.hint.unwrap().contains("rows must be"));
    }

    #[tokio::test]
    async fn describe_information_schema_table() {
        let svc = service(None);
        let d = svc
            .describe_table("alice", "memory", "information_schema", "tables", None)
            .await
            .unwrap();
        let names: Vec<_> = d.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"table_name"), "{names:?}");
        assert_eq!(d.statistics.row_count, None);
        let d = svc
            .describe_table("alice", "system", "jdbc", "tables", None)
            .await
            .unwrap();
        assert!(!d.columns.is_empty());
    }

    #[tokio::test]
    async fn sample_information_schema_table() {
        let svc = service(None);
        let s = svc
            .sample(
                "alice",
                "memory",
                "information_schema",
                "tables",
                Some(5),
                None,
            )
            .await
            .unwrap();
        assert_eq!(s.state, StatementState::Finished, "{:?}", s.error);
        assert_eq!(s.first_page.unwrap().rows.len(), 5);
    }

    const HIDE_SCHEMA_AND_CATALOG: &str = r#"{
        "catalogs": [
            {"user": "bob", "catalog": "stats", "allow": "none"},
            {"allow": "all"}
        ],
        "schemas": [
            {"user": "bob", "catalog": "memory", "schema": "secret", "owner": false},
            {"owner": true}
        ],
        "tables": [
            {"user": "bob", "schema": "default", "privileges": ["SELECT"]},
            {"user": "alice", "privileges": ["SELECT"]}
        ]
    }"#;

    #[tokio::test]
    async fn hidden_schema_and_catalog_are_indistinguishable_from_missing() {
        let svc = service(Some(HIDE_SCHEMA_AND_CATALOG));
        // catalog "stats" hidden from bob vs "nocat" missing
        let hidden = svc.list_schemas("bob", "stats", &all()).await.unwrap_err();
        let missing = svc.list_schemas("bob", "nocat", &all()).await.unwrap_err();
        assert_eq!(hidden.code, AgentErrorCode::CatalogNotFound);
        assert_eq!(hidden.code, missing.code);
        assert_eq!(
            hidden.message.replace("stats", "X"),
            missing.message.replace("nocat", "X")
        );
        assert!(!hidden.candidates.contains(&"stats".to_string()));
        assert!(!missing.candidates.contains(&"stats".to_string()));
        let d = svc
            .describe_table("bob", "stats", "s", "stat", None)
            .await
            .unwrap_err();
        assert_eq!(d.code, AgentErrorCode::CatalogNotFound);
        assert!(!d.candidates.contains(&"stats".to_string()));
        // alice still sees it
        assert!(svc.list_schemas("alice", "stats", &all()).await.is_ok());

        // schema "secret" hidden from bob vs "nosuch" missing
        let hidden = svc
            .list_tables("bob", "memory", "secret", &all())
            .await
            .unwrap_err();
        let missing = svc
            .list_tables("bob", "memory", "nosuch", &all())
            .await
            .unwrap_err();
        assert_eq!(hidden.code, AgentErrorCode::SchemaNotFound);
        assert_eq!(hidden.code, missing.code);
        assert_eq!(
            hidden.message.replace("secret", "X"),
            missing.message.replace("nosuch", "X")
        );
        assert!(!hidden.candidates.contains(&"secret".to_string()));
        let d = svc
            .describe_table("bob", "memory", "secret", "vault", None)
            .await
            .unwrap_err();
        assert_eq!(d.code, AgentErrorCode::SchemaNotFound);
        assert!(!d.candidates.contains(&"secret".to_string()));
        assert!(svc
            .list_tables("alice", "memory", "secret", &all())
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn sample_quotes_identifiers() {
        let svc = service(None);
        let s = svc
            .sample("alice", "memory", "default", "we\"ird", Some(2), None)
            .await
            .unwrap();
        assert_eq!(s.state, StatementState::Finished, "{:?}", s.error);
        assert_eq!(s.first_page.unwrap().rows.len(), 2);
    }
}
