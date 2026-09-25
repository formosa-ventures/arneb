//! Catalog browsing for Trino clients: `SHOW …` / `DESCRIBE` results and
//! virtual `information_schema` / `system.jdbc` / `system.metadata` tables.
//!
//! The virtual tables are materialized from the live [`CatalogManager`] into
//! an in-memory catalog per query, and the client's SQL then runs over them
//! through the regular planner/executor — so filters, projections, ordering
//! and `LIMIT` behave exactly as for any other table.

use std::sync::Arc;

use arneb_catalog::CatalogManager;
use arneb_common::types::{ColumnInfo, DataType, TableReference};
use arneb_connectors::memory::{MemoryCatalog, MemoryConnectorFactory, MemorySchema, MemoryTable};
use arneb_connectors::ConnectorRegistry;
use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType as ArrowType, Field, Schema};

use super::types::{trino_type_of, TrinoType};

/// Name of the virtual catalog that hosts `system.jdbc` / `system.metadata`.
pub(crate) const SYSTEM_CATALOG: &str = "system";
const INFORMATION_SCHEMA: &str = "information_schema";
const INFO_SCHEMA_TABLES: [&str; 4] = ["columns", "schemata", "tables", "views"];

/// A cell of a metadata row.
#[derive(Debug, Clone)]
pub(crate) enum Cell {
    Str(Option<String>),
    Int(Option<i64>),
}

fn s(v: impl Into<String>) -> Cell {
    Cell::Str(Some(v.into()))
}

const NULL_STR: Cell = Cell::Str(None);
const NULL_INT: Cell = Cell::Int(None);

/// Column-typed row buffer that turns into an Arrow batch / memory table.
pub(crate) struct TableBuilder {
    columns: Vec<(&'static str, bool)>, // (name, is_int)
    rows: Vec<Vec<Cell>>,
}

impl TableBuilder {
    /// `columns`: names; a trailing `#` marks a bigint column.
    pub(crate) fn new(columns: &[&'static str]) -> Self {
        Self {
            columns: columns
                .iter()
                .map(|c| match c.strip_suffix('#') {
                    Some(name) => (name, true),
                    None => (*c, false),
                })
                .collect(),
            rows: Vec::new(),
        }
    }

    pub(crate) fn push(&mut self, row: Vec<Cell>) {
        debug_assert_eq!(row.len(), self.columns.len());
        self.rows.push(row);
    }

    pub(crate) fn column_infos(&self) -> Vec<ColumnInfo> {
        self.columns
            .iter()
            .map(|(name, is_int)| ColumnInfo {
                name: name.to_string(),
                data_type: if *is_int {
                    DataType::Int64
                } else {
                    DataType::Utf8
                },
                nullable: true,
            })
            .collect()
    }

    pub(crate) fn batch(&self) -> RecordBatch {
        let fields: Vec<Field> = self
            .columns
            .iter()
            .map(|(name, is_int)| {
                let ty = if *is_int {
                    ArrowType::Int64
                } else {
                    ArrowType::Utf8
                };
                Field::new(*name, ty, true)
            })
            .collect();
        let arrays: Vec<ArrayRef> = (0..self.columns.len())
            .map(|i| -> ArrayRef {
                if self.columns[i].1 {
                    Arc::new(Int64Array::from_iter(self.rows.iter().map(|r| {
                        match &r[i] {
                            Cell::Int(v) => *v,
                            Cell::Str(_) => None,
                        }
                    })))
                } else {
                    Arc::new(StringArray::from_iter(self.rows.iter().map(|r| {
                        match &r[i] {
                            Cell::Str(v) => v.clone(),
                            Cell::Int(v) => v.map(|n| n.to_string()),
                        }
                    })))
                }
            })
            .collect();
        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
            .expect("metadata batch columns match schema")
    }

    fn into_memory_table(self) -> Arc<MemoryTable> {
        let batch = self.batch();
        Arc::new(MemoryTable::new(self.column_infos(), vec![batch]))
    }
}

/// One table's metadata, as the virtual tables need it.
struct TableMeta {
    catalog: String,
    schema: String,
    table: String,
    /// Loaded only when the query touches a `columns` table.
    columns: Vec<ColumnInfo>,
}

/// Lists the catalogs visible to Trino clients (registered + `system`).
pub(crate) fn catalog_names(cm: &CatalogManager) -> Vec<String> {
    let mut names = cm.catalog_names();
    if !names.iter().any(|n| n == SYSTEM_CATALOG) {
        names.push(SYSTEM_CATALOG.to_string());
    }
    names.sort();
    names
}

/// Schemas of a catalog, including the virtual `information_schema`.
pub(crate) async fn schema_names(cm: &CatalogManager, catalog: &str) -> Option<Vec<String>> {
    let mut names = if catalog == SYSTEM_CATALOG && cm.catalog(catalog).is_none() {
        vec!["jdbc".to_string(), "metadata".to_string()]
    } else {
        cm.catalog(catalog)?.schema_names().await
    };
    if !names.iter().any(|n| n == INFORMATION_SCHEMA) {
        names.push(INFORMATION_SCHEMA.to_string());
    }
    names.sort();
    Some(names)
}

/// Tables of `catalog.schema` (virtual tables for the virtual schemas).
pub(crate) async fn table_names(
    cm: &CatalogManager,
    catalog: &str,
    schema: &str,
) -> Option<Vec<String>> {
    if schema == INFORMATION_SCHEMA {
        return Some(INFO_SCHEMA_TABLES.iter().map(|t| t.to_string()).collect());
    }
    if catalog == SYSTEM_CATALOG && cm.catalog(catalog).is_none() {
        return match schema {
            "jdbc" => Some(
                [
                    "catalogs",
                    "columns",
                    "schemas",
                    "table_types",
                    "tables",
                    "types",
                ]
                .iter()
                .map(|t| t.to_string())
                .collect(),
            ),
            "metadata" => Some(vec!["table_comments".to_string()]),
            _ => None,
        };
    }
    let mut names = cm
        .catalog(catalog)?
        .schema(schema)
        .await?
        .table_names()
        .await;
    names.sort();
    Some(names)
}

/// Columns of a real table.
pub(crate) async fn table_columns(
    cm: &CatalogManager,
    catalog: &str,
    schema: &str,
    table: &str,
) -> Option<Vec<ColumnInfo>> {
    let provider = cm
        .resolve_table(&TableReference {
            catalog: Some(catalog.to_string()),
            schema: Some(schema.to_string()),
            table: table.to_string(),
        })
        .await
        .ok()?;
    Some(provider.schema())
}

/// Does `sql` reference one of the virtual metadata schemas?
pub(crate) fn references_virtual_metadata(sql: &str) -> bool {
    let lower = sql.to_ascii_lowercase();
    lower.contains(INFORMATION_SCHEMA)
        || lower.contains("system.jdbc")
        || lower.contains("system.metadata")
        || lower.contains("\"system\".\"jdbc\"")
}

async fn collect_tables(
    cm: &CatalogManager,
    catalogs: &[String],
    with_columns: bool,
) -> Vec<TableMeta> {
    let mut out = Vec::new();
    for catalog in catalogs {
        if catalog == SYSTEM_CATALOG && cm.catalog(catalog).is_none() {
            continue;
        }
        let Some(provider) = cm.catalog(catalog) else {
            continue;
        };
        let mut schemas = provider.schema_names().await;
        schemas.sort();
        for schema in schemas {
            let Some(schema_provider) = provider.schema(&schema).await else {
                continue;
            };
            let mut tables = schema_provider.table_names().await;
            tables.sort();
            for table in tables {
                let columns = if with_columns {
                    match schema_provider.table(&table).await {
                        Some(t) => t.schema(),
                        None => continue,
                    }
                } else {
                    Vec::new()
                };
                out.push(TableMeta {
                    catalog: catalog.clone(),
                    schema: schema.clone(),
                    table,
                    columns,
                });
            }
        }
    }
    out
}

fn yes_no(nullable: bool) -> &'static str {
    if nullable {
        "YES"
    } else {
        "NO"
    }
}

/// Column size / decimal digits the Trino JDBC driver reports.
fn jdbc_sizes(ty: &TrinoType) -> (Option<i64>, Option<i64>) {
    match ty {
        TrinoType::TinyInt => (Some(3), Some(0)),
        TrinoType::SmallInt => (Some(5), Some(0)),
        TrinoType::Integer => (Some(10), Some(0)),
        TrinoType::BigInt => (Some(19), Some(0)),
        TrinoType::Real => (Some(24), None),
        TrinoType::Double => (Some(53), None),
        TrinoType::Decimal(p, s) => (Some(*p as i64), Some(*s as i64)),
        TrinoType::Varchar | TrinoType::Varbinary => (Some(2_147_483_647), None),
        TrinoType::Date => (Some(10), None),
        TrinoType::Timestamp(p) | TrinoType::TimestampTz(p) => (Some(19 + *p as i64), None),
        _ => (None, None),
    }
}

/// Builds the per-query metadata catalogs and runs nothing: returns a
/// catalog manager + connector registry the client's SQL can execute against.
///
/// `session_catalog` becomes the default catalog so that an unqualified
/// `information_schema.tables` refers to the session catalog, as in Trino.
pub(crate) async fn build_virtual_catalogs(
    root: &CatalogManager,
    session_catalog: &str,
    sql: &str,
) -> (CatalogManager, ConnectorRegistry) {
    let lower = sql.to_ascii_lowercase();
    let with_columns = lower.contains("columns");
    let wants_system = lower.contains("system.") || lower.contains("\"system\"");
    let real_catalogs = root.catalog_names();

    // Which catalogs' information_schema does the query read? The session
    // catalog (unqualified form) plus any `<catalog>.information_schema`.
    let mut info_catalogs: Vec<String> = catalog_names(root)
        .into_iter()
        .filter(|c| {
            c == session_catalog
                || lower.contains(&format!("{}.information_schema", c.to_ascii_lowercase()))
                || lower.contains(&format!(
                    "\"{}\".\"information_schema\"",
                    c.to_ascii_lowercase()
                ))
        })
        .collect();
    if !info_catalogs.iter().any(|c| c == session_catalog) {
        info_catalogs.push(session_catalog.to_string());
    }

    let scan_catalogs: Vec<String> = if wants_system {
        real_catalogs.clone()
    } else {
        info_catalogs.clone()
    };
    let tables = collect_tables(root, &scan_catalogs, with_columns).await;

    let meta_cm = CatalogManager::new(session_catalog, INFORMATION_SCHEMA);
    let mut registry = ConnectorRegistry::new();

    let mut register = |name: &str, catalog: Arc<MemoryCatalog>| {
        meta_cm.register_catalog(name, catalog.clone());
        registry.register(
            name,
            Arc::new(MemoryConnectorFactory::new(catalog, INFORMATION_SCHEMA)),
        );
    };

    let mut system_catalog: Option<Arc<MemoryCatalog>> = None;
    for catalog in &info_catalogs {
        let mem = Arc::new(MemoryCatalog::new());
        let info = Arc::new(MemorySchema::new());
        let in_catalog: Vec<&TableMeta> = tables.iter().filter(|t| &t.catalog == catalog).collect();

        let mut schemata = TableBuilder::new(&["catalog_name", "schema_name"]);
        for schema in schema_names(root, catalog).await.unwrap_or_default() {
            schemata.push(vec![s(catalog.clone()), s(schema)]);
        }
        info.register_table("schemata", schemata.into_memory_table());

        let mut t = TableBuilder::new(&[
            "table_catalog",
            "table_schema",
            "table_name",
            "table_type",
            "table_comment",
        ]);
        for meta in &in_catalog {
            t.push(vec![
                s(catalog.clone()),
                s(meta.schema.clone()),
                s(meta.table.clone()),
                s("BASE TABLE"),
                NULL_STR,
            ]);
        }
        for name in INFO_SCHEMA_TABLES {
            t.push(vec![
                s(catalog.clone()),
                s(INFORMATION_SCHEMA),
                s(name),
                s("BASE TABLE"),
                NULL_STR,
            ]);
        }
        info.register_table("tables", t.into_memory_table());

        let mut c = TableBuilder::new(&[
            "table_catalog",
            "table_schema",
            "table_name",
            "column_name",
            "ordinal_position#",
            "column_default",
            "is_nullable",
            "data_type",
            "comment",
            "extra_info",
        ]);
        for meta in &in_catalog {
            for (i, col) in meta.columns.iter().enumerate() {
                c.push(vec![
                    s(catalog.clone()),
                    s(meta.schema.clone()),
                    s(meta.table.clone()),
                    s(col.name.clone()),
                    Cell::Int(Some(i as i64 + 1)),
                    NULL_STR,
                    s(yes_no(col.nullable)),
                    s(trino_type_of(&col.data_type).display()),
                    NULL_STR,
                    NULL_STR,
                ]);
            }
        }
        info.register_table("columns", c.into_memory_table());

        let views = TableBuilder::new(&[
            "table_catalog",
            "table_schema",
            "table_name",
            "view_owner",
            "view_definition",
        ]);
        info.register_table("views", views.into_memory_table());

        mem.register_schema(INFORMATION_SCHEMA, info);
        if catalog == SYSTEM_CATALOG {
            system_catalog = Some(mem.clone());
        }
        register(catalog, mem);
    }

    if wants_system && root.catalog(SYSTEM_CATALOG).is_none() {
        let mem = system_catalog.unwrap_or_else(|| Arc::new(MemoryCatalog::new()));
        mem.register_schema("jdbc", Arc::new(jdbc_schema(root, &tables).await));
        mem.register_schema("metadata", Arc::new(metadata_schema(&tables)));
        register(SYSTEM_CATALOG, mem);
    }

    (meta_cm, registry)
}

async fn jdbc_schema(root: &CatalogManager, tables: &[TableMeta]) -> MemorySchema {
    let schema = MemorySchema::new();

    let mut catalogs = TableBuilder::new(&["table_cat"]);
    for c in catalog_names(root) {
        catalogs.push(vec![s(c)]);
    }
    schema.register_table("catalogs", catalogs.into_memory_table());

    let mut schemas = TableBuilder::new(&["table_schem", "table_catalog"]);
    for c in catalog_names(root) {
        for sc in schema_names(root, &c).await.unwrap_or_default() {
            schemas.push(vec![s(sc), s(c.clone())]);
        }
    }
    schema.register_table("schemas", schemas.into_memory_table());

    let mut types = TableBuilder::new(&["table_type"]);
    types.push(vec![s("TABLE")]);
    types.push(vec![s("VIEW")]);
    schema.register_table("table_types", types.into_memory_table());

    let mut t = TableBuilder::new(&[
        "table_cat",
        "table_schem",
        "table_name",
        "table_type",
        "remarks",
        "type_cat",
        "type_schem",
        "type_name",
        "self_referencing_col_name",
        "ref_generation",
    ]);
    for meta in tables {
        t.push(vec![
            s(meta.catalog.clone()),
            s(meta.schema.clone()),
            s(meta.table.clone()),
            s("TABLE"),
            NULL_STR,
            NULL_STR,
            NULL_STR,
            NULL_STR,
            NULL_STR,
            NULL_STR,
        ]);
    }
    schema.register_table("tables", t.into_memory_table());

    let mut c = TableBuilder::new(&[
        "table_cat",
        "table_schem",
        "table_name",
        "column_name",
        "data_type#",
        "type_name",
        "column_size#",
        "buffer_length#",
        "decimal_digits#",
        "num_prec_radix#",
        "nullable#",
        "remarks",
        "column_def",
        "sql_data_type#",
        "sql_datetime_sub#",
        "char_octet_length#",
        "ordinal_position#",
        "is_nullable",
        "scope_catalog",
        "scope_schema",
        "scope_table",
        "source_data_type#",
        "is_autoincrement",
        "is_generatedcolumn",
    ]);
    for meta in tables {
        for (i, col) in meta.columns.iter().enumerate() {
            let ty = trino_type_of(&col.data_type);
            let (size, digits) = jdbc_sizes(&ty);
            let radix = match ty {
                TrinoType::TinyInt
                | TrinoType::SmallInt
                | TrinoType::Integer
                | TrinoType::BigInt
                | TrinoType::Decimal(..) => Some(10),
                TrinoType::Real | TrinoType::Double => Some(2),
                _ => None,
            };
            c.push(vec![
                s(meta.catalog.clone()),
                s(meta.schema.clone()),
                s(meta.table.clone()),
                s(col.name.clone()),
                Cell::Int(Some(ty.jdbc_type_code())),
                s(ty.display()),
                Cell::Int(size),
                NULL_INT,
                Cell::Int(digits),
                Cell::Int(radix),
                Cell::Int(Some(if col.nullable { 1 } else { 0 })),
                NULL_STR,
                NULL_STR,
                NULL_INT,
                NULL_INT,
                Cell::Int(if matches!(ty, TrinoType::Varchar | TrinoType::Varbinary) {
                    size
                } else {
                    None
                }),
                Cell::Int(Some(i as i64 + 1)),
                s(yes_no(col.nullable)),
                NULL_STR,
                NULL_STR,
                NULL_STR,
                NULL_INT,
                s("NO"),
                s("NO"),
            ]);
        }
    }
    schema.register_table("columns", c.into_memory_table());

    let mut ty = TableBuilder::new(&[
        "type_name",
        "data_type#",
        "precision#",
        "literal_prefix",
        "literal_suffix",
        "create_params",
        "nullable#",
        "case_sensitive",
        "searchable#",
        "unsigned_attribute",
        "fixed_prec_scale",
        "auto_increment",
        "local_type_name",
        "minimum_scale#",
        "maximum_scale#",
        "sql_data_type#",
        "sql_datetime_sub#",
        "num_prec_radix#",
    ]);
    for t in [
        TrinoType::Boolean,
        TrinoType::TinyInt,
        TrinoType::SmallInt,
        TrinoType::Integer,
        TrinoType::BigInt,
        TrinoType::Real,
        TrinoType::Double,
        TrinoType::Decimal(38, 0),
        TrinoType::Varchar,
        TrinoType::Varbinary,
        TrinoType::Date,
        TrinoType::Timestamp(3),
    ] {
        let (size, _) = jdbc_sizes(&t);
        let name = match t {
            TrinoType::Decimal(..) => "decimal".to_string(),
            TrinoType::Timestamp(_) => "timestamp".to_string(),
            ref other => other.display(),
        };
        ty.push(vec![
            s(name),
            Cell::Int(Some(t.jdbc_type_code())),
            Cell::Int(size),
            NULL_STR,
            NULL_STR,
            NULL_STR,
            Cell::Int(Some(1)),
            s(if matches!(t, TrinoType::Varchar) {
                "true"
            } else {
                "false"
            }),
            Cell::Int(Some(3)),
            s("false"),
            s("false"),
            s("false"),
            NULL_STR,
            NULL_INT,
            NULL_INT,
            NULL_INT,
            NULL_INT,
            NULL_INT,
        ]);
    }
    schema.register_table("types", ty.into_memory_table());

    schema
}

fn metadata_schema(tables: &[TableMeta]) -> MemorySchema {
    let schema = MemorySchema::new();
    let mut t = TableBuilder::new(&["catalog_name", "schema_name", "table_name", "comment"]);
    for meta in tables {
        t.push(vec![
            s(meta.catalog.clone()),
            s(meta.schema.clone()),
            s(meta.table.clone()),
            NULL_STR,
        ]);
    }
    schema.register_table("table_comments", t.into_memory_table());
    schema
}

#[cfg(test)]
mod tests {
    use super::*;
    use arneb_catalog::{MemoryCatalog as CatMemoryCatalog, MemorySchema as CatMemorySchema};

    fn root() -> CatalogManager {
        let cm = CatalogManager::new("lake", "sales");
        let schema = Arc::new(CatMemorySchema::new());
        schema.register_table(
            "orders",
            Arc::new(arneb_catalog::MemoryTable::new(vec![ColumnInfo {
                name: "id".into(),
                data_type: DataType::Int64,
                nullable: false,
            }])),
        );
        let catalog = Arc::new(CatMemoryCatalog::new());
        catalog.register_schema("sales", schema);
        cm.register_catalog("lake", catalog);
        cm
    }

    #[tokio::test]
    async fn builds_information_schema_for_session_catalog() {
        let root = root();
        let (cm, registry) =
            build_virtual_catalogs(&root, "lake", "SELECT * FROM information_schema.columns").await;
        let columns = cm
            .resolve_table(&TableReference {
                catalog: None,
                schema: Some("information_schema".into()),
                table: "columns".into(),
            })
            .await
            .unwrap();
        assert_eq!(columns.schema()[3].name, "column_name");
        assert!(registry.get("lake").is_some());
        assert!(registry.get("system").is_none());
    }

    #[tokio::test]
    async fn builds_system_jdbc_when_referenced() {
        let root = root();
        let (cm, registry) =
            build_virtual_catalogs(&root, "lake", "SELECT * FROM system.jdbc.tables").await;
        assert!(cm.catalog("system").is_some());
        assert!(registry.get("system").is_some());
        let names = table_names(&root, "system", "jdbc").await.unwrap();
        assert!(names.contains(&"columns".to_string()));
    }

    #[tokio::test]
    async fn lists_schemas_with_information_schema() {
        let root = root();
        let schemas = schema_names(&root, "lake").await.unwrap();
        assert_eq!(schemas, vec!["information_schema", "sales"]);
        assert_eq!(catalog_names(&root), vec!["lake", "system"]);
    }
}
