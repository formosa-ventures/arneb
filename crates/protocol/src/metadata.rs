//! Synthetic PostgreSQL system catalog metadata handler.
//!
//! Intercepts queries to pg_catalog.*, information_schema.*, and version()
//! before they reach the SQL parser, returning synthetic results from CatalogManager.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arneb_catalog::CatalogManager;
use arneb_common::types::DataType;
use arrow::array::{BooleanArray, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};

use crate::encoding::datatype_to_pg_type;

/// Result from a metadata query.
pub(crate) enum MetadataResponse {
    /// Query result with fields and batches (SELECT-style).
    Query(Vec<arrow::datatypes::FieldRef>, Vec<RecordBatch>),
    /// Command complete (SET/RESET-style) — no rows, just a tag.
    Command(String),
}

pub(crate) type MetadataResult = Result<MetadataResponse, String>;

/// Try to handle a metadata query. Returns Some if intercepted, None if not.
pub(crate) async fn try_handle_metadata(
    sql: &str,
    catalog_manager: &CatalogManager,
) -> Option<MetadataResult> {
    let lower = sql.trim().to_lowercase();

    // version()
    if lower.contains("version()") && lower.starts_with("select") {
        return Some(handle_version());
    }

    // pg_catalog tables — match the primary FROM table, not JOINed tables
    // Use "from pg_catalog.X" pattern to find the main table
    if has_from_table(&lower, "pg_type") || lower.contains("pg_catalog.pg_type") {
        return Some(handle_pg_type());
    }
    if has_from_table(&lower, "pg_class") || lower.contains("pg_catalog.pg_class") {
        return Some(handle_pg_class(catalog_manager).await);
    }
    if has_from_table(&lower, "pg_attribute") || lower.contains("pg_catalog.pg_attribute") {
        return Some(handle_pg_attribute(catalog_manager).await);
    }
    if has_from_table(&lower, "pg_namespace") || lower.contains("pg_catalog.pg_namespace") {
        return Some(handle_pg_namespace(catalog_manager).await);
    }
    if has_from_table(&lower, "pg_database") || lower.contains("pg_catalog.pg_database") {
        return Some(handle_pg_database(catalog_manager));
    }

    // information_schema
    if lower.contains("information_schema.tables") {
        return Some(handle_info_tables(catalog_manager).await);
    }
    if lower.contains("information_schema.columns") {
        return Some(handle_info_columns(catalog_manager).await);
    }
    if lower.contains("information_schema.schemata") {
        return Some(handle_info_schemata(catalog_manager).await);
    }

    // current_database(), current_schema(), current_user
    if lower.contains("current_database()")
        || lower.contains("current_schema()")
        || lower.contains("current_user")
    {
        return Some(handle_current_info(catalog_manager));
    }

    // SET / SHOW / RESET — silently accept session commands
    if lower.starts_with("set ") || lower.starts_with("reset ") {
        return Some(handle_empty_ok());
    }
    if lower.starts_with("show ") {
        return Some(handle_show(&lower));
    }

    None
}

/// Check if the SQL has a FROM clause targeting a specific pg_catalog table.
fn has_from_table(lower: &str, table_name: &str) -> bool {
    lower.contains(&format!("from pg_catalog.{table_name}"))
        || lower.contains(&format!("from pg_catalog.{table_name} "))
}

fn stable_hash(s: &str) -> i64 {
    let mut hasher = DefaultHasher::new();
    s.hash(&mut hasher);
    (hasher.finish() & 0x7FFF_FFFF) as i64 // positive i32 range
}

fn pg_type_name(dt: &DataType) -> &'static str {
    match dt {
        DataType::Boolean => "bool",
        DataType::Int8 | DataType::Int16 => "int2",
        DataType::Int32 => "int4",
        DataType::Int64 => "int8",
        DataType::Float32 => "float4",
        DataType::Float64 => "float8",
        DataType::Decimal128 { .. } => "numeric",
        DataType::Utf8 | DataType::LargeUtf8 => "varchar",
        DataType::Binary => "bytea",
        DataType::Date32 => "date",
        DataType::Timestamp { .. } => "timestamp",
        _ => "text",
    }
}

fn pg_type_oid(dt: &DataType) -> i64 {
    let pg_type = datatype_to_pg_type(dt);
    pg_type.oid() as i64
}

type MetaResult = MetadataResult;

fn make_result(schema: Arc<Schema>, batch: RecordBatch) -> MetaResult {
    let fields = schema.fields().to_vec();
    Ok(MetadataResponse::Query(fields, vec![batch]))
}

// ---------------------------------------------------------------------------
// version()
// ---------------------------------------------------------------------------

fn handle_version() -> MetaResult {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "version",
        ArrowDataType::Utf8,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec![format!(
            "Arneb {}",
            env!("CARGO_PKG_VERSION")
        )]))],
    )
    .map_err(|e| e.to_string())?;
    make_result(schema, batch)
}

// ---------------------------------------------------------------------------
// pg_catalog.pg_type
// ---------------------------------------------------------------------------

fn handle_pg_type() -> MetaResult {
    let types: Vec<(&str, i64, i64, &str)> = vec![
        ("bool", 16, 1, "b"),
        ("int2", 21, 2, "b"),
        ("int4", 23, 4, "b"),
        ("int8", 20, 8, "b"),
        ("float4", 700, 4, "b"),
        ("float8", 701, 8, "b"),
        ("numeric", 1700, -1, "b"),
        ("varchar", 1043, -1, "b"),
        ("text", 25, -1, "b"),
        ("bytea", 17, -1, "b"),
        ("date", 1082, 4, "b"),
        ("timestamp", 1114, 8, "b"),
        ("name", 19, 64, "b"),
        ("oid", 26, 4, "b"),
        ("int2vector", 22, -1, "b"),
    ];

    let schema = Arc::new(Schema::new(vec![
        Field::new("oid", ArrowDataType::Int64, false),
        Field::new("typname", ArrowDataType::Utf8, false),
        Field::new("typnamespace", ArrowDataType::Int64, false),
        Field::new("typlen", ArrowDataType::Int64, false),
        Field::new("typtype", ArrowDataType::Utf8, false),
        Field::new("typbasetype", ArrowDataType::Int64, false),
        Field::new("typnotnull", ArrowDataType::Boolean, false),
    ]));

    let pg_catalog_oid = stable_hash("pg_catalog");
    let n = types.len();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(
                types.iter().map(|t| t.1).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                types.iter().map(|t| t.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(vec![pg_catalog_oid; n])),
            Arc::new(Int64Array::from(
                types.iter().map(|t| t.2).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                types.iter().map(|t| t.3).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(vec![0i64; n])),
            Arc::new(BooleanArray::from(vec![false; n])),
        ],
    )
    .map_err(|e| e.to_string())?;
    make_result(schema, batch)
}

// ---------------------------------------------------------------------------
// pg_catalog.pg_namespace
// ---------------------------------------------------------------------------

async fn handle_pg_namespace(catalog_manager: &CatalogManager) -> MetaResult {
    let mut names = Vec::new();
    let mut oids = Vec::new();

    // Built-in namespaces
    names.push("pg_catalog".to_string());
    oids.push(stable_hash("pg_catalog"));
    names.push("information_schema".to_string());
    oids.push(stable_hash("information_schema"));

    // User schemas from catalogs (only include schemas that contain tables)
    for cat_name in catalog_manager.catalog_names() {
        if let Some(catalog) = catalog_manager.catalog(&cat_name) {
            for schema_name in catalog.schema_names().await {
                if names.contains(&schema_name) {
                    continue;
                }
                if let Some(schema) = catalog.schema(&schema_name).await {
                    if !schema.table_names().await.is_empty() {
                        oids.push(stable_hash(&schema_name));
                        names.push(schema_name);
                    }
                }
            }
        }
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("oid", ArrowDataType::Int64, false),
        Field::new("nspname", ArrowDataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(oids)),
            Arc::new(StringArray::from(names)),
        ],
    )
    .map_err(|e| e.to_string())?;
    make_result(schema, batch)
}

// ---------------------------------------------------------------------------
// pg_catalog.pg_class
// ---------------------------------------------------------------------------

async fn handle_pg_class(catalog_manager: &CatalogManager) -> MetaResult {
    let mut oids = Vec::new();
    let mut relnames = Vec::new();
    let mut relnamespaces = Vec::new();
    let mut relkinds = Vec::new();
    let mut relnatts = Vec::new();

    for cat_name in catalog_manager.catalog_names() {
        if let Some(catalog) = catalog_manager.catalog(&cat_name) {
            for schema_name in catalog.schema_names().await {
                let ns_oid = stable_hash(&schema_name);
                if let Some(schema) = catalog.schema(&schema_name).await {
                    for table_name in schema.table_names().await {
                        let table_key = format!("{cat_name}.{schema_name}.{table_name}");
                        oids.push(stable_hash(&table_key));
                        relnames.push(table_name.clone());
                        relnamespaces.push(ns_oid);
                        relkinds.push("r".to_string());
                        let ncols = schema
                            .table(&table_name)
                            .await
                            .map(|t| t.schema().len() as i64)
                            .unwrap_or(0);
                        relnatts.push(ncols);
                    }
                }
            }
        }
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("oid", ArrowDataType::Int64, false),
        Field::new("relname", ArrowDataType::Utf8, false),
        Field::new("relnamespace", ArrowDataType::Int64, false),
        Field::new("relkind", ArrowDataType::Utf8, false),
        Field::new("relnatts", ArrowDataType::Int64, false),
        Field::new("relowner", ArrowDataType::Int64, false),
    ]));
    let n = oids.len();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(oids)),
            Arc::new(StringArray::from(relnames)),
            Arc::new(Int64Array::from(relnamespaces)),
            Arc::new(StringArray::from(relkinds)),
            Arc::new(Int64Array::from(relnatts)),
            Arc::new(Int64Array::from(vec![0i64; n])),
        ],
    )
    .map_err(|e| e.to_string())?;
    make_result(schema, batch)
}

// ---------------------------------------------------------------------------
// pg_catalog.pg_attribute
// ---------------------------------------------------------------------------

async fn handle_pg_attribute(catalog_manager: &CatalogManager) -> MetaResult {
    let mut attrelids = Vec::new();
    let mut attnames = Vec::new();
    let mut atttypids = Vec::new();
    let mut attnums = Vec::new();
    let mut attnotnulls = Vec::new();

    for cat_name in catalog_manager.catalog_names() {
        if let Some(catalog) = catalog_manager.catalog(&cat_name) {
            for schema_name in catalog.schema_names().await {
                if let Some(schema) = catalog.schema(&schema_name).await {
                    for table_name in schema.table_names().await {
                        let table_key = format!("{cat_name}.{schema_name}.{table_name}");
                        let rel_oid = stable_hash(&table_key);
                        if let Some(table) = schema.table(&table_name).await {
                            for (i, col) in table.schema().iter().enumerate() {
                                attrelids.push(rel_oid);
                                attnames.push(col.name.clone());
                                atttypids.push(pg_type_oid(&col.data_type));
                                attnums.push((i + 1) as i64);
                                attnotnulls.push(!col.nullable);
                            }
                        }
                    }
                }
            }
        }
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("attrelid", ArrowDataType::Int64, false),
        Field::new("attname", ArrowDataType::Utf8, false),
        Field::new("atttypid", ArrowDataType::Int64, false),
        Field::new("attnum", ArrowDataType::Int64, false),
        Field::new("attnotnull", ArrowDataType::Boolean, false),
        Field::new("attlen", ArrowDataType::Int64, false),
        Field::new("atttypmod", ArrowDataType::Int64, false),
    ]));
    let n = attrelids.len();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(attrelids)),
            Arc::new(StringArray::from(attnames)),
            Arc::new(Int64Array::from(atttypids)),
            Arc::new(Int64Array::from(attnums)),
            Arc::new(BooleanArray::from(attnotnulls)),
            Arc::new(Int64Array::from(vec![-1i64; n])), // attlen
            Arc::new(Int64Array::from(vec![-1i64; n])), // atttypmod
        ],
    )
    .map_err(|e| e.to_string())?;
    make_result(schema, batch)
}

// ---------------------------------------------------------------------------
// pg_catalog.pg_database
// ---------------------------------------------------------------------------

fn handle_pg_database(catalog_manager: &CatalogManager) -> MetaResult {
    let names = catalog_manager.catalog_names();
    let oids: Vec<i64> = names.iter().map(|n| stable_hash(n)).collect();

    let schema = Arc::new(Schema::new(vec![
        Field::new("oid", ArrowDataType::Int64, false),
        Field::new("datname", ArrowDataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(oids)),
            Arc::new(StringArray::from(names)),
        ],
    )
    .map_err(|e| e.to_string())?;
    make_result(schema, batch)
}

// ---------------------------------------------------------------------------
// information_schema.tables
// ---------------------------------------------------------------------------

async fn handle_info_tables(catalog_manager: &CatalogManager) -> MetaResult {
    let mut catalogs = Vec::new();
    let mut schemas = Vec::new();
    let mut tables = Vec::new();
    let mut types = Vec::new();

    for cat_name in catalog_manager.catalog_names() {
        if let Some(catalog) = catalog_manager.catalog(&cat_name) {
            for schema_name in catalog.schema_names().await {
                if let Some(schema) = catalog.schema(&schema_name).await {
                    for table_name in schema.table_names().await {
                        catalogs.push(cat_name.clone());
                        schemas.push(schema_name.clone());
                        tables.push(table_name);
                        types.push("BASE TABLE".to_string());
                    }
                }
            }
        }
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("table_catalog", ArrowDataType::Utf8, false),
        Field::new("table_schema", ArrowDataType::Utf8, false),
        Field::new("table_name", ArrowDataType::Utf8, false),
        Field::new("table_type", ArrowDataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(catalogs)),
            Arc::new(StringArray::from(schemas)),
            Arc::new(StringArray::from(tables)),
            Arc::new(StringArray::from(types)),
        ],
    )
    .map_err(|e| e.to_string())?;
    make_result(schema, batch)
}

// ---------------------------------------------------------------------------
// information_schema.columns
// ---------------------------------------------------------------------------

async fn handle_info_columns(catalog_manager: &CatalogManager) -> MetaResult {
    let mut catalogs = Vec::new();
    let mut schemas = Vec::new();
    let mut tables = Vec::new();
    let mut col_names = Vec::new();
    let mut ordinals = Vec::new();
    let mut data_types = Vec::new();
    let mut nullables = Vec::new();

    for cat_name in catalog_manager.catalog_names() {
        if let Some(catalog) = catalog_manager.catalog(&cat_name) {
            for schema_name in catalog.schema_names().await {
                if let Some(schema) = catalog.schema(&schema_name).await {
                    for table_name in schema.table_names().await {
                        if let Some(table) = schema.table(&table_name).await {
                            for (i, col) in table.schema().iter().enumerate() {
                                catalogs.push(cat_name.clone());
                                schemas.push(schema_name.clone());
                                tables.push(table_name.clone());
                                col_names.push(col.name.clone());
                                ordinals.push((i + 1) as i64);
                                data_types.push(pg_type_name(&col.data_type).to_string());
                                nullables.push(if col.nullable { "YES" } else { "NO" }.to_string());
                            }
                        }
                    }
                }
            }
        }
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("table_catalog", ArrowDataType::Utf8, false),
        Field::new("table_schema", ArrowDataType::Utf8, false),
        Field::new("table_name", ArrowDataType::Utf8, false),
        Field::new("column_name", ArrowDataType::Utf8, false),
        Field::new("ordinal_position", ArrowDataType::Int64, false),
        Field::new("data_type", ArrowDataType::Utf8, false),
        Field::new("is_nullable", ArrowDataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(catalogs)),
            Arc::new(StringArray::from(schemas)),
            Arc::new(StringArray::from(tables)),
            Arc::new(StringArray::from(col_names)),
            Arc::new(Int64Array::from(ordinals)),
            Arc::new(StringArray::from(data_types)),
            Arc::new(StringArray::from(nullables)),
        ],
    )
    .map_err(|e| e.to_string())?;
    make_result(schema, batch)
}

// ---------------------------------------------------------------------------
// information_schema.schemata
// ---------------------------------------------------------------------------

async fn handle_info_schemata(catalog_manager: &CatalogManager) -> MetaResult {
    let mut catalogs = Vec::new();
    let mut schemas = Vec::new();

    for cat_name in catalog_manager.catalog_names() {
        if let Some(catalog) = catalog_manager.catalog(&cat_name) {
            for schema_name in catalog.schema_names().await {
                if let Some(schema) = catalog.schema(&schema_name).await {
                    if !schema.table_names().await.is_empty() {
                        catalogs.push(cat_name.clone());
                        schemas.push(schema_name);
                    }
                }
            }
        }
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("catalog_name", ArrowDataType::Utf8, false),
        Field::new("schema_name", ArrowDataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(catalogs)),
            Arc::new(StringArray::from(schemas)),
        ],
    )
    .map_err(|e| e.to_string())?;
    make_result(schema, batch)
}

// ---------------------------------------------------------------------------
// current_database() / current_schema() / current_user
// ---------------------------------------------------------------------------

fn handle_current_info(catalog_manager: &CatalogManager) -> MetaResult {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "current_database",
        ArrowDataType::Utf8,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec![catalog_manager
            .default_catalog()
            .to_string()]))],
    )
    .map_err(|e| e.to_string())?;
    make_result(schema, batch)
}

// ---------------------------------------------------------------------------
// SET / SHOW / RESET — session commands
// ---------------------------------------------------------------------------

fn handle_empty_ok() -> MetaResult {
    Ok(MetadataResponse::Command("SET".to_string()))
}

fn handle_show(lower: &str) -> MetaResult {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "setting",
        ArrowDataType::Utf8,
        false,
    )]));
    let value = if lower.contains("search_path") {
        "\"$user\", public"
    } else if lower.contains("server_version") {
        "14.0"
    } else {
        ""
    };
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec![value]))],
    )
    .map_err(|e| e.to_string())?;
    make_result(schema, batch)
}
