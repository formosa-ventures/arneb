//! Synthetic PostgreSQL system catalog metadata handler.
//!
//! Intercepts queries to pg_catalog.*, information_schema.*, and version()
//! before they reach the SQL parser, returning synthetic results from CatalogManager.

use std::sync::Arc;

use arneb_catalog::CatalogManager;
use arneb_common::pg_oid;
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
    user: Option<&str>,
) -> Option<MetadataResult> {
    let lower = sql.trim().to_lowercase();

    // version()
    if lower.contains("version()") && lower.starts_with("select") {
        return Some(handle_version());
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
        return Some(handle_current_info(&lower, catalog_manager, user));
    }

    // Transactions: every statement is read-only and autocommitted, so
    // drivers that wrap statements in a transaction (psycopg2, SQLAlchemy,
    // dbt) get the tag they expect and nothing else happens.
    if let Some(tag) = transaction_tag(&lower) {
        return Some(command_ok(tag));
    }

    // SET / SHOW / RESET — silently accept session commands
    if lower.starts_with("set ") {
        return Some(command_ok("SET"));
    }
    if lower.starts_with("reset ") {
        return Some(command_ok("RESET"));
    }
    if lower.starts_with("show ") {
        return Some(handle_show(&lower));
    }

    None
}

/// The fixed result for a `pg_catalog` statement the engine could not run
/// (pg-catalog-tables D7): the whole of the first table it names, whatever
/// the query asks. Kept for clients whose catalog queries need more SQL
/// than the engine has, until they are covered.
pub(crate) async fn fixed_pg_catalog(
    sql: &str,
    catalog_manager: &CatalogManager,
) -> Option<MetadataResult> {
    let lower = sql.trim().to_lowercase();
    // Match the primary FROM table, not JOINed tables.
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
    None
}

/// Check if the SQL has a FROM clause targeting a specific pg_catalog table.
fn has_from_table(lower: &str, table_name: &str) -> bool {
    lower.contains(&format!("from pg_catalog.{table_name}"))
        || lower.contains(&format!("from pg_catalog.{table_name} "))
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

/// The PostgreSQL version Arneb reports (`server_version`).
const PG_SERVER_VERSION: &str = "14.0";

/// The parameters sent at startup. They agree with [`show_value`]:
/// drivers read `server_version` from here (psycopg2, JDBC) and others
/// with `SHOW`.
pub(crate) fn server_parameters() -> pgwire::api::auth::DefaultServerParameterProvider {
    let mut params = pgwire::api::auth::DefaultServerParameterProvider::default();
    params.server_version = PG_SERVER_VERSION.to_string();
    params.is_superuser = false;
    params
}

/// `version()`: starts with `PostgreSQL <version>` because drivers parse it
/// (SQLAlchemy's psycopg2 dialect fails to connect otherwise), then names
/// Arneb.
fn version_string() -> String {
    format!(
        "PostgreSQL {PG_SERVER_VERSION} on {}-{} (Arneb {})",
        std::env::consts::ARCH,
        std::env::consts::OS,
        env!("CARGO_PKG_VERSION")
    )
}

fn handle_version() -> MetaResult {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "version",
        ArrowDataType::Utf8,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec![version_string()]))],
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

    let pg_catalog_oid = pg_oid::PG_CATALOG_NAMESPACE_OID;
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
    oids.push(pg_oid::PG_CATALOG_NAMESPACE_OID);
    names.push("information_schema".to_string());
    oids.push(pg_oid::namespace_oid(
        catalog_manager.default_catalog(),
        "information_schema",
    ));

    // User schemas from catalogs (only include schemas that contain tables)
    for cat_name in catalog_manager.visible_catalog_names() {
        for schema_name in catalog_manager.visible_schema_names(&cat_name).await {
            if names.contains(&schema_name) {
                continue;
            }
            if !catalog_manager
                .visible_table_names(&cat_name, &schema_name)
                .await
                .is_empty()
            {
                oids.push(pg_oid::namespace_oid(&cat_name, &schema_name));
                names.push(schema_name);
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

    for cat_name in catalog_manager.visible_catalog_names() {
        if let Some(catalog) = catalog_manager.catalog(&cat_name) {
            for schema_name in catalog_manager.visible_schema_names(&cat_name).await {
                let ns_oid = pg_oid::namespace_oid(&cat_name, &schema_name);
                if let Some(schema) = catalog.schema(&schema_name).await {
                    for table_name in catalog_manager
                        .visible_table_names(&cat_name, &schema_name)
                        .await
                    {
                        oids.push(pg_oid::relation_oid(&cat_name, &schema_name, &table_name));
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

    for cat_name in catalog_manager.visible_catalog_names() {
        if let Some(catalog) = catalog_manager.catalog(&cat_name) {
            for schema_name in catalog_manager.visible_schema_names(&cat_name).await {
                if let Some(schema) = catalog.schema(&schema_name).await {
                    for table_name in catalog_manager
                        .visible_table_names(&cat_name, &schema_name)
                        .await
                    {
                        let rel_oid = pg_oid::relation_oid(&cat_name, &schema_name, &table_name);
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
    let names = catalog_manager.visible_catalog_names();
    let oids: Vec<i64> = names.iter().map(|n| pg_oid::database_oid(n)).collect();

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

    for cat_name in catalog_manager.visible_catalog_names() {
        if let Some(catalog) = catalog_manager.catalog(&cat_name) {
            for schema_name in catalog_manager.visible_schema_names(&cat_name).await {
                if catalog.schema(&schema_name).await.is_some() {
                    for table_name in catalog_manager
                        .visible_table_names(&cat_name, &schema_name)
                        .await
                    {
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

    for cat_name in catalog_manager.visible_catalog_names() {
        if let Some(catalog) = catalog_manager.catalog(&cat_name) {
            for schema_name in catalog_manager.visible_schema_names(&cat_name).await {
                if let Some(schema) = catalog.schema(&schema_name).await {
                    for table_name in catalog_manager
                        .visible_table_names(&cat_name, &schema_name)
                        .await
                    {
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

    for cat_name in catalog_manager.visible_catalog_names() {
        for schema_name in catalog_manager.visible_schema_names(&cat_name).await {
            if !catalog_manager
                .visible_table_names(&cat_name, &schema_name)
                .await
                .is_empty()
            {
                catalogs.push(cat_name.clone());
                schemas.push(schema_name);
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

fn handle_current_info(
    lower: &str,
    catalog_manager: &CatalogManager,
    user: Option<&str>,
) -> MetaResult {
    // current_schema() reflects the connection's search_path; current_user
    // is the connection's user; current_database() is the default catalog.
    let (name, value) = if lower.contains("current_schema()") {
        ("current_schema", catalog_manager.default_schema())
    } else if lower.contains("current_database()") {
        ("current_database", catalog_manager.default_catalog())
    } else {
        ("current_user", user.unwrap_or(""))
    };
    let schema = Arc::new(Schema::new(vec![Field::new(
        name,
        ArrowDataType::Utf8,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec![value.to_string()]))],
    )
    .map_err(|e| e.to_string())?;
    make_result(schema, batch)
}

// ---------------------------------------------------------------------------
// SET / SHOW / RESET — session commands
// ---------------------------------------------------------------------------

fn command_ok(tag: &str) -> MetaResult {
    Ok(MetadataResponse::Command(tag.to_string()))
}

/// The command tag for a transaction-control statement, or `None`.
fn transaction_tag(lower: &str) -> Option<&'static str> {
    let mut words = lower.trim_end_matches(';').split_whitespace();
    match (words.next()?, words.next()) {
        ("begin", _) => Some("BEGIN"),
        ("start", Some("transaction")) => Some("START TRANSACTION"),
        ("commit" | "end", _) => Some("COMMIT"),
        ("rollback" | "abort", _) => Some("ROLLBACK"),
        ("savepoint", Some(_)) => Some("SAVEPOINT"),
        ("release", Some(_)) => Some("RELEASE"),
        _ => None,
    }
}

/// PostgreSQL's value for a setting clients commonly read with `SHOW`.
fn show_value(name: &str) -> &'static str {
    match name {
        "server_version" => PG_SERVER_VERSION,
        "server_version_num" => "140000",
        "standard_conforming_strings" | "integer_datetimes" => "on",
        "transaction isolation level" | "default_transaction_isolation" => "read committed",
        "transaction_read_only" | "default_transaction_read_only" | "is_superuser" => "off",
        "client_encoding" | "server_encoding" => "UTF8",
        "datestyle" => "ISO, YMD",
        "intervalstyle" => "postgres",
        "timezone" => "Etc/UTC",
        "max_identifier_length" => "63",
        "lc_collate" | "lc_ctype" => "C",
        _ => "",
    }
}

fn handle_show(lower: &str) -> MetaResult {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "setting",
        ArrowDataType::Utf8,
        false,
    )]));
    // `SHOW search_path` is answered per connection in `crate::session`.
    let name = lower
        .trim_start_matches("show")
        .trim()
        .trim_end_matches(';')
        .trim();
    let value = show_value(name);
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec![value]))],
    )
    .map_err(|e| e.to_string())?;
    make_result(schema, batch)
}
