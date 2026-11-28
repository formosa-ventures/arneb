//! PostgreSQL system catalog tables for pgwire clients (pg-catalog-tables).
//!
//! A statement that reads one of [`TABLES`] or [`INFO_SCHEMA_TABLES`] runs
//! in the engine over a
//! per-statement memory catalog: one catalog named after the session's
//! catalog (the PostgreSQL database), holding a `pg_catalog` schema and an
//! `information_schema` schema ([`INFO_SCHEMA_TABLES`]). The `pg_catalog`
//! tables also sit under the session's default schema, so unqualified
//! `FROM pg_class` resolves while `'nation'::regclass` still means the
//! session's table. Rows come from the session's access view, so objects
//! the user may not see are absent. Oids come from
//! [`arneb_common::pg_oid`], which `regclass` uses too.

use std::sync::Arc;

use arneb_catalog::{CatalogManager, SessionPrivileges, TablePrivilege};
use arneb_common::error::ArnebError;
use arneb_common::pg_oid::{self, PG_CATALOG, PG_TYPES};
use arneb_common::types::{ColumnInfo, DataType};
use arneb_connectors::memory::{MemoryCatalog, MemoryConnectorFactory, MemorySchema};
use arneb_connectors::ConnectorRegistry;
use arneb_execution::memory_pool::MemoryPool;
use arrow::datatypes::Field;

use crate::encoding::datatype_to_pg_type;
use crate::memory_table::{s, Cell, TableBuilder, NULL};
use crate::metadata::MetadataResponse;

/// The `pg_catalog` tables provided, with their columns. A trailing `#`
/// marks a `BIGINT` column, `?` a `BOOLEAN` one; the rest are `VARCHAR`. A
/// `[]` before that marker makes it an array (`indkey[]#` is `BIGINT[]`,
/// `reloptions[]` is `VARCHAR[]`).
const TABLES: &[(&str, &[&str])] = &[
    ("pg_namespace", &["oid#", "nspname", "nspowner#", "nspacl"]),
    (
        "pg_class",
        &[
            "oid#",
            "relname",
            "relnamespace#",
            "reltype#",
            "relowner#",
            "relam#",
            "reltablespace#",
            "relkind",
            "relpersistence",
            "relnatts#",
            "relchecks#",
            "relhasindex?",
            "relhasrules?",
            "relhastriggers?",
            "relhassubclass?",
            "relrowsecurity?",
            "relispartition?",
            "reloptions[]",
        ],
    ),
    (
        "pg_attribute",
        &[
            "attrelid#",
            "attname",
            "atttypid#",
            "attnum#",
            "attlen#",
            "atttypmod#",
            "attndims#",
            "attnotnull?",
            "attisdropped?",
            "atthasdef?",
            "attidentity",
            "attgenerated",
            "attislocal?",
            "attinhcount#",
            "attcollation#",
        ],
    ),
    (
        "pg_type",
        &[
            "oid#",
            "typname",
            "typnamespace#",
            "typowner#",
            "typlen#",
            "typtype",
            "typcategory",
            "typdelim",
            "typrelid#",
            "typelem#",
            "typarray#",
            "typbasetype#",
            "typtypmod#",
            "typndims#",
            "typcollation#",
            "typnotnull?",
            "typisdefined?",
        ],
    ),
    (
        "pg_database",
        &[
            "oid#",
            "datname",
            "datdba#",
            "encoding#",
            "datcollate",
            "datctype",
            "datistemplate?",
            "datallowconn?",
            "datconnlimit#",
            "dattablespace#",
        ],
    ),
    (
        "pg_roles",
        &[
            "oid#",
            "rolname",
            "rolsuper?",
            "rolinherit?",
            "rolcreaterole?",
            "rolcreatedb?",
            "rolcanlogin?",
        ],
    ),
    ("pg_am", &["oid#", "amname", "amtype"]),
    ("pg_tablespace", &["oid#", "spcname", "spcowner#"]),
    (
        "pg_depend",
        &[
            "classid#",
            "objid#",
            "objsubid#",
            "refclassid#",
            "refobjid#",
            "refobjsubid#",
            "deptype",
        ],
    ),
    (
        "pg_description",
        &["objoid#", "classoid#", "objsubid#", "description"],
    ),
    ("pg_attrdef", &["oid#", "adrelid#", "adnum#", "adbin"]),
    (
        "pg_constraint",
        &[
            "oid#",
            "conname",
            "connamespace#",
            "contype",
            "conrelid#",
            "contypid#",
            "conindid#",
            "confrelid#",
            "convalidated?",
            "conkey[]#",
            "confkey[]#",
        ],
    ),
    (
        "pg_index",
        &[
            "indexrelid#",
            "indrelid#",
            "indisprimary?",
            "indisunique?",
            "indisvalid?",
            "indnkeyatts#",
            "indnullsnotdistinct?",
            "indpred",
            "indkey[]#",
            "indclass[]#",
            "indoption[]#",
        ],
    ),
    (
        "pg_sequence",
        &[
            "seqrelid#",
            "seqstart#",
            "seqincrement#",
            "seqmin#",
            "seqmax#",
            "seqcache#",
            "seqcycle?",
        ],
    ),
    (
        "pg_stat_user_tables",
        &["relid#", "schemaname", "relname", "n_live_tup#"],
    ),
    (
        "pg_enum",
        &["oid#", "enumtypid#", "enumsortorder#", "enumlabel"],
    ),
    (
        "pg_tables",
        &[
            "schemaname",
            "tablename",
            "tableowner",
            "tablespace",
            "hasindexes?",
            "hasrules?",
            "hastriggers?",
            "rowsecurity?",
        ],
    ),
    (
        "pg_views",
        &["schemaname", "viewname", "viewowner", "definition"],
    ),
    (
        "pg_matviews",
        &[
            "schemaname",
            "matviewname",
            "matviewowner",
            "tablespace",
            "hasindexes?",
            "ispopulated?",
            "definition",
        ],
    ),
    ("pg_collation", &["oid#", "collname", "collnamespace#"]),
    ("pg_opclass", &["oid#", "opcname", "opcdefault?"]),
    (
        "pg_inherits",
        &["inhrelid#", "inhparent#", "inhseqno#", "inhdetachpending?"],
    ),
];

/// The `information_schema` tables provided, in the notation of [`TABLES`].
/// Their rows cover the user tables of the session's catalog (PostgreSQL's
/// current database), as its access view shows them.
const INFO_SCHEMA_TABLES: &[(&str, &[&str])] = &[
    ("schemata", &["catalog_name", "schema_name", "schema_owner"]),
    (
        "tables",
        &["table_catalog", "table_schema", "table_name", "table_type"],
    ),
    (
        "columns",
        &[
            "table_catalog",
            "table_schema",
            "table_name",
            "column_name",
            "ordinal_position#",
            "column_default",
            "is_nullable",
            "data_type",
            "character_maximum_length#",
            "numeric_precision#",
            "numeric_scale#",
            "datetime_precision#",
            "udt_catalog",
            "udt_schema",
            "udt_name",
            "is_identity",
            "is_generated",
        ],
    ),
    (
        "views",
        &[
            "table_catalog",
            "table_schema",
            "table_name",
            "view_definition",
        ],
    ),
    (
        "table_constraints",
        &[
            "constraint_catalog",
            "constraint_schema",
            "constraint_name",
            "table_catalog",
            "table_schema",
            "table_name",
            "constraint_type",
        ],
    ),
    (
        "key_column_usage",
        &[
            "constraint_catalog",
            "constraint_schema",
            "constraint_name",
            "table_catalog",
            "table_schema",
            "table_name",
            "column_name",
            "ordinal_position#",
        ],
    ),
];

const INFORMATION_SCHEMA: &str = "information_schema";

/// Whether `sql` reads one of the provided `pg_catalog` or
/// `information_schema` tables, so it runs over [`catalog_for`].
pub(crate) fn references_catalog_tables(sql: &str) -> bool {
    references_pg_catalog(sql) || references_information_schema(sql)
}

/// Whether `sql` names one of [`INFO_SCHEMA_TABLES`]: the word
/// `information_schema` followed by one of their names, in any quoting
/// (quotes and dots separate words, as in [`references_pg_catalog`]).
pub(crate) fn references_information_schema(sql: &str) -> bool {
    if !sql
        .as_bytes()
        .windows(INFORMATION_SCHEMA.len())
        .any(|w| w.eq_ignore_ascii_case(INFORMATION_SCHEMA.as_bytes()))
    {
        return false;
    }
    let lower = sql.to_ascii_lowercase();
    let mut words = lower
        .split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .filter(|w| !w.is_empty())
        .peekable();
    while let Some(word) = words.next() {
        if word == INFORMATION_SCHEMA
            && words
                .peek()
                .is_some_and(|next| INFO_SCHEMA_TABLES.iter().any(|(name, _)| name == next))
        {
            return true;
        }
    }
    false
}

/// Whether `sql` reads one of the provided `pg_catalog` tables.
pub(crate) fn references_pg_catalog(sql: &str) -> bool {
    // Every table name starts with `pg_`; most statements have none, so they
    // skip the lowercase copy and the word split.
    if !sql
        .as_bytes()
        .windows(3)
        .any(|w| w.eq_ignore_ascii_case(b"pg_"))
    {
        return false;
    }
    let lower = sql.to_ascii_lowercase();
    lower
        .split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .any(|word| TABLES.iter().any(|(name, _)| *name == word))
}

/// The catalog manager and connectors a `pg_catalog` statement runs
/// against, for `session` (its access view).
pub(crate) async fn catalog_for(
    sql: &str,
    session: &CatalogManager,
) -> (CatalogManager, ConnectorRegistry) {
    let lower = sql.to_ascii_lowercase();
    build(
        session,
        lower.contains("pg_attribute") || lower.contains("columns"),
    )
    .await
}

/// Run `sql` over the `pg_catalog` tables of `session` (its access view).
pub(crate) async fn run(
    sql: &str,
    session: &CatalogManager,
    memory_pool: &Arc<dyn MemoryPool>,
) -> Result<MetadataResponse, ArnebError> {
    let (catalog_manager, registry) = catalog_for(sql, session).await;
    let (plan, batches) = crate::handler::execute_query_with_limit(
        sql,
        &catalog_manager,
        &registry,
        None,
        memory_pool,
        None,
    )
    .await?;
    let fields = plan
        .schema()
        .into_iter()
        .map(|c| Arc::new(Field::new(c.name, c.data_type.into(), true)))
        .collect();
    Ok(MetadataResponse::Query(fields, batches))
}

fn i(v: i64) -> Cell {
    Cell::Int(Some(v))
}

fn b(v: bool) -> Cell {
    Cell::Bool(Some(v))
}

/// An empty table of `name`, typed by its column spec in [`TABLES`] or
/// [`INFO_SCHEMA_TABLES`] (their names do not overlap).
fn table(name: &str) -> TableBuilder {
    let spec = TABLES
        .iter()
        .chain(INFO_SCHEMA_TABLES)
        .find(|(n, _)| *n == name)
        .map(|(_, c)| *c)
        .expect("known catalog table");
    TableBuilder::new(spec)
}

/// A relation listed in `pg_class` / `pg_attribute`.
struct Relation {
    schema: String,
    name: String,
    columns: Vec<ColumnInfo>,
}

/// The type oid and typmod `pg_attribute` reports for a column type.
fn column_type(dt: &DataType) -> (i64, i64) {
    let oid = i64::from(datatype_to_pg_type(dt).oid());
    let typmod = match dt {
        DataType::Decimal128 { precision, scale } => pg_oid::numeric_typmod(*precision, *scale),
        _ => -1,
    };
    (oid, typmod)
}

fn type_len(oid: i64) -> i64 {
    pg_oid::pg_type(oid).map_or(-1, |t| t.len)
}

/// How `information_schema.columns` describes a column type: `data_type`,
/// `udt_name` (the `typname` of the type `pg_attribute` reports),
/// `character_maximum_length`, `numeric_precision`, `numeric_scale` and
/// `datetime_precision`, as PostgreSQL fills them.
fn info_schema_type(dt: &DataType) -> (String, String, Cell, Cell, Cell, Cell) {
    let ty = datatype_to_pg_type(dt);
    let udt_name = ty.name().to_string();
    let oid = i64::from(ty.oid());
    let data_type = match dt {
        DataType::List(_) => "ARRAY".to_string(),
        DataType::Char(_) => "character".to_string(),
        _ => pg_oid::pg_type(oid).map_or_else(|| udt_name.clone(), |t| t.sql_name.to_string()),
    };
    let none = || Cell::Int(None);
    let (length, precision, scale, datetime) = match dt {
        DataType::Char(n) => (i(i64::from(*n)), none(), none(), none()),
        DataType::Int8 | DataType::Int16 => (none(), i(16), i(0), none()),
        DataType::Int32 => (none(), i(32), i(0), none()),
        DataType::Int64 => (none(), i(64), i(0), none()),
        DataType::Float32 => (none(), i(24), none(), none()),
        DataType::Float64 => (none(), i(53), none(), none()),
        DataType::Decimal128 { precision, scale } => (
            none(),
            i(i64::from(*precision)),
            i(i64::from(*scale)),
            none(),
        ),
        DataType::Date32 => (none(), none(), none(), i(0)),
        DataType::Timestamp { .. } => (none(), none(), none(), i(6)),
        _ => (none(), none(), none(), none()),
    };
    (data_type, udt_name, length, precision, scale, datetime)
}

/// The catalog manager and connectors `sql` runs against. `with_columns`
/// loads every table's columns for `pg_attribute`.
async fn build(
    session: &CatalogManager,
    with_columns: bool,
) -> (CatalogManager, ConnectorRegistry) {
    let database = session.default_catalog().to_string();
    let mut schemas = session.visible_schema_names(&database).await;
    schemas.sort();

    let mut relations = Vec::new();
    if let Some(provider) = session.catalog(&database) {
        for schema in &schemas {
            let Some(schema_provider) = provider.schema(schema).await else {
                continue;
            };
            let mut tables = session.visible_table_names(&database, schema).await;
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
                relations.push(Relation {
                    schema: schema.clone(),
                    name: table,
                    columns,
                });
            }
        }
    }
    // The catalog's own tables, so clients can list and describe them.
    for (name, spec) in TABLES {
        relations.push(Relation {
            schema: PG_CATALOG.to_string(),
            name: name.to_string(),
            columns: TableBuilder::new(spec).column_infos(),
        });
    }

    let pg_schema = Arc::new(MemorySchema::new());
    let owner = pg_oid::OWNER_ROLE_OID;

    let mut namespace = table("pg_namespace");
    for name in [PG_CATALOG, "information_schema"].into_iter().chain(
        schemas
            .iter()
            .map(String::as_str)
            .filter(|s| *s != PG_CATALOG),
    ) {
        namespace.push(vec![
            i(pg_oid::namespace_oid(&database, name)),
            s(name),
            i(owner),
            NULL,
        ]);
    }

    let mut class = table("pg_class");
    let mut attribute = table("pg_attribute");
    for rel in &relations {
        let oid = pg_oid::relation_oid(&database, &rel.schema, &rel.name);
        class.push(vec![
            i(oid),
            s(&rel.name),
            i(pg_oid::namespace_oid(&database, &rel.schema)),
            i(0),
            i(owner),
            i(2),
            i(0),
            s("r"),
            s("p"),
            i(rel.columns.len() as i64),
            i(0),
            b(false),
            b(false),
            b(false),
            b(false),
            b(false),
            b(false),
            NULL,
        ]);
        for (n, col) in rel.columns.iter().enumerate() {
            let (type_oid, typmod) = column_type(&col.data_type);
            attribute.push(vec![
                i(oid),
                s(&col.name),
                i(type_oid),
                i(n as i64 + 1),
                i(type_len(type_oid)),
                i(typmod),
                i(0),
                b(!col.nullable),
                b(false),
                b(false),
                s(""),
                s(""),
                b(true),
                i(0),
                i(0),
            ]);
        }
    }

    // One row per user relation `pg_class` lists (the catalog's own tables
    // are system tables, not user tables); no statistics, so zero live rows.
    let mut stat_user_tables = table("pg_stat_user_tables");
    for rel in relations.iter().filter(|r| r.schema != PG_CATALOG) {
        stat_user_tables.push(vec![
            i(pg_oid::relation_oid(&database, &rel.schema, &rel.name)),
            s(&rel.schema),
            s(&rel.name),
            i(0),
        ]);
    }

    // Every relation is a table (`relkind` 'r'); Arneb has no views or
    // materialized views here.
    let mut tables = table("pg_tables");
    for rel in &relations {
        tables.push(vec![
            s(&rel.schema),
            s(&rel.name),
            s(pg_oid::OWNER_ROLE_NAME),
            NULL,
            b(false),
            b(false),
            b(false),
            b(false),
        ]);
    }

    let info_schema = information_schema(&database, &schemas, &relations);

    let mut types = table("pg_type");
    for ty in PG_TYPES {
        types.push(vec![
            i(ty.oid),
            s(ty.name),
            i(pg_oid::PG_CATALOG_NAMESPACE_OID),
            i(owner),
            i(ty.len),
            s("b"),
            s(ty.category),
            s(","),
            i(0),
            i(0),
            i(ty.array_oid),
            i(0),
            i(-1),
            i(0),
            i(0),
            b(false),
            b(true),
        ]);
    }

    let mut database_table = table("pg_database");
    for name in session.visible_catalog_names() {
        database_table.push(vec![
            i(pg_oid::database_oid(&name)),
            s(name),
            i(owner),
            i(6),
            s("C"),
            s("C"),
            b(false),
            b(true),
            i(-1),
            i(1663),
        ]);
    }

    let mut roles = table("pg_roles");
    roles.push(vec![
        i(owner),
        s(pg_oid::OWNER_ROLE_NAME),
        b(false),
        b(true),
        b(false),
        b(false),
        b(true),
    ]);

    let mut am = table("pg_am");
    am.push(vec![i(2), s("heap"), s("t")]);
    am.push(vec![i(403), s("btree"), s("i")]);

    let mut tablespace = table("pg_tablespace");
    tablespace.push(vec![i(1663), s("pg_default"), i(owner)]);
    tablespace.push(vec![i(1664), s("pg_global"), i(owner)]);

    for (name, table) in [
        ("pg_namespace", namespace),
        ("pg_class", class),
        ("pg_attribute", attribute),
        ("pg_type", types),
        ("pg_database", database_table),
        ("pg_roles", roles),
        ("pg_am", am),
        ("pg_tablespace", tablespace),
        ("pg_depend", table("pg_depend")),
        ("pg_description", table("pg_description")),
        ("pg_attrdef", table("pg_attrdef")),
        ("pg_constraint", table("pg_constraint")),
        ("pg_stat_user_tables", stat_user_tables),
        ("pg_enum", table("pg_enum")),
        ("pg_tables", tables),
        ("pg_views", table("pg_views")),
        ("pg_matviews", table("pg_matviews")),
        ("pg_index", table("pg_index")),
        ("pg_sequence", table("pg_sequence")),
        ("pg_collation", table("pg_collation")),
        ("pg_opclass", table("pg_opclass")),
        ("pg_inherits", table("pg_inherits")),
    ] {
        pg_schema.register_table(name, table.into_memory_table());
    }

    let default_schema = session.default_schema().to_string();
    let catalog = Arc::new(MemoryCatalog::new());
    catalog.register_schema(PG_CATALOG, Arc::clone(&pg_schema));
    catalog.register_schema(INFORMATION_SCHEMA, info_schema);
    if default_schema != PG_CATALOG {
        catalog.register_schema(&default_schema, pg_schema);
    }
    // `regclass` casts name the relations listed above, not the tables of
    // this per-statement catalog.
    let listed = relations
        .iter()
        .map(|r| (r.schema.clone(), r.name.clone()))
        .collect();
    let mut catalog_manager = CatalogManager::new(database.clone(), default_schema.clone())
        .with_regclass_relations(listed)
        .with_session_privileges(session_privileges(session, &database, &schemas, &relations));
    if let Some(user) = session.session_user() {
        catalog_manager = catalog_manager.with_session_user(user);
    }
    catalog_manager.register_catalog(&database, catalog.clone());
    let registry = ConnectorRegistry::new();
    registry.register(
        &database,
        Arc::new(MemoryConnectorFactory::new(catalog, &default_schema)),
    );
    (catalog_manager, registry)
}

/// What `has_*_privilege` reports for `session`: the schemas `pg_namespace`
/// lists are usable; the user relations carry the privileges the access
/// rules grant, the catalog's own tables `SELECT`.
fn session_privileges(
    session: &CatalogManager,
    database: &str,
    schemas: &[String],
    relations: &[Relation],
) -> SessionPrivileges {
    let mut usable = vec![PG_CATALOG.to_string(), INFORMATION_SCHEMA.to_string()];
    usable.extend(
        schemas
            .iter()
            .filter(|s| !usable.contains(s))
            .cloned()
            .collect::<Vec<_>>(),
    );
    let tables = relations
        .iter()
        .map(|rel| {
            let held = if rel.schema == PG_CATALOG {
                vec![TablePrivilege::Select]
            } else {
                session.table_privileges_held(database, &rel.schema, &rel.name)
            };
            (rel.schema.clone(), rel.name.clone(), held)
        })
        .collect();
    SessionPrivileges {
        schemas: usable,
        tables,
    }
}

/// The `information_schema` tables over the user relations of `database`
/// (`relations` without the catalog's own tables) and its `schemas`.
fn information_schema(
    database: &str,
    schemas: &[String],
    relations: &[Relation],
) -> Arc<MemorySchema> {
    let mut schemata = table("schemata");
    for name in [PG_CATALOG, INFORMATION_SCHEMA].into_iter().chain(
        schemas
            .iter()
            .map(String::as_str)
            .filter(|s| *s != PG_CATALOG && *s != INFORMATION_SCHEMA),
    ) {
        schemata.push(vec![s(database), s(name), s(pg_oid::OWNER_ROLE_NAME)]);
    }

    let mut tables = table("tables");
    let mut columns = table("columns");
    for rel in relations.iter().filter(|r| r.schema != PG_CATALOG) {
        tables.push(vec![
            s(database),
            s(&rel.schema),
            s(&rel.name),
            s("BASE TABLE"),
        ]);
        for (n, col) in rel.columns.iter().enumerate() {
            let (data_type, udt_name, length, precision, scale, datetime) =
                info_schema_type(&col.data_type);
            columns.push(vec![
                s(database),
                s(&rel.schema),
                s(&rel.name),
                s(&col.name),
                i(n as i64 + 1),
                NULL,
                s(if col.nullable { "YES" } else { "NO" }),
                s(data_type),
                length,
                precision,
                scale,
                datetime,
                s(database),
                s(PG_CATALOG),
                s(udt_name),
                s("NO"),
                s("NEVER"),
            ]);
        }
    }

    let schema = Arc::new(MemorySchema::new());
    for (name, table) in [
        ("schemata", schemata),
        ("tables", tables),
        ("columns", columns),
        ("views", table("views")),
        ("table_constraints", table("table_constraints")),
        ("key_column_usage", table("key_column_usage")),
    ] {
        schema.register_table(name, table.into_memory_table());
    }
    schema
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn references_pg_catalog_matches_table_names_only() {
        // The `pg_` pre-check in `references_pg_catalog` relies on this.
        assert!(TABLES.iter().all(|(name, _)| name.starts_with("pg_")));
        assert!(references_pg_catalog("SELECT relname FROM PG_CLASS"));
        assert!(references_pg_catalog("select * from pg_catalog.pg_type t"));
        assert!(!references_pg_catalog("SELECT * FROM lineitem"));
        assert!(!references_pg_catalog("SELECT pg_typeof(x) FROM t"));
    }

    #[test]
    fn references_information_schema_needs_a_table_name() {
        for sql in [
            "SELECT * FROM information_schema.columns",
            "SELECT * FROM \"information_schema\".\"columns\" AS \"c\"",
            "select * from INFORMATION_SCHEMA . TABLES",
            "SELECT * FROM file.information_schema.key_column_usage",
        ] {
            assert!(references_information_schema(sql), "{sql}");
            assert!(references_catalog_tables(sql), "{sql}");
        }
        for sql in [
            "SELECT information_schema._pg_expandarray(ARRAY[1])",
            "SELECT 'information_schema' AS s, columns FROM t",
            "SELECT * FROM information_schema.routines",
            "SELECT * FROM lineitem",
        ] {
            assert!(!references_information_schema(sql), "{sql}");
        }
        assert!(references_catalog_tables("SELECT * FROM pg_class"));
    }
}
