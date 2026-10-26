//! PostgreSQL system catalog tables for pgwire clients (pg-catalog-tables).
//!
//! A statement that reads one of [`TABLES`] runs in the engine over a
//! per-statement memory catalog: one catalog named after the session's
//! catalog (the PostgreSQL database), holding a `pg_catalog` schema. The
//! same tables also sit under the session's default schema, so unqualified
//! `FROM pg_class` resolves while `'nation'::regclass` still means the
//! session's table. Rows come from the session's access view, so objects
//! the user may not see are absent. Oids come from
//! [`arneb_common::pg_oid`], which `regclass` uses too.

use std::sync::Arc;

use arneb_catalog::CatalogManager;
use arneb_common::error::ArnebError;
use arneb_common::pg_oid::{self, PG_CATALOG, PG_TYPES};
use arneb_common::types::{ColumnInfo, DataType};
use arneb_connectors::memory::{MemoryCatalog, MemoryConnectorFactory, MemorySchema, MemoryTable};
use arneb_connectors::ConnectorRegistry;
use arneb_execution::memory_pool::MemoryPool;
use arrow::array::{ArrayRef, BooleanArray, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{Field, Schema};

use crate::encoding::datatype_to_pg_type;
use crate::metadata::MetadataResponse;

/// The `pg_catalog` tables provided, with their columns. A trailing `#`
/// marks a `BIGINT` column, `?` a `BOOLEAN` one; the rest are `VARCHAR`.
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
            "reloptions",
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
        ],
    ),
];

/// Whether `sql` reads one of the provided `pg_catalog` tables.
pub(crate) fn references_pg_catalog(sql: &str) -> bool {
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
    build(session, sql.to_ascii_lowercase().contains("pg_attribute")).await
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

/// A cell of a catalog row.
enum Cell {
    Str(Option<String>),
    Int(Option<i64>),
    Bool(Option<bool>),
}

fn s(v: impl Into<String>) -> Cell {
    Cell::Str(Some(v.into()))
}

fn i(v: i64) -> Cell {
    Cell::Int(Some(v))
}

fn b(v: bool) -> Cell {
    Cell::Bool(Some(v))
}

const NULL: Cell = Cell::Str(None);

#[derive(Clone, Copy)]
enum Kind {
    Str,
    Int,
    Bool,
}

/// Rows of one catalog table, typed by its column spec in [`TABLES`].
struct Table {
    columns: Vec<(&'static str, Kind)>,
    rows: Vec<Vec<Cell>>,
}

impl Table {
    fn new(name: &str) -> Self {
        let spec = TABLES
            .iter()
            .find(|(n, _)| *n == name)
            .map(|(_, c)| *c)
            .expect("known pg_catalog table");
        let columns = spec
            .iter()
            .map(|c| {
                if let Some(n) = c.strip_suffix('#') {
                    (n, Kind::Int)
                } else if let Some(n) = c.strip_suffix('?') {
                    (n, Kind::Bool)
                } else {
                    (*c, Kind::Str)
                }
            })
            .collect();
        Self {
            columns,
            rows: Vec::new(),
        }
    }

    fn push(&mut self, row: Vec<Cell>) {
        debug_assert_eq!(row.len(), self.columns.len());
        self.rows.push(row);
    }

    fn into_memory_table(self) -> Arc<MemoryTable> {
        let infos: Vec<ColumnInfo> = self
            .columns
            .iter()
            .map(|(name, kind)| ColumnInfo {
                name: name.to_string(),
                data_type: match kind {
                    Kind::Str => DataType::Utf8,
                    Kind::Int => DataType::Int64,
                    Kind::Bool => DataType::Boolean,
                },
                nullable: true,
            })
            .collect();
        let fields: Vec<Field> = infos
            .iter()
            .map(|c| Field::new(&c.name, c.data_type.clone().into(), true))
            .collect();
        let arrays: Vec<ArrayRef> = (0..self.columns.len())
            .map(|col| -> ArrayRef {
                let cells = self.rows.iter().map(move |r| &r[col]);
                match self.columns[col].1 {
                    Kind::Str => Arc::new(StringArray::from_iter(cells.map(|c| match c {
                        Cell::Str(v) => v.clone(),
                        _ => None,
                    }))),
                    Kind::Int => Arc::new(Int64Array::from_iter(cells.map(|c| match c {
                        Cell::Int(v) => *v,
                        _ => None,
                    }))),
                    Kind::Bool => Arc::new(BooleanArray::from_iter(cells.map(|c| match c {
                        Cell::Bool(v) => *v,
                        _ => None,
                    }))),
                }
            })
            .collect();
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
            .expect("pg_catalog batch columns match schema");
        Arc::new(MemoryTable::new(infos, vec![batch]))
    }
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
    PG_TYPES.iter().find(|t| t.oid == oid).map_or(-1, |t| t.len)
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
        let columns = Table::new(name)
            .columns
            .iter()
            .map(|(col, kind)| ColumnInfo {
                name: col.to_string(),
                data_type: match kind {
                    Kind::Str => DataType::Utf8,
                    Kind::Int => DataType::Int64,
                    Kind::Bool => DataType::Boolean,
                },
                nullable: true,
            })
            .collect();
        debug_assert!(!spec.is_empty());
        relations.push(Relation {
            schema: PG_CATALOG.to_string(),
            name: name.to_string(),
            columns,
        });
    }

    let pg_schema = Arc::new(MemorySchema::new());
    let owner = pg_oid::OWNER_ROLE_OID;

    let mut namespace = Table::new("pg_namespace");
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

    let mut class = Table::new("pg_class");
    let mut attribute = Table::new("pg_attribute");
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

    let mut types = Table::new("pg_type");
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

    let mut database_table = Table::new("pg_database");
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

    let mut roles = Table::new("pg_roles");
    roles.push(vec![
        i(owner),
        s(pg_oid::OWNER_ROLE_NAME),
        b(false),
        b(true),
        b(false),
        b(false),
        b(true),
    ]);

    let mut am = Table::new("pg_am");
    am.push(vec![i(2), s("heap"), s("t")]);
    am.push(vec![i(403), s("btree"), s("i")]);

    let mut tablespace = Table::new("pg_tablespace");
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
        ("pg_depend", Table::new("pg_depend")),
        ("pg_description", Table::new("pg_description")),
        ("pg_attrdef", Table::new("pg_attrdef")),
        ("pg_constraint", Table::new("pg_constraint")),
    ] {
        pg_schema.register_table(name, table.into_memory_table());
    }

    let default_schema = session.default_schema().to_string();
    let catalog = Arc::new(MemoryCatalog::new());
    catalog.register_schema(PG_CATALOG, Arc::clone(&pg_schema));
    if default_schema != PG_CATALOG {
        catalog.register_schema(&default_schema, pg_schema);
    }
    let catalog_manager = CatalogManager::new(database.clone(), default_schema.clone());
    catalog_manager.register_catalog(&database, catalog.clone());
    let mut registry = ConnectorRegistry::new();
    registry.register(
        &database,
        Arc::new(MemoryConnectorFactory::new(catalog, &default_schema)),
    );
    (catalog_manager, registry)
}
