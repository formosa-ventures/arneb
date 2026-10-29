//! A parenthesized join with an alias, `FROM (a JOIN b ON ...) t`, as Trino
//! accepts it. The alias names the joined relation: its columns are reached
//! through `t`, the inner table names are no longer in scope, and a name both
//! sides share is ambiguous through `t`. Expected rows and errors were taken
//! from Trino 484.
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int32Array};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;

use arneb_catalog::CatalogManager;
use arneb_common::types::{ColumnInfo, DataType};
use arneb_connectors::memory::{MemoryCatalog, MemoryConnectorFactory, MemorySchema, MemoryTable};
use arneb_connectors::ConnectorRegistry;
use arneb_execution::memory_pool::{MemoryPool, UnboundedMemoryPool};

fn int_table(cols: &[(&str, Vec<i32>)]) -> Arc<MemoryTable> {
    let fields: Vec<Field> = cols
        .iter()
        .map(|(n, _)| Field::new(*n, ArrowDataType::Int32, true))
        .collect();
    let arrays: Vec<ArrayRef> = cols
        .iter()
        .map(|(_, v)| Arc::new(Int32Array::from(v.clone())) as ArrayRef)
        .collect();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap();
    Arc::new(MemoryTable::new(
        cols.iter()
            .map(|(n, _)| ColumnInfo {
                name: n.to_string(),
                data_type: DataType::Int32,
                nullable: true,
            })
            .collect(),
        vec![batch],
    ))
}

/// Every column is nullable, as Hive columns are.
///
/// | a.x | | b.x | b.y | | c.y |
/// |-----| |-----|-----| |-----|
/// | 1   | | 1   | 10  | | 10  |
/// | 2   | | 2   | 20  | | 99  |
/// | 3   |
fn setup() -> (Arc<CatalogManager>, Arc<ConnectorRegistry>) {
    let schema = Arc::new(MemorySchema::new());
    schema.register_table("a", int_table(&[("x", vec![1, 2, 3])]));
    schema.register_table("b", int_table(&[("x", vec![1, 2]), ("y", vec![10, 20])]));
    schema.register_table("c", int_table(&[("y", vec![10, 99])]));
    let catalog = Arc::new(MemoryCatalog::new());
    catalog.register_schema("default", schema);
    let factory = MemoryConnectorFactory::new(catalog.clone(), "default");
    let catalog_manager = Arc::new(CatalogManager::new("memory", "default"));
    catalog_manager.register_catalog("memory", catalog);
    let mut registry = ConnectorRegistry::new();
    registry.register("memory", Arc::new(factory));
    (catalog_manager, Arc::new(registry))
}

/// Run `sql`; return every cell as text (`NULL` for nulls).
async fn rows(sql: &str) -> Vec<Vec<String>> {
    let (cm, cr) = setup();
    let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::new());
    let (_plan, batches) = arneb_protocol::__private::execute_query(sql, &cm, &cr, None, &pool)
        .await
        .unwrap_or_else(|e| panic!("query failed: {sql}\n{e}"));
    let mut out = Vec::new();
    for batch in &batches {
        for r in 0..batch.num_rows() {
            out.push(
                batch
                    .columns()
                    .iter()
                    .map(|c| {
                        if c.is_null(r) {
                            "NULL".to_string()
                        } else {
                            array_value_to_string(c, r).unwrap()
                        }
                    })
                    .collect(),
            );
        }
    }
    out
}

fn table(rows: &[&[&str]]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|r| r.iter().map(|s| s.to_string()).collect())
        .collect()
}

/// Plan and run `sql`, expecting an error; return its message.
async fn error(sql: &str) -> String {
    let (cm, cr) = setup();
    let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::new());
    match arneb_protocol::__private::execute_query(sql, &cm, &cr, None, &pool).await {
        Ok(_) => panic!("query should fail: {sql}"),
        Err(e) => e.to_string(),
    }
}

#[tokio::test]
async fn aliased_join_keeps_its_join() {
    assert_eq!(
        rows("SELECT count(*) FROM (a JOIN b ON a.x = b.x) t").await,
        table(&[&["2"]])
    );
}

#[tokio::test]
async fn aliased_join_columns_are_reached_through_the_alias() {
    assert_eq!(
        rows("SELECT t.y FROM (a JOIN b ON a.x = b.x) t ORDER BY t.y").await,
        table(&[&["10"], &["20"]])
    );
}

#[tokio::test]
async fn aliased_join_wildcard_returns_both_sides() {
    assert_eq!(
        rows("SELECT * FROM (a JOIN b ON a.x = b.x) t ORDER BY 1").await,
        table(&[&["1", "1", "10"], &["2", "2", "20"]])
    );
}

#[tokio::test]
async fn aliased_left_join_keeps_unmatched_rows() {
    assert_eq!(
        rows("SELECT y FROM (a LEFT JOIN b ON a.x = b.x) t ORDER BY 1 NULLS LAST").await,
        table(&[&["10"], &["20"], &["NULL"]])
    );
}

#[tokio::test]
async fn aliased_join_joined_to_another_table() {
    assert_eq!(
        rows(
            "SELECT t.y, c.y FROM (a JOIN b ON a.x = b.x) t \
             LEFT JOIN c ON t.y = c.y ORDER BY 1"
        )
        .await,
        table(&[&["10", "10"], &["20", "NULL"]])
    );
}

#[tokio::test]
async fn aliased_join_with_column_aliases() {
    assert_eq!(
        rows("SELECT u, v FROM (b JOIN c ON b.y = c.y) t(u, v, w)").await,
        table(&[&["1", "10"]])
    );
}

#[tokio::test]
async fn inner_table_names_are_out_of_scope_after_the_alias() {
    let message = error("SELECT b.y FROM (a JOIN b ON a.x = b.x) t").await;
    assert!(message.contains("b.y"), "{message}");
}

#[tokio::test]
async fn a_name_both_sides_share_is_ambiguous_through_the_alias() {
    let message = error("SELECT t.x FROM (a JOIN b ON a.x = b.x) t").await;
    assert!(message.contains("ambiguous"), "{message}");
}
