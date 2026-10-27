//! Parenthesized joins in FROM, e.g. `FROM (a JOIN b ON ...) JOIN c ON ...`
//! (TPC-DS q40, q49, q72, q75, q78, q93, q97 in Trino's form). Every join
//! inside the parentheses must take part in the query, whether the nested
//! join leads the FROM clause or sits on the right side of another join.

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

#[tokio::test]
async fn leading_nested_join_keeps_its_join() {
    assert_eq!(
        rows("SELECT count(*) FROM (a JOIN b ON a.x = b.x)").await,
        table(&[&["2"]])
    );
}

#[tokio::test]
async fn leading_nested_join_exposes_right_columns() {
    assert_eq!(
        rows("SELECT a.x, b.y FROM (a JOIN b ON a.x = b.x) ORDER BY a.x").await,
        table(&[&["1", "10"], &["2", "20"]])
    );
}

#[tokio::test]
async fn doubly_parenthesized_join_keeps_its_join() {
    assert_eq!(
        rows("SELECT count(*) FROM ((a JOIN b ON a.x = b.x))").await,
        table(&[&["2"]])
    );
}

#[tokio::test]
async fn leading_nested_left_join_then_comma_table() {
    // TPC-DS q93's shape: the WHERE references the nested join's right side.
    assert_eq!(
        rows("SELECT a.x, c.y FROM (a LEFT JOIN b ON a.x = b.x), c WHERE b.y = c.y").await,
        table(&[&["1", "10"]])
    );
}

#[tokio::test]
async fn leading_nested_left_join_keeps_unmatched_rows() {
    assert_eq!(
        rows("SELECT a.x FROM (a LEFT JOIN b ON a.x = b.x) WHERE b.y IS NULL").await,
        table(&[&["3"]])
    );
}

#[tokio::test]
async fn leading_nested_join_then_join() {
    assert_eq!(
        rows("SELECT a.x FROM (a JOIN b ON a.x = b.x) JOIN c ON b.y = c.y").await,
        table(&[&["1"]])
    );
}

#[tokio::test]
async fn nested_inner_join_on_right_side_of_inner_join() {
    assert_eq!(
        rows("SELECT count(*) FROM a JOIN (b JOIN c ON b.y = c.y) ON a.x = b.x").await,
        table(&[&["1"]])
    );
}

#[tokio::test]
async fn nested_inner_join_on_right_side_of_left_join_is_not_flattened() {
    // a LEFT JOIN (b JOIN c): rows 2 and 3 of `a` survive with NULLs. Flattening
    // to (a LEFT JOIN b) JOIN c would drop them.
    assert_eq!(
        rows(
            "SELECT a.x, b.y FROM a LEFT JOIN (b JOIN c ON b.y = c.y) ON a.x = b.x \
             ORDER BY a.x"
        )
        .await,
        table(&[&["1", "10"], &["2", "NULL"], &["3", "NULL"]])
    );
}
