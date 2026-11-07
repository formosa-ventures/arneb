//! ORDER BY a qualified column that the SELECT list does not output, e.g.
//! `SELECT t.v FROM a t ORDER BY t.x` (TPC-DS q39 orders by `inv1.cov` while
//! selecting only `CAST(inv1.cov AS ...)`). Trino sorts by the named
//! column; expected rows were taken from Trino 484.

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

/// A table of non-nullable Int32 columns.
fn int_table(cols: &[(&str, Vec<i32>)]) -> Arc<MemoryTable> {
    let fields: Vec<Field> = cols
        .iter()
        .map(|(n, _)| Field::new(*n, ArrowDataType::Int32, false))
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
                nullable: false,
            })
            .collect(),
        vec![batch],
    ))
}

/// | a.x | a.v |
/// |-----|-----|
/// | 1   | 300 |
/// | 2   | 100 |
/// | 3   | 200 |
fn setup() -> (Arc<CatalogManager>, Arc<ConnectorRegistry>) {
    let schema = Arc::new(MemorySchema::new());
    schema.register_table(
        "a",
        int_table(&[("x", vec![1, 2, 3]), ("v", vec![300, 100, 200])]),
    );
    let catalog = Arc::new(MemoryCatalog::new());
    catalog.register_schema("default", schema);
    let factory = MemoryConnectorFactory::new(catalog.clone(), "default");
    let catalog_manager = Arc::new(CatalogManager::new("memory", "default"));
    catalog_manager.register_catalog("memory", catalog);
    let registry = ConnectorRegistry::new();
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
async fn table_name_qualifier() {
    assert_eq!(
        rows("SELECT a.v FROM a ORDER BY a.x").await,
        table(&[&["300"], &["100"], &["200"]])
    );
}

#[tokio::test]
async fn table_alias_qualifier() {
    assert_eq!(
        rows("SELECT t.v FROM a t ORDER BY t.x DESC").await,
        table(&[&["200"], &["100"], &["300"]])
    );
}

#[tokio::test]
async fn qualifier_picks_the_side_of_a_self_join() {
    let sql = "SELECT a1.v, a2.v FROM a a1 JOIN a a2 ON a1.x = 4 - a2.x";
    assert_eq!(
        rows(&format!("{sql} ORDER BY a2.x")).await,
        table(&[&["200", "300"], &["100", "100"], &["300", "200"]])
    );
    assert_eq!(
        rows(&format!("{sql} ORDER BY a1.x")).await,
        table(&[&["300", "200"], &["100", "100"], &["200", "300"]])
    );
}

#[tokio::test]
async fn cte_self_join_selecting_an_expression_as_in_tpcds_q39() {
    assert_eq!(
        rows(
            "WITH c AS (SELECT x, v FROM a) \
             SELECT c1.v + 0 FROM c c1, c c2 WHERE c1.x = c2.x ORDER BY c1.x DESC"
        )
        .await,
        table(&[&["200"], &["100"], &["300"]])
    );
}

#[tokio::test]
async fn qualified_group_key_not_selected() {
    assert_eq!(
        rows("SELECT sum(v) FROM a GROUP BY a.x ORDER BY a.x DESC").await,
        table(&[&["200"], &["100"], &["300"]])
    );
}

#[tokio::test]
async fn unknown_qualifier_is_an_error() {
    let (cm, cr) = setup();
    let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::new());
    let res = arneb_protocol::__private::execute_query(
        "SELECT v FROM a ORDER BY zz.x",
        &cm,
        &cr,
        None,
        &pool,
    )
    .await;
    assert!(res.is_err(), "ORDER BY zz.x must not resolve");
}
