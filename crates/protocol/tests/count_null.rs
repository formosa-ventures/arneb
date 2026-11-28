//! `count(NULL)` counts no rows (#322), in every aggregate path: single
//! group, grouped, DISTINCT and window. Trino and PostgreSQL return 0.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int32Array};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;

use arneb_catalog::CatalogManager;
use arneb_common::types::{ColumnInfo, DataType};
use arneb_connectors::memory::{MemoryCatalog, MemoryConnectorFactory, MemorySchema, MemoryTable};
use arneb_connectors::ConnectorRegistry;
use arneb_execution::memory_pool::{MemoryPool, UnboundedMemoryPool};

/// t(g, x): (1, 10), (1, NULL), (2, NULL).
fn setup() -> (Arc<CatalogManager>, Arc<ConnectorRegistry>) {
    let g: ArrayRef = Arc::new(Int32Array::from(vec![1, 1, 2]));
    let x: ArrayRef = Arc::new(Int32Array::from(vec![Some(10), None, None]));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("g", g.data_type().clone(), true),
            Field::new("x", x.data_type().clone(), true),
        ])),
        vec![g, x],
    )
    .unwrap();
    let column = |name: &str| ColumnInfo {
        name: name.to_string(),
        data_type: DataType::Int32,
        nullable: true,
    };
    let schema = Arc::new(MemorySchema::new());
    schema.register_table(
        "t",
        Arc::new(MemoryTable::new(
            vec![column("g"), column("x")],
            vec![batch],
        )),
    );
    let catalog = Arc::new(MemoryCatalog::new());
    catalog.register_schema("default", schema);
    let catalog_manager = Arc::new(CatalogManager::new("memory", "default"));
    catalog_manager.register_catalog("memory", catalog.clone());
    let registry = ConnectorRegistry::new();
    registry.register(
        "memory",
        Arc::new(MemoryConnectorFactory::new(catalog, "default")),
    );
    (catalog_manager, Arc::new(registry))
}

async fn rows(sql: &str) -> Vec<Vec<String>> {
    let (cm, cr) = setup();
    let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::new());
    let (_plan, batches) = arneb_protocol::__private::execute_query(sql, &cm, &cr, None, &pool)
        .await
        .unwrap_or_else(|e| panic!("query failed: {sql}\n{e}"));
    let mut out = Vec::new();
    for b in batches.iter().filter(|b| b.num_rows() > 0) {
        for r in 0..b.num_rows() {
            out.push(
                b.columns()
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

fn row(cells: &[&str]) -> Vec<String> {
    cells.iter().map(|c| c.to_string()).collect()
}

#[tokio::test]
async fn count_null_without_group_by_is_zero() {
    assert_eq!(
        rows("SELECT count(NULL), count(*), count(x) FROM t").await,
        vec![row(&["0", "3", "1"])]
    );
}

#[tokio::test]
async fn count_null_per_group_is_zero() {
    assert_eq!(
        rows("SELECT g, count(NULL), count(x) FROM t GROUP BY g ORDER BY g").await,
        vec![row(&["1", "0", "1"]), row(&["2", "0", "0"])]
    );
}

#[tokio::test]
async fn count_distinct_null_is_zero() {
    assert_eq!(
        rows("SELECT count(DISTINCT NULL) FROM t").await,
        vec![row(&["0"])]
    );
}

#[tokio::test]
async fn count_null_over_a_window_is_zero() {
    assert_eq!(
        rows("SELECT g, count(NULL) OVER (PARTITION BY g) FROM t ORDER BY g").await,
        vec![row(&["1", "0"]), row(&["1", "0"]), row(&["2", "0"])]
    );
}
