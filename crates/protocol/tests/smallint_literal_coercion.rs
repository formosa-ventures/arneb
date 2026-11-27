//! SMALLINT / TINYINT values compare with wider integers by widening, as in
//! PostgreSQL. psycopg 3 sends SQLAlchemy's reflection SQL with
//! `attnum > 0::SMALLINT` against a BIGINT column; that used to fail with
//! `type mismatch: expected Int64, found Int16`, and a literal cast to
//! SMALLINT failed constant folding.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int32Array, Int64Array};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;

use arneb_catalog::CatalogManager;
use arneb_common::types::{ColumnInfo, DataType};
use arneb_connectors::memory::{MemoryCatalog, MemoryConnectorFactory, MemorySchema, MemoryTable};
use arneb_connectors::ConnectorRegistry;
use arneb_execution::memory_pool::{MemoryPool, UnboundedMemoryPool};

/// `t(k INTEGER, b BIGINT)` with rows (1, 10), (2, 20), (3, 30).
fn setup() -> (Arc<CatalogManager>, Arc<ConnectorRegistry>) {
    let k: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
    let b: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30]));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", ArrowDataType::Int32, false),
            Field::new("b", ArrowDataType::Int64, false),
        ])),
        vec![k, b],
    )
    .unwrap();
    let columns = [("k", DataType::Int32), ("b", DataType::Int64)]
        .into_iter()
        .map(|(name, data_type)| ColumnInfo {
            name: name.to_string(),
            data_type,
            nullable: false,
        })
        .collect();
    let schema = Arc::new(MemorySchema::new());
    schema.register_table("t", Arc::new(MemoryTable::new(columns, vec![batch])));
    let catalog = Arc::new(MemoryCatalog::new());
    catalog.register_schema("default", schema);
    let cm = CatalogManager::new("memory", "default");
    cm.register_catalog("memory", catalog.clone());
    let registry = ConnectorRegistry::new();
    registry.register(
        "memory",
        Arc::new(MemoryConnectorFactory::new(catalog, "default")),
    );
    (Arc::new(cm), Arc::new(registry))
}

async fn rows(sql: &str) -> (Vec<Vec<String>>, Vec<ArrowDataType>) {
    let (cm, cr) = setup();
    let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::new());
    let (_plan, batches) = arneb_protocol::__private::execute_query(sql, &cm, &cr, None, &pool)
        .await
        .unwrap_or_else(|e| panic!("query failed: {sql}\n{e}"));
    let mut out = Vec::new();
    let mut types = Vec::new();
    for b in batches.iter().filter(|b| b.num_rows() > 0) {
        types = b.columns().iter().map(|c| c.data_type().clone()).collect();
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
    (out, types)
}

fn column(rows: &[Vec<String>]) -> Vec<&str> {
    rows.iter().map(|r| r[0].as_str()).collect()
}

#[tokio::test]
async fn smallint_literal_compares_with_wider_columns() {
    for sql in [
        "SELECT b FROM t WHERE b > 10::SMALLINT ORDER BY b",
        "SELECT b FROM t WHERE k > CAST(1 AS SMALLINT) ORDER BY b",
        "SELECT b FROM t WHERE CAST(k AS SMALLINT) >= 2::BIGINT ORDER BY b",
        "SELECT b FROM t WHERE CAST(k AS TINYINT) IN (2, 3) ORDER BY b",
    ] {
        let (got, _) = rows(sql).await;
        assert_eq!(column(&got), ["20", "30"], "{sql}");
    }
}

#[tokio::test]
async fn smallint_literal_is_a_smallint() {
    let (got, types) = rows("SELECT CAST(5 AS SMALLINT), 0::SMALLINT + b FROM t WHERE k = 1").await;
    assert_eq!(got, [vec!["5".to_string(), "10".to_string()]]);
    assert_eq!(types, [ArrowDataType::Int16, ArrowDataType::Int64]);
}

#[tokio::test]
async fn smallint_literal_out_of_range_errors() {
    let (cm, cr) = setup();
    let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::new());
    let result = arneb_protocol::__private::execute_query(
        "SELECT CAST(70000 AS SMALLINT)",
        &cm,
        &cr,
        None,
        &pool,
    )
    .await;
    assert!(result.is_err(), "70000 does not fit a SMALLINT");
}
