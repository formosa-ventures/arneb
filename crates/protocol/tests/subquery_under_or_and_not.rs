//! IN / EXISTS subqueries that are not a top-level AND conjunct: under OR
//! (TPC-DS q10 q35 q45) or wrapped as `NOT (EXISTS (...))` (q16 q69 q94).
//! Expected rows were taken from Trino 484.

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

fn int_table(columns: &[(&str, Vec<Option<i32>>, bool)]) -> MemoryTable {
    let fields: Vec<Field> = columns
        .iter()
        .map(|(n, _, nullable)| Field::new(*n, ArrowDataType::Int32, *nullable))
        .collect();
    let arrays: Vec<ArrayRef> = columns
        .iter()
        .map(|(_, v, _)| Arc::new(Int32Array::from(v.clone())) as ArrayRef)
        .collect();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap();
    MemoryTable::new(
        columns
            .iter()
            .map(|(n, _, nullable)| ColumnInfo {
                name: n.to_string(),
                data_type: DataType::Int32,
                nullable: *nullable,
            })
            .collect(),
        vec![batch],
    )
}

/// - `c(ck, z)`: (1,10) (2,20) (3,30) (4,NULL) (5,50)
/// - `w(wk)`: 1, 1, 2 (key 1 twice)
/// - `k(kk)`: 2, 3, 3 (key 3 twice; key 2 is also in `w`)
/// - `e(ek)`: no rows
/// - `s(o, wh)`: (1,1) (1,2) (2,1) (2,1) (3,1) (4,1) (4,2)
/// - `r(ro)`: 2, 4
fn setup() -> (Arc<CatalogManager>, Arc<ConnectorRegistry>) {
    let schema = Arc::new(MemorySchema::new());
    let tables: Vec<(&str, MemoryTable)> = vec![
        (
            "c",
            int_table(&[
                (
                    "ck",
                    vec![Some(1), Some(2), Some(3), Some(4), Some(5)],
                    false,
                ),
                (
                    "z",
                    vec![Some(10), Some(20), Some(30), None, Some(50)],
                    true,
                ),
            ]),
        ),
        (
            "w",
            int_table(&[("wk", vec![Some(1), Some(1), Some(2)], false)]),
        ),
        (
            "k",
            int_table(&[("kk", vec![Some(2), Some(3), Some(3)], false)]),
        ),
        ("e", int_table(&[("ek", vec![], false)])),
        (
            "s",
            int_table(&[
                (
                    "o",
                    vec![
                        Some(1),
                        Some(1),
                        Some(2),
                        Some(2),
                        Some(3),
                        Some(4),
                        Some(4),
                    ],
                    false,
                ),
                (
                    "wh",
                    vec![
                        Some(1),
                        Some(2),
                        Some(1),
                        Some(1),
                        Some(1),
                        Some(1),
                        Some(2),
                    ],
                    false,
                ),
            ]),
        ),
        ("r", int_table(&[("ro", vec![Some(2), Some(4)], false)])),
    ];
    for (name, table) in tables {
        schema.register_table(name, Arc::new(table));
    }
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

/// The first column of every row.
async fn keys(sql: &str) -> Vec<String> {
    rows(sql).await.into_iter().map(|r| r[0].clone()).collect()
}

#[tokio::test]
async fn exists_or_exists_as_in_tpcds_q10() {
    assert_eq!(
        keys(
            "SELECT ck FROM c WHERE EXISTS (SELECT * FROM w WHERE wk = ck) \
             OR EXISTS (SELECT * FROM k WHERE kk = ck) ORDER BY ck"
        )
        .await,
        ["1", "2", "3"]
    );
}

#[tokio::test]
async fn exists_or_exists_over_an_empty_table() {
    assert_eq!(
        keys(
            "SELECT ck FROM c WHERE EXISTS (SELECT * FROM w WHERE wk = ck) \
             OR EXISTS (SELECT * FROM e WHERE ek = ck) ORDER BY ck"
        )
        .await,
        ["1", "2"]
    );
}

#[tokio::test]
async fn exists_or_exists_next_to_another_conjunct() {
    assert_eq!(
        keys(
            "SELECT ck FROM c WHERE ck > 1 AND (EXISTS (SELECT * FROM w WHERE wk = ck) \
             OR EXISTS (SELECT * FROM k WHERE kk = ck)) ORDER BY ck"
        )
        .await,
        ["2", "3"]
    );
}

#[tokio::test]
async fn predicate_or_in_subquery_as_in_tpcds_q45() {
    assert_eq!(
        keys("SELECT ck FROM c WHERE z IN (10, 50) OR ck IN (SELECT kk FROM k) ORDER BY ck").await,
        ["1", "2", "3", "5"]
    );
}

#[tokio::test]
async fn negated_or_with_in_subquery_keeps_null_unknown() {
    assert_eq!(
        keys("SELECT ck FROM c WHERE NOT (z IN (10) OR ck IN (SELECT kk FROM k)) ORDER BY ck")
            .await,
        ["5"]
    );
}

#[tokio::test]
async fn parenthesized_not_exists_as_in_tpcds_q69() {
    assert_eq!(
        keys("SELECT ck FROM c WHERE NOT (EXISTS (SELECT * FROM w WHERE wk = ck)) ORDER BY ck")
            .await,
        ["3", "4", "5"]
    );
}

#[tokio::test]
async fn parenthesized_not_in_subquery() {
    assert_eq!(
        keys("SELECT ck FROM c WHERE NOT (ck IN (SELECT kk FROM k)) ORDER BY ck").await,
        ["1", "4", "5"]
    );
}

#[tokio::test]
async fn exists_and_parenthesized_not_exists_as_in_tpcds_q16() {
    assert_eq!(
        rows(
            "SELECT count(*), count(DISTINCT o) FROM s s1 \
             WHERE EXISTS (SELECT * FROM s s2 WHERE s1.o = s2.o AND s1.wh <> s2.wh) \
             AND NOT (EXISTS (SELECT * FROM r WHERE r.ro = s1.o))"
        )
        .await,
        vec![vec!["2".to_string(), "1".to_string()]]
    );
}
