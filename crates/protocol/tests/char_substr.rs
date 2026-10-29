//! `substr` of a CHAR(n) value works on the blank-padded value and returns
//! varchar, as in Trino 484 (`substr(i_category, 3)` on `char(50)` 'Books'
//! is 'oks' followed by 45 blanks). TPC-DS q85 groups and orders by
//! `substr(r_reason_desc, 1, 20)` over a `char(100)` column.
//!
//! The column holds trimmed values, as Parquet written by Trino does.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int32Array, StringArray};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;

use arneb_catalog::CatalogManager;
use arneb_common::types::{ColumnInfo, DataType};
use arneb_connectors::memory::{MemoryCatalog, MemoryConnectorFactory, MemorySchema, MemoryTable};
use arneb_connectors::ConnectorRegistry;
use arneb_execution::memory_pool::{MemoryPool, UnboundedMemoryPool};

/// Table `t`:
///
/// | id | cat (char(50)) |
/// |----|----------------|
/// | 1  | 'Books'        |
/// | 2  | 'Music'        |
/// | 3  | 'Books'        |
/// | 4  | NULL           |
fn setup() -> (Arc<CatalogManager>, Arc<ConnectorRegistry>) {
    let arrow_schema = Arc::new(Schema::new(vec![
        Field::new("id", ArrowDataType::Int32, false),
        Field::new("cat", ArrowDataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
            Arc::new(StringArray::from(vec![
                Some("Books"),
                Some("Music"),
                Some("Books"),
                None,
            ])),
        ],
    )
    .unwrap();
    let table = Arc::new(MemoryTable::new(
        vec![
            ColumnInfo {
                name: "id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            },
            ColumnInfo {
                name: "cat".to_string(),
                data_type: DataType::Char(50),
                nullable: true,
            },
        ],
        vec![batch],
    ));
    let schema = Arc::new(MemorySchema::new());
    schema.register_table("t", table);
    let catalog = Arc::new(MemoryCatalog::new());
    catalog.register_schema("default", schema);
    let factory = MemoryConnectorFactory::new(catalog.clone(), "default");
    let catalog_manager = Arc::new(CatalogManager::new("memory", "default"));
    catalog_manager.register_catalog("memory", catalog);
    let mut registry = ConnectorRegistry::new();
    registry.register("memory", Arc::new(factory));
    (catalog_manager, Arc::new(registry))
}

/// Run `sql`; return the planned column types and every cell as text
/// (`NULL` for nulls).
async fn run(sql: &str) -> (Vec<DataType>, Vec<Vec<String>>) {
    let (cm, cr) = setup();
    let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::new());
    let (plan, batches) = arneb_protocol::__private::execute_query(sql, &cm, &cr, None, &pool)
        .await
        .unwrap_or_else(|e| panic!("query failed: {sql}\n{e}"));
    let types = plan.schema().into_iter().map(|c| c.data_type).collect();
    let mut rows = Vec::new();
    for batch in &batches {
        for r in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|c: &ArrayRef| {
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
    (types, rows)
}

async fn rows(sql: &str) -> Vec<Vec<String>> {
    run(sql).await.1
}

async fn one(sql: &str) -> String {
    let rows = rows(sql).await;
    assert_eq!(rows.len(), 1, "{sql}: {rows:?}");
    rows[0][0].clone()
}

fn padded(value: &str, n: usize) -> String {
    format!("{value:<n$}")
}

#[tokio::test]
async fn substr_keeps_the_padding_inside_the_range() {
    assert_eq!(
        rows("SELECT substr(cat, 1, 8), length(substr(cat, 1, 8)) FROM t WHERE id = 1").await,
        vec![vec!["Books   ".to_string(), "8".to_string()]]
    );
}

#[tokio::test]
async fn substr_to_the_end_runs_to_the_declared_length() {
    assert_eq!(
        rows("SELECT substr(cat, 3), length(substr(cat, 3)) FROM t WHERE id = 1").await,
        vec![vec![padded("oks", 48), "48".to_string()]]
    );
}

#[tokio::test]
async fn substr_past_the_value_is_blanks_then_empty() {
    assert_eq!(
        one("SELECT substr(cat, 49) FROM t WHERE id = 2").await,
        "  "
    );
    assert_eq!(one("SELECT substr(cat, 60) FROM t WHERE id = 2").await, "");
}

#[tokio::test]
async fn substr_of_a_null_char_is_null() {
    assert_eq!(
        one("SELECT substr(cat, 1, 3) FROM t WHERE id = 4").await,
        "NULL"
    );
}

#[tokio::test]
async fn substr_of_char_is_varchar() {
    let (types, _) = run("SELECT substr(cat, 1, 8) FROM t WHERE id = 1").await;
    assert_eq!(types, vec![DataType::Utf8]);
}

#[tokio::test]
async fn group_and_order_by_a_substr_of_char() {
    assert_eq!(
        rows(
            "SELECT substr(cat, 1, 20), count(*) FROM t GROUP BY cat \
             ORDER BY substr(cat, 1, 20) NULLS LAST"
        )
        .await,
        vec![
            vec![padded("Books", 20), "2".to_string()],
            vec![padded("Music", 20), "1".to_string()],
            vec!["NULL".to_string(), "1".to_string()],
        ]
    );
}
