//! End-to-end CHAR(n) semantics, matched against Trino 484 on the TPC-DS
//! `item` table (`i_category char(50)`): values are blank-padded to n on
//! output, comparisons ignore trailing blanks, LIKE and length() see the
//! padded value, and a cast to varchar or `||` drops the padding.
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
    let registry = ConnectorRegistry::new();
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
async fn char_values_are_padded_on_output() {
    let (types, rows) = run("SELECT cat, length(cat) FROM t WHERE id = 1").await;
    assert_eq!(rows, vec![vec![padded("Books", 50), "50".to_string()]]);
    assert_eq!(types[0], DataType::Char(50));
}

#[tokio::test]
async fn a_null_char_stays_null() {
    assert_eq!(one("SELECT cat FROM t WHERE id = 4").await, "NULL");
}

#[tokio::test]
async fn comparisons_ignore_trailing_blanks() {
    for sql in [
        "SELECT count(*) FROM t WHERE cat = 'Books'",
        "SELECT count(*) FROM t WHERE cat IN ('Books', 'Home')",
        "SELECT count(*) FROM t WHERE cat < 'C'",
        "SELECT count(*) FROM t WHERE cat = CAST('Books' AS char(10))",
    ] {
        assert_eq!(one(sql).await, "2", "{sql}");
    }
}

#[tokio::test]
async fn a_char_literal_comparison_keeps_its_pushdown_shape() {
    // `cat = 'Books'` must stay a plain column-vs-literal comparison (no
    // cast or padding wrapped around the column), so scan pushdown and
    // row-group pruning still see it.
    let plan = rows("EXPLAIN SELECT id FROM t WHERE cat = 'Books'")
        .await
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join("\n");
    assert!(plan.contains("cat = 'Books'"), "{plan}");
}

#[tokio::test]
async fn like_sees_the_padded_value() {
    for (pattern, expected) in [("Books", "0"), ("%s", "0"), ("Book%", "2")] {
        let sql = format!("SELECT count(*) FROM t WHERE cat LIKE '{pattern}'");
        assert_eq!(one(&sql).await, expected, "{sql}");
    }
}

#[tokio::test]
async fn cast_to_varchar_and_concat_drop_the_padding() {
    let (types, rows) = run(
        "SELECT CAST(cat AS varchar), length(CAST(cat AS varchar)), cat || 'x', substr(cat, 1, 3) \
         FROM t WHERE id = 2",
    )
    .await;
    assert_eq!(rows, vec![vec!["Music", "5", "Musicx", "Mus"]]);
    assert_eq!(
        types,
        vec![
            DataType::Utf8,
            DataType::Int64,
            DataType::Utf8,
            DataType::Utf8
        ]
    );
}

#[tokio::test]
async fn upper_and_max_stay_char() {
    let (types, rows) = run("SELECT upper(cat) FROM t WHERE id = 2").await;
    assert_eq!(rows, vec![vec![padded("MUSIC", 50)]]);
    assert_eq!(types, vec![DataType::Char(50)]);

    let (types, rows) = run("SELECT max(cat) FROM t").await;
    assert_eq!(rows, vec![vec![padded("Music", 50)]]);
    assert_eq!(types, vec![DataType::Char(50)]);
}

#[tokio::test]
async fn group_by_and_order_by_output_is_padded() {
    let rows = rows("SELECT cat, count(*) FROM t GROUP BY cat ORDER BY cat NULLS LAST").await;
    assert_eq!(
        rows,
        vec![
            vec![padded("Books", 50), "2".to_string()],
            vec![padded("Music", 50), "1".to_string()],
            vec!["NULL".to_string(), "1".to_string()],
        ]
    );
}

#[tokio::test]
async fn joins_on_char_keys_match_equal_values() {
    // Books 2 x 2 + Music 1 x 1; NULL joins nothing.
    assert_eq!(
        one("SELECT count(*) FROM t a JOIN t b ON a.cat = b.cat").await,
        "5"
    );
}

#[tokio::test]
async fn char_literal_casts() {
    let (types, rows) = run(
        "SELECT CAST('ab' AS char(5)), length(CAST('ab' AS char(5))), \
         CAST('ab' AS char(5)) = 'ab', CAST('ab' AS char(5)) || '|'",
    )
    .await;
    assert_eq!(rows, vec![vec!["ab   ", "5", "true", "ab|"]]);
    assert_eq!(types[0], DataType::Char(5));
}

#[tokio::test]
async fn char_type_survives_plan_serialization() {
    // Distributed fragments travel as JSON.
    let json = serde_json::to_string(&DataType::Char(50)).unwrap();
    assert_eq!(
        serde_json::from_str::<DataType>(&json).unwrap(),
        DataType::Char(50)
    );
}
