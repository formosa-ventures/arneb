//! End-to-end result types of aggregate window functions: SUM/AVG/MIN/MAX
//! OVER return the same Trino types as their GROUP BY form (BIGINT for
//! integer SUM, DECIMAL for decimal SUM/AVG, the argument type for MIN/MAX),
//! through the same `execute_query` pipeline the pgwire handler uses.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Decimal128Array, Int32Array, Int64Array, StringArray};
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
/// | id | grp | v    | d    |
/// |----|-----|------|------|
/// | 1  | a   | 10   | 1.50 |
/// | 2  | a   | 20   | 2.00 |
/// | 3  | a   | 20   | 3.00 |
/// | 4  | b   | NULL | 4.00 |
/// | 5  | b   | 5    | 5.25 |
/// | 6  | c   | 7    | NULL |
fn setup() -> (Arc<CatalogManager>, Arc<ConnectorRegistry>) {
    let arrow_schema = Arc::new(Schema::new(vec![
        Field::new("id", ArrowDataType::Int32, false),
        Field::new("grp", ArrowDataType::Utf8, false),
        Field::new("v", ArrowDataType::Int64, true),
        Field::new("d", ArrowDataType::Decimal128(10, 2), true),
    ]));
    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6])),
            Arc::new(StringArray::from(vec!["a", "a", "a", "b", "b", "c"])),
            Arc::new(Int64Array::from(vec![
                Some(10),
                Some(20),
                Some(20),
                None,
                Some(5),
                Some(7),
            ])),
            Arc::new(
                Decimal128Array::from(vec![
                    Some(150),
                    Some(200),
                    Some(300),
                    Some(400),
                    Some(525),
                    None,
                ])
                .with_precision_and_scale(10, 2)
                .unwrap(),
            ),
        ],
    )
    .unwrap();

    let col = |name: &str, data_type: DataType, nullable: bool| ColumnInfo {
        name: name.to_string(),
        data_type,
        nullable,
    };
    let table = Arc::new(MemoryTable::new(
        vec![
            col("id", DataType::Int32, false),
            col("grp", DataType::Utf8, false),
            col("v", DataType::Int64, true),
            col(
                "d",
                DataType::Decimal128 {
                    precision: 10,
                    scale: 2,
                },
                true,
            ),
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

async fn try_query(sql: &str) -> Result<(Vec<ArrowDataType>, Vec<Vec<String>>), String> {
    let (cm, cr) = setup();
    let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::new());
    let (_plan, batches) = arneb_protocol::__private::execute_query(sql, &cm, &cr, None, &pool)
        .await
        .map_err(|e| e.to_string())?;
    let types = batches
        .first()
        .map(|b| {
            b.schema()
                .fields()
                .iter()
                .map(|f| f.data_type().clone())
                .collect()
        })
        .unwrap_or_default();
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
    Ok((types, rows))
}

/// Run `sql`, returning the result column types and every cell as text
/// (`NULL` for nulls).
async fn query(sql: &str) -> (Vec<ArrowDataType>, Vec<Vec<String>>) {
    try_query(sql)
        .await
        .unwrap_or_else(|e| panic!("query failed: {sql}\n{e}"))
}

fn rows(cells: &[&[&str]]) -> Vec<Vec<String>> {
    cells
        .iter()
        .map(|r| r.iter().map(|s| s.to_string()).collect())
        .collect()
}

#[tokio::test]
async fn aggregates_over_whole_partition() {
    let (types, got) = query(
        "SELECT id, sum(v) OVER (PARTITION BY grp), count(v) OVER (PARTITION BY grp), \
         count(*) OVER (), min(v) OVER (PARTITION BY grp), max(grp) OVER () \
         FROM t ORDER BY id",
    )
    .await;
    assert_eq!(
        types[1..],
        [
            ArrowDataType::Int64,
            ArrowDataType::Int64,
            ArrowDataType::Int64,
            ArrowDataType::Int64,
            ArrowDataType::Utf8,
        ]
    );
    assert_eq!(
        got,
        rows(&[
            &["1", "50", "3", "6", "10", "c"],
            &["2", "50", "3", "6", "10", "c"],
            &["3", "50", "3", "6", "10", "c"],
            &["4", "5", "1", "6", "5", "c"],
            &["5", "5", "1", "6", "5", "c"],
            &["6", "7", "1", "6", "7", "c"],
        ])
    );
}

#[tokio::test]
async fn decimal_aggregates_keep_trino_types() {
    let (types, got) = query(
        "SELECT id, sum(d) OVER (PARTITION BY grp), avg(d) OVER (PARTITION BY grp) \
         FROM t ORDER BY id",
    )
    .await;
    assert_eq!(
        types[1..],
        [
            ArrowDataType::Decimal128(38, 2),
            ArrowDataType::Decimal128(10, 2),
        ]
    );
    // avg rounds half up: 6.50 / 3 = 2.1666.. -> 2.17, 9.25 / 2 = 4.625 -> 4.63.
    assert_eq!(
        got,
        rows(&[
            &["1", "6.50", "2.17"],
            &["2", "6.50", "2.17"],
            &["3", "6.50", "2.17"],
            &["4", "9.25", "4.63"],
            &["5", "9.25", "4.63"],
            &["6", "NULL", "NULL"],
        ])
    );
}

#[tokio::test]
async fn running_decimal_sum_is_exact_and_includes_peers() {
    let (types, got) =
        query("SELECT id, sum(d) OVER (PARTITION BY grp ORDER BY v) FROM t ORDER BY id").await;
    assert_eq!(types[1], ArrowDataType::Decimal128(38, 2));
    // ids 2 and 3 tie on v = 20, so both see 1.50 + 2.00 + 3.00.
    assert_eq!(
        got,
        rows(&[
            &["1", "1.50"],
            &["2", "6.50"],
            &["3", "6.50"],
            &["4", "9.25"],
            &["5", "5.25"],
            &["6", "NULL"],
        ])
    );
}
