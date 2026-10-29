//! Comma joins whose WHERE also holds an EXISTS / NOT EXISTS / IN subquery
//! or a correlated scalar subquery (TPC-DS q30/q81 shape). The remaining
//! WHERE equalities must still become hash-join keys; a cross product of the
//! inputs exceeds the per-query memory cap. Expected rows were computed by
//! SQLite on the same data.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;

use arneb_catalog::CatalogManager;
use arneb_common::types::{ColumnInfo, DataType};
use arneb_connectors::memory::{MemoryCatalog, MemoryConnectorFactory, MemorySchema, MemoryTable};
use arneb_connectors::ConnectorRegistry;
use arneb_execution::memory_pool::{MemoryPool, QueryMemoryPool, UnboundedMemoryPool};

const N: i64 = 1500;

/// Per-query memory cap. Every query below fits easily when its joins are
/// hash joins on the WHERE equalities; a cross product of two 1500-row
/// tables does not.
const LIMIT: usize = 16 * 1024 * 1024;

fn memory_table(cols: [&str; 2], rows: Vec<(Option<i64>, i64)>) -> Arc<MemoryTable> {
    let fields = vec![
        Field::new(cols[0], ArrowDataType::Int64, true),
        Field::new(cols[1], ArrowDataType::Int64, false),
    ];
    let (keys, vals): (Vec<Option<i64>>, Vec<i64>) = rows.into_iter().unzip();
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(keys)),
        Arc::new(Int64Array::from(vals)),
    ];
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap();
    Arc::new(MemoryTable::new(
        vec![
            ColumnInfo {
                name: cols[0].into(),
                data_type: DataType::Int64,
                nullable: true,
            },
            ColumnInfo {
                name: cols[1].into(),
                data_type: DataType::Int64,
                nullable: false,
            },
        ],
        vec![batch],
    ))
}

/// - `t(k, v)`: k = 0..1499, v = k % 10, plus a NULL-key row with v = 1.
/// - `u(uk, w)`: uk = 0..1499, w = uk % 7, plus a duplicate key 11 and a
///   NULL key.
/// - `s(sk, sv)`: even sk = 0..1498, sv = sk % 5, plus a NULL key.
fn setup() -> (Arc<CatalogManager>, Arc<ConnectorRegistry>) {
    let mut t: Vec<(Option<i64>, i64)> = (0..N).map(|i| (Some(i), i % 10)).collect();
    t.push((None, 1));
    let mut u: Vec<(Option<i64>, i64)> = (0..N).map(|i| (Some(i), i % 7)).collect();
    u.push((Some(11), 4));
    u.push((None, 3));
    let mut s: Vec<(Option<i64>, i64)> = (0..N).step_by(2).map(|i| (Some(i), i % 5)).collect();
    s.push((None, 3));

    let schema = Arc::new(MemorySchema::new());
    schema.register_table("t", memory_table(["k", "v"], t));
    schema.register_table("u", memory_table(["uk", "w"], u));
    schema.register_table("s", memory_table(["sk", "sv"], s));
    let catalog = Arc::new(MemoryCatalog::new());
    catalog.register_schema("default", schema);
    let factory = MemoryConnectorFactory::new(catalog.clone(), "default");
    let catalog_manager = Arc::new(CatalogManager::new("memory", "default"));
    catalog_manager.register_catalog("memory", catalog);
    let mut registry = ConnectorRegistry::new();
    registry.register("memory", Arc::new(factory));
    (catalog_manager, Arc::new(registry))
}

/// Run `sql` under [`LIMIT`]; return every cell as text (`NULL` for nulls).
async fn rows(sql: &str) -> Vec<Vec<String>> {
    let (cm, cr) = setup();
    let pool: Arc<dyn MemoryPool> = Arc::new(QueryMemoryPool::new(
        Arc::new(UnboundedMemoryPool::new()),
        LIMIT,
    ));
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
async fn comma_join_without_subquery_is_a_hash_join() {
    // Control: same join, no subquery.
    assert_eq!(
        rows("SELECT count(*) FROM t, s, u WHERE t.k = u.uk AND s.sk = u.uk").await,
        table(&[&["750"]])
    );
}

#[tokio::test]
async fn comma_join_beside_exists() {
    assert_eq!(
        rows(
            "SELECT count(*) FROM t, s, u \
             WHERE EXISTS (SELECT 1 FROM s x WHERE x.sk = t.k AND x.sv = 3) \
             AND t.k = u.uk AND s.sk = u.uk"
        )
        .await,
        table(&[&["150"]])
    );
}

#[tokio::test]
async fn comma_join_beside_not_exists() {
    assert_eq!(
        rows(
            "SELECT count(*) FROM t, s, u \
             WHERE NOT EXISTS (SELECT 1 FROM s x WHERE x.sk = t.k AND x.sv = 3) \
             AND t.k = u.uk AND s.sk = u.uk"
        )
        .await,
        table(&[&["600"]])
    );
}

#[tokio::test]
async fn comma_join_beside_in_subquery() {
    assert_eq!(
        rows(
            "SELECT count(*) FROM t, s, u \
             WHERE t.k IN (SELECT x.sk FROM s x WHERE x.sv = 3) \
             AND t.k = u.uk AND s.sk = u.uk"
        )
        .await,
        table(&[&["150"]])
    );
}

#[tokio::test]
async fn comma_join_beside_correlated_scalar_subquery() {
    assert_eq!(
        rows(
            "SELECT count(*) FROM t, s, u \
             WHERE t.v >= (SELECT min(x.sv) FROM s x WHERE x.sk = t.k) \
             AND t.k = u.uk AND s.sk = u.uk"
        )
        .await,
        table(&[&["750"]])
    );
}
