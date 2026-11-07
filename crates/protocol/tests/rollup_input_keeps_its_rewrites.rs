//! A GROUP BY ROLLUP evaluates its input once, so the input is planned as
//! one subtree under the ROLLUP node. Every rewrite the input gets under a
//! plain GROUP BY must still reach it there, including the ones inside a
//! scalar subquery. Measured on TPC-DS q14a at SF1: with the ROLLUP input
//! planned once, the `avg_sales` subquery's comma join (store_sales,
//! date_dim WHERE ss_sold_date_sk = d_date_sk) stayed a cross join and the
//! query hit the 4 GiB per-query cap (2,880,404 x 73,049 rows), while the
//! same query without the change ran in 10 s. Expected rows were worked out
//! by hand from the data in `setup`.

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

/// Per-query memory cap. Every query below fits easily when the subquery's
/// join is a hash join on its WHERE equality; a cross product of `t` and `u`
/// (about 2.25 M rows) does not.
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
    let registry = ConnectorRegistry::new();
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
async fn plain_group_by_over_having_subquery() {
    // Control: the same input under a plain GROUP BY. The subquery is
    // count(*) / 2 = 1501 / 2 = 750, so only the odd group (751 rows) passes.
    assert_eq!(
        rows(
            "SELECT g, sum(c) FROM ( \
               SELECT v % 2 g, count(*) c FROM t GROUP BY v % 2 \
               HAVING count(*) > (SELECT count(*) / 2 FROM t, u WHERE t.k = u.uk) \
             ) x GROUP BY g ORDER BY g"
        )
        .await,
        table(&[&["1", "751"]])
    );
}

#[tokio::test]
async fn rollup_over_having_subquery() {
    assert_eq!(
        rows(
            "SELECT g, sum(c) FROM ( \
               SELECT v % 2 g, count(*) c FROM t GROUP BY v % 2 \
               HAVING count(*) > (SELECT count(*) / 2 FROM t, u WHERE t.k = u.uk) \
             ) x GROUP BY ROLLUP(g) ORDER BY g NULLS LAST"
        )
        .await,
        table(&[&["1", "751"], &["NULL", "751"]])
    );
}

#[tokio::test]
async fn rollup_over_where_subquery() {
    // The subquery is 1501 / 300 = 5; v > 5 keeps v in 6..9, 600 rows,
    // 300 with an even k and 300 with an odd k.
    assert_eq!(
        rows(
            "SELECT k % 2 g, count(*) FROM t \
             WHERE v > (SELECT count(*) / 300 FROM t, u WHERE t.k = u.uk) \
             GROUP BY ROLLUP(k % 2) ORDER BY g NULLS LAST"
        )
        .await,
        table(&[&["0", "300"], &["1", "300"], &["NULL", "600"]])
    );
}
