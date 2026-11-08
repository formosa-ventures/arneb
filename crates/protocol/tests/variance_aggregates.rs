//! Trino's variance family of aggregates: stddev_samp / stddev / stddev_pop
//! and var_samp / variance / var_pop. They return DOUBLE, skip NULL inputs,
//! and return NULL when there are too few non-NULL values (none for the
//! population forms, fewer than two for the sample forms). Expected values
//! were taken from Trino 484.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Array, Int32Array};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use arneb_catalog::CatalogManager;
use arneb_common::types::{ColumnInfo, DataType};
use arneb_connectors::memory::{MemoryCatalog, MemoryConnectorFactory, MemorySchema, MemoryTable};
use arneb_connectors::ConnectorRegistry;
use arneb_execution::memory_pool::{MemoryPool, UnboundedMemoryPool};

type Row = (i32, Option<i32>, Option<f64>);

fn batch(schema: &Arc<Schema>, rows: &[Row]) -> RecordBatch {
    let g: ArrayRef = Arc::new(Int32Array::from(
        rows.iter().map(|r| r.0).collect::<Vec<_>>(),
    ));
    let x: ArrayRef = Arc::new(Int32Array::from(
        rows.iter().map(|r| r.1).collect::<Vec<_>>(),
    ));
    let d: ArrayRef = Arc::new(Float64Array::from(
        rows.iter().map(|r| r.2).collect::<Vec<_>>(),
    ));
    RecordBatch::try_new(schema.clone(), vec![g, x, d]).unwrap()
}

/// Table `t(g, x, d)`, split over two batches so that a group spans both:
///
/// | g | x            | d          |
/// |---|--------------|------------|
/// | 1 | 1, 2, 3, 4   | 1.5, 2.5, 4.0, 8.0 |
/// | 2 | 7            | 7.0        |
/// | 3 | NULL, NULL   | NULL, NULL |
/// | 4 | 5, NULL, 5   | 5.0, NULL, 5.0 |
fn setup() -> (Arc<CatalogManager>, Arc<ConnectorRegistry>) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("g", ArrowDataType::Int32, false),
        Field::new("x", ArrowDataType::Int32, true),
        Field::new("d", ArrowDataType::Float64, true),
    ]));
    let b1 = batch(
        &schema,
        &[
            (1, Some(1), Some(1.5)),
            (1, Some(2), Some(2.5)),
            (2, Some(7), Some(7.0)),
            (3, None, None),
            (4, Some(5), Some(5.0)),
        ],
    );
    let b2 = batch(
        &schema,
        &[
            (1, Some(3), Some(4.0)),
            (1, Some(4), Some(8.0)),
            (3, None, None),
            (4, None, None),
            (4, Some(5), Some(5.0)),
        ],
    );
    let table = Arc::new(MemoryTable::new(
        vec![
            ColumnInfo {
                name: "g".into(),
                data_type: DataType::Int32,
                nullable: false,
            },
            ColumnInfo {
                name: "x".into(),
                data_type: DataType::Int32,
                nullable: true,
            },
            ColumnInfo {
                name: "d".into(),
                data_type: DataType::Float64,
                nullable: true,
            },
        ],
        vec![b1, b2],
    ));
    let mem_schema = Arc::new(MemorySchema::new());
    mem_schema.register_table("t", table);
    let catalog = Arc::new(MemoryCatalog::new());
    catalog.register_schema("default", mem_schema);
    let factory = MemoryConnectorFactory::new(catalog.clone(), "default");
    let catalog_manager = Arc::new(CatalogManager::new("memory", "default"));
    catalog_manager.register_catalog("memory", catalog);
    let registry = ConnectorRegistry::new();
    registry.register("memory", Arc::new(factory));
    (catalog_manager, Arc::new(registry))
}

async fn run(sql: &str) -> Vec<RecordBatch> {
    let (cm, cr) = setup();
    let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::new());
    let (_plan, batches) = arneb_protocol::__private::execute_query(sql, &cm, &cr, None, &pool)
        .await
        .unwrap_or_else(|e| panic!("query failed: {sql}\n{e}"));
    batches
}

/// Every value column after the first `skip` columns must be Float64; return
/// the rows of those columns (`None` for NULL).
async fn doubles(sql: &str, skip: usize) -> Vec<Vec<Option<f64>>> {
    let mut out = Vec::new();
    for b in run(sql).await {
        for (i, c) in b.columns().iter().enumerate().skip(skip) {
            assert_eq!(
                c.data_type(),
                &ArrowDataType::Float64,
                "column {i} of `{sql}` is not DOUBLE"
            );
        }
        for r in 0..b.num_rows() {
            out.push(
                b.columns()
                    .iter()
                    .skip(skip)
                    .map(|c| {
                        let c = c.as_any().downcast_ref::<Float64Array>().unwrap();
                        (!c.is_null(r)).then(|| c.value(r))
                    })
                    .collect(),
            );
        }
    }
    out
}

fn assert_close(sql: &str, got: Vec<Vec<Option<f64>>>, want: &[&[Option<f64>]]) {
    assert_eq!(got.len(), want.len(), "row count of `{sql}`: {got:?}");
    for (r, (g, w)) in got.iter().zip(want).enumerate() {
        assert_eq!(g.len(), w.len(), "column count of `{sql}`");
        for (c, (g, w)) in g.iter().zip(w.iter()).enumerate() {
            match (g, w) {
                (Some(g), Some(w)) => assert!(
                    (g - w).abs() <= 1e-9 * w.abs().max(1.0),
                    "row {r} col {c} of `{sql}`: got {g}, want {w}"
                ),
                _ => assert_eq!(g, w, "row {r} col {c} of `{sql}`"),
            }
        }
    }
}

#[tokio::test]
async fn grouped_variance_family_over_integers() {
    let sql = "SELECT g, stddev_samp(x), stddev_pop(x), var_samp(x), var_pop(x), \
               stddev(x), variance(x) FROM t GROUP BY g ORDER BY g";
    let n = None;
    assert_close(
        sql,
        doubles(sql, 1).await,
        &[
            &[
                Some(1.2909944487358056),
                Some(1.118033988749895),
                Some(1.6666666666666667),
                Some(1.25),
                Some(1.2909944487358056),
                Some(1.6666666666666667),
            ],
            // One value: the sample forms are NULL, the population forms 0.
            &[n, Some(0.0), n, Some(0.0), n, n],
            // Only NULLs: everything is NULL.
            &[n, n, n, n, n, n],
            // NULLs are skipped: two equal values.
            &[
                Some(0.0),
                Some(0.0),
                Some(0.0),
                Some(0.0),
                Some(0.0),
                Some(0.0),
            ],
        ],
    );
}

#[tokio::test]
async fn grouped_variance_over_doubles() {
    let sql = "SELECT g, stddev_samp(d), var_pop(d) FROM t GROUP BY g ORDER BY g";
    assert_close(
        sql,
        doubles(sql, 1).await,
        &[
            &[Some(2.8577380332470415), Some(6.125)],
            &[None, Some(0.0)],
            &[None, None],
            &[Some(0.0), Some(0.0)],
        ],
    );
}

#[tokio::test]
async fn variance_combined_with_other_aggregates() {
    // TPC-DS q17 shape: stddev divided by avg in the same aggregation.
    let sql = "SELECT g, stddev_samp(x) / avg(x) FROM t GROUP BY g ORDER BY g";
    assert_close(
        sql,
        doubles(sql, 1).await,
        &[&[Some(0.5163977794943222)], &[None], &[None], &[Some(0.0)]],
    );
}

#[tokio::test]
async fn ungrouped_variance() {
    let sql = "SELECT stddev_samp(x), var_pop(x), stddev(d) FROM t";
    // x over all groups: 1, 2, 3, 4, 7, 5, 5; d: 1.5, 2.5, 4, 8, 7, 5, 5.
    assert_close(
        sql,
        doubles(sql, 0).await,
        &[&[
            Some(2.0354009783964297),
            Some(3.5510204081632653),
            Some(2.306822180279306),
        ]],
    );
}

#[tokio::test]
async fn ungrouped_variance_over_no_rows_is_null() {
    let sql = "SELECT stddev_samp(x), var_pop(x), stddev_pop(d) FROM t WHERE g > 100";
    assert_close(sql, doubles(sql, 0).await, &[&[None, None, None]]);
}

#[tokio::test]
async fn variance_in_having() {
    let sql = "SELECT g FROM t GROUP BY g HAVING stddev_samp(x) > 1 ORDER BY g";
    let batches = run(sql).await;
    let gs: Vec<i32> = batches
        .iter()
        .flat_map(|b| {
            let c = b.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
            (0..b.num_rows()).map(|i| c.value(i)).collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(gs, vec![1]);
}
