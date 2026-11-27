//! TINYINT and SMALLINT take part in implicit coercion like INTEGER and
//! BIGINT do: they widen to a wider integer, a floating-point type or a
//! decimal (TINYINT as DECIMAL(3,0), SMALLINT as DECIMAL(5,0), as in
//! Trino). `CAST(x AS SMALLINT) > 0` used to fail with `42804` because the
//! coercion matrix had no rule for either type.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Decimal128Array, Float64Array, Int16Array, Int64Array, Int8Array,
};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;

use arneb_catalog::CatalogManager;
use arneb_common::types::{ColumnInfo, DataType};
use arneb_connectors::memory::{MemoryCatalog, MemoryConnectorFactory, MemorySchema, MemoryTable};
use arneb_connectors::ConnectorRegistry;
use arneb_execution::memory_pool::{MemoryPool, UnboundedMemoryPool};

/// | k | s (SMALLINT) | t (TINYINT) | i (BIGINT) | f (DOUBLE) | d (DECIMAL(5,2)) |
/// |---|--------------|-------------|------------|------------|------------------|
/// | 1 | 10           | 10          | 3          | 0.5        | 1.50             |
/// | 2 | -5           | 3           | 2          | 1.0        | 2.25             |
/// | 3 | NULL         | NULL        | NULL       | NULL       | NULL             |
fn setup() -> (Arc<CatalogManager>, Arc<ConnectorRegistry>) {
    let decimal = ArrowDataType::Decimal128(5, 2);
    let fields = vec![
        Field::new("k", ArrowDataType::Int64, false),
        Field::new("s", ArrowDataType::Int16, true),
        Field::new("t", ArrowDataType::Int8, true),
        Field::new("i", ArrowDataType::Int64, true),
        Field::new("f", ArrowDataType::Float64, true),
        Field::new("d", decimal, true),
    ];
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![1, 2, 3])),
        Arc::new(Int16Array::from(vec![Some(10), Some(-5), None])),
        Arc::new(Int8Array::from(vec![Some(10), Some(3), None])),
        Arc::new(Int64Array::from(vec![Some(3), Some(2), None])),
        Arc::new(Float64Array::from(vec![Some(0.5), Some(1.0), None])),
        Arc::new(
            Decimal128Array::from(vec![Some(150), Some(225), None])
                .with_precision_and_scale(5, 2)
                .unwrap(),
        ),
    ];
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap();
    let column = |name: &str, data_type: DataType| ColumnInfo {
        name: name.into(),
        data_type,
        nullable: name != "k",
    };
    let table = Arc::new(MemoryTable::new(
        vec![
            column("k", DataType::Int64),
            column("s", DataType::Int16),
            column("t", DataType::Int8),
            column("i", DataType::Int64),
            column("f", DataType::Float64),
            column(
                "d",
                DataType::Decimal128 {
                    precision: 5,
                    scale: 2,
                },
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
    let registry = ConnectorRegistry::new();
    registry.register("memory", Arc::new(factory));
    (catalog_manager, Arc::new(registry))
}

async fn run(sql: &str) -> Result<Vec<RecordBatch>, String> {
    let (cm, cr) = setup();
    let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::new());
    arneb_protocol::__private::execute_query(sql, &cm, &cr, None, &pool)
        .await
        .map(|(_plan, batches)| batches)
        .map_err(|e| e.to_string())
}

/// Run `sql`; return the first column of every row as text (`NULL` for nulls).
async fn column(sql: &str) -> Vec<String> {
    let batches = run(sql)
        .await
        .unwrap_or_else(|e| panic!("query failed: {sql}\n{e}"));
    let mut out = Vec::new();
    for b in &batches {
        let c = b.column(0);
        for r in 0..b.num_rows() {
            out.push(if c.is_null(r) {
                "NULL".to_string()
            } else {
                array_value_to_string(c, r).unwrap()
            });
        }
    }
    out
}

#[tokio::test]
async fn cast_to_smallint_compared_with_an_integer_literal() {
    assert_eq!(
        column("SELECT k FROM t WHERE CAST(i AS SMALLINT) > 2 ORDER BY k").await,
        vec!["1"]
    );
    assert_eq!(
        column("SELECT k FROM t WHERE CAST(i AS TINYINT) = 2 ORDER BY k").await,
        vec!["2"]
    );
}

#[tokio::test]
async fn small_integer_columns_compared_with_literals() {
    assert_eq!(
        column("SELECT k FROM t WHERE s > 0 ORDER BY k").await,
        vec!["1"]
    );
    assert_eq!(
        column("SELECT k FROM t WHERE t < 5 ORDER BY k").await,
        vec!["2"]
    );
    assert_eq!(
        column("SELECT k FROM t WHERE s IN (10, 3) ORDER BY k").await,
        vec!["1"]
    );
    assert_eq!(
        column("SELECT k FROM t WHERE s BETWEEN -10 AND 0 ORDER BY k").await,
        vec!["2"]
    );
}

#[tokio::test]
async fn small_integers_compared_with_each_other_and_with_bigint() {
    assert_eq!(
        column("SELECT t = s FROM t ORDER BY k").await,
        vec!["true", "false", "NULL"]
    );
    assert_eq!(
        column("SELECT k FROM t WHERE t = i ORDER BY k").await,
        Vec::<String>::new()
    );
}

#[tokio::test]
async fn same_type_small_integer_comparisons() {
    assert_eq!(
        column("SELECT k FROM t WHERE s = s ORDER BY k").await,
        vec!["1", "2"]
    );
    assert_eq!(
        column("SELECT k FROM t a WHERE t < (SELECT max(t) FROM t) ORDER BY k").await,
        vec!["2"]
    );
}

#[tokio::test]
async fn min_max_and_group_by_keep_the_small_integer_type() {
    assert_eq!(column("SELECT min(s) FROM t").await, vec!["-5"]);
    assert_eq!(column("SELECT max(t) FROM t").await, vec!["10"]);
    assert_eq!(
        column("SELECT max(s) FROM t GROUP BY k ORDER BY k").await,
        vec!["10", "-5", "NULL"]
    );
    assert_eq!(
        column("SELECT s FROM t GROUP BY s ORDER BY s").await,
        vec!["-5", "10", "NULL"]
    );
    assert_eq!(
        column("SELECT count(DISTINCT s) + sum(DISTINCT t) FROM t").await,
        vec!["15"]
    );
    assert_eq!(
        column("SELECT count(*) FROM t GROUP BY t ORDER BY t").await,
        vec!["1", "1", "1"]
    );
}

#[tokio::test]
async fn arithmetic_widens_small_integers() {
    // SMALLINT + an integer literal is INTEGER-wide: no SMALLINT overflow.
    assert_eq!(
        column("SELECT s + 32767 FROM t ORDER BY k").await,
        vec!["32777", "32762", "NULL"]
    );
    assert_eq!(
        column("SELECT t + s FROM t ORDER BY k").await,
        vec!["20", "-2", "NULL"]
    );
    assert_eq!(
        column("SELECT s + f FROM t ORDER BY k").await,
        vec!["10.5", "-4.0", "NULL"]
    );
}

#[tokio::test]
async fn small_integer_with_decimal() {
    assert_eq!(
        column("SELECT s * d FROM t ORDER BY k").await,
        vec!["15.00", "-11.25", "NULL"]
    );
    assert_eq!(
        column("SELECT k FROM t WHERE t > d ORDER BY k").await,
        vec!["1", "2"]
    );
}

#[tokio::test]
async fn join_on_small_integer_and_bigint_keys() {
    assert_eq!(
        column("SELECT a.k FROM t a JOIN t b ON a.t = b.i ORDER BY a.k").await,
        vec!["2"]
    );
}

#[tokio::test]
async fn case_arms_and_union_branches_unify() {
    assert_eq!(
        column("SELECT CASE WHEN k = 1 THEN s ELSE 1 END FROM t ORDER BY k").await,
        vec!["10", "1", "1"]
    );
    assert_eq!(
        column(
            "SELECT x FROM (SELECT s AS x FROM t WHERE s IS NOT NULL UNION ALL SELECT i FROM t \
             WHERE i IS NOT NULL) u ORDER BY x"
        )
        .await,
        vec!["-5", "2", "3", "10"]
    );
}
