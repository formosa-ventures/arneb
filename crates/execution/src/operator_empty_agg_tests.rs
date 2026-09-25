//! Regression tests for aggregates over empty / all-NULL input (e.g. a scan
//! fully pruned by `WHERE o_orderkey > 99999999`). Non-COUNT outputs must be
//! NULLs of the declared type, not an untyped `NullArray`, or
//! `RecordBatch::try_new` rejects the batch ("expected Int64 but found Null").

use super::*;
use crate::datasource::InMemoryDataSource;
use crate::partitioning::Partitioning;
use arneb_common::stream::collect_stream;
use arneb_common::types::DataType;
use arrow::array::{new_null_array, Decimal128Array};
use arrow::datatypes::DataType as A;

fn col(index: usize, name: &str) -> PlanExpr {
    PlanExpr::Column {
        index,
        name: name.to_string(),
        span: None,
    }
}

fn func(name: &str, arg: Option<PlanExpr>, distinct: bool) -> PlanExpr {
    PlanExpr::Function {
        name: name.to_string(),
        args: arg.into_iter().collect(),
        distinct,
        span: None,
    }
}

fn ci(name: &str, data_type: DataType) -> ColumnInfo {
    ColumnInfo {
        name: name.to_string(),
        data_type,
        nullable: true,
    }
}

/// Scan over `batches`, optionally repartitioned.
fn input(
    schema: Vec<ColumnInfo>,
    batches: Vec<RecordBatch>,
    partitioning: Option<Partitioning>,
) -> Arc<dyn ExecutionPlan> {
    let scan: Arc<dyn ExecutionPlan> = Arc::new(ScanExec {
        source: Arc::new(InMemoryDataSource::new(schema, batches)),
        _table_name: "t".to_string(),
        scan_context: ScanContext::default(),
        dynamic_filters: Default::default(),
        dynamic_filters_consumed: Vec::new(),
        dynamic_filter_collector: None,
        dynamic_filtering_enabled: false,
        dynamic_filtering_wait_timeout: std::time::Duration::from_secs(10),
        scan_task_index: 0,
        scan_task_count: 1,
    });
    match partitioning {
        Some(p) => Arc::new(crate::RepartitionExec::new(scan, p)),
        None => scan,
    }
}

async fn run_agg(
    input: Arc<dyn ExecutionPlan>,
    group_by: Vec<PlanExpr>,
    aggr_exprs: Vec<PlanExpr>,
    output_schema: &[ColumnInfo],
) -> Result<Vec<RecordBatch>, String> {
    let plan = HashAggregateExec {
        input,
        group_by,
        aggr_exprs,
        output_schema: output_schema.to_vec(),
        output_order: None,
        estimated_groups: None,
        memory_pool: Arc::new(crate::memory_pool::UnboundedMemoryPool::new()),
    };
    let stream = plan.execute(0).await.map_err(|e| e.to_string())?;
    collect_stream(stream).await.map_err(|e| e.to_string())
}

/// Global COUNT / MIN / MAX (/ SUM) over zero rows emits one row: counts 0,
/// everything else a NULL of the declared type. `distinct` adds a
/// COUNT(DISTINCT), which routes to the legacy per-row path; otherwise the
/// batch-aware path runs. 3 partitions exercises the parallel merge.
#[tokio::test]
async fn global_aggregate_over_empty_input_emits_typed_nulls() {
    let dec = DataType::Decimal128 {
        precision: 15,
        scale: 2,
    };
    // (value type, declared SUM type if summable)
    let cases = [
        (DataType::Int64, Some(DataType::Int64)),
        (dec.clone(), Some(dec)),
        (DataType::Utf8, None),
        (DataType::Date32, None),
    ];
    for (dt, sum_dt) in cases {
        for distinct in [false, true] {
            for partitions in [None, Some(Partitioning::RoundRobinBatch(3))] {
                let v = || Some(col(0, "v"));
                let mut exprs = vec![
                    func("COUNT", None, false),
                    func("COUNT", v(), false),
                    func("MIN", v(), false),
                    func("MAX", v(), false),
                ];
                let mut schema = vec![
                    ci("count(*)", DataType::Int64),
                    ci("count(v)", DataType::Int64),
                    ci("min(v)", dt.clone()),
                    ci("max(v)", dt.clone()),
                ];
                if let Some(sum_dt) = &sum_dt {
                    exprs.push(func("SUM", v(), false));
                    schema.push(ci("sum(v)", sum_dt.clone()));
                }
                if distinct {
                    exprs.push(func("COUNT", v(), true));
                    schema.push(ci("count(DISTINCT v)", DataType::Int64));
                }
                let label = format!("{dt:?} distinct={distinct} partitions={partitions:?}");
                let scan = input(vec![ci("v", dt.clone())], vec![], partitions);
                let out = run_agg(scan, vec![], exprs, &schema)
                    .await
                    .unwrap_or_else(|e| panic!("{label}: {e}"));

                assert_eq!(
                    out.iter().map(|b| b.num_rows()).sum::<usize>(),
                    1,
                    "{label}"
                );
                let b = out.iter().find(|b| b.num_rows() == 1).unwrap();
                for (arr, c) in b.columns().iter().zip(&schema) {
                    let want: A = c.data_type.clone().into();
                    assert_eq!(arr.data_type(), &want, "{label}: {} type", c.name);
                    if c.name.starts_with("count") {
                        let n = arr.as_primitive::<datatypes::Int64Type>().value(0);
                        assert_eq!(n, 0, "{label}: {}", c.name);
                    } else {
                        assert!(arr.is_null(0), "{label}: {} must be NULL", c.name);
                    }
                }
            }
        }
    }
}

/// GROUP BY over an all-NULL Utf8 key with all-NULL Decimal values: one
/// group whose key and MIN / MAX / SUM are typed NULLs. Round-robin over 3
/// partitions leaves two partitions empty, which the parallel merge must
/// skip; the hash-partitioned path is covered too.
#[tokio::test]
async fn grouped_aggregate_with_empty_partitions_and_null_key_is_typed() {
    let dec = DataType::Decimal128 {
        precision: 15,
        scale: 2,
    };
    let in_schema = vec![ci("k", DataType::Utf8), ci("v", dec.clone())];
    let batch = RecordBatch::try_new(
        crate::datasource::column_info_to_arrow_schema(&in_schema),
        vec![
            new_null_array(&A::Utf8, 2),
            new_null_array(&A::Decimal128(15, 2), 2),
        ],
    )
    .unwrap();
    let schema = vec![
        ci("k", DataType::Utf8),
        ci("count(v)", DataType::Int64),
        ci("min(v)", dec.clone()),
        ci("max(v)", dec.clone()),
        ci("sum(v)", dec),
    ];
    let group_by = vec![col(0, "k")];
    let partitionings = [
        None,
        Some(Partitioning::RoundRobinBatch(3)),
        Some(Partitioning::Hash(group_by.clone(), 4)),
    ];
    for distinct in [false, true] {
        for partitioning in partitionings.clone() {
            let label = format!("distinct={distinct} partitioning={partitioning:?}");
            let v = || Some(col(1, "v"));
            let exprs = vec![
                func("COUNT", v(), distinct),
                func("MIN", v(), false),
                func("MAX", v(), false),
                func("SUM", v(), false),
            ];
            let scan = input(in_schema.clone(), vec![batch.clone()], partitioning);
            let out = run_agg(scan, group_by.clone(), exprs, &schema)
                .await
                .unwrap_or_else(|e| panic!("{label}: {e}"));

            assert_eq!(
                out.iter().map(|b| b.num_rows()).sum::<usize>(),
                1,
                "{label}"
            );
            let b = out.iter().find(|b| b.num_rows() == 1).unwrap();
            for (i, (arr, c)) in b.columns().iter().zip(&schema).enumerate() {
                let want: A = c.data_type.clone().into();
                assert_eq!(arr.data_type(), &want, "{label}: {} type", c.name);
                assert_eq!(arr.is_null(0), i != 1, "{label}: {}", c.name);
            }
            let count = b.column(1).as_primitive::<datatypes::Int64Type>().value(0);
            assert_eq!(count, 0, "{label}: count(v)");
        }
    }
}

#[test]
fn scalars_to_array_all_null_uses_declared_type() {
    let dec_t = A::Decimal128(15, 2);
    let arr = scalars_to_array(&[ScalarValue::Null, ScalarValue::Null], &dec_t).unwrap();
    assert_eq!(arr.data_type(), &dec_t);
    assert_eq!(arr.null_count(), 2);
    let arr = scalars_to_array(&[], &A::Utf8).unwrap();
    assert_eq!(arr.data_type(), &A::Utf8);
    assert_eq!(arr.len(), 0);
}

#[test]
fn scalars_to_array_handles_decimal_values() {
    let dec_t = A::Decimal128(15, 2);
    let v = |value| ScalarValue::Decimal128 {
        value,
        precision: 15,
        scale: 2,
    };
    let arr = scalars_to_array(&[ScalarValue::Null, v(100), v(250)], &dec_t).unwrap();
    let want: ArrayRef = Arc::new(
        Decimal128Array::from(vec![None, Some(100), Some(250)])
            .with_precision_and_scale(15, 2)
            .unwrap(),
    );
    assert_eq!(arr.as_ref(), want.as_ref());
}
