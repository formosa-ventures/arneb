//! Regression tests for aggregates over empty / all-NULL input (e.g. a
//! worker whose scan was fully pruned by a selective filter such as
//! `WHERE o_orderkey > 99999999`).
//!
//! A global aggregate over zero rows must still emit exactly one row whose
//! non-COUNT columns are NULL *of the declared output type* — not an
//! untyped `NullArray` — or `RecordBatch::try_new` rejects the batch with
//! "expected Int64 but found Null" (on the worker's partial aggregate in
//! distributed mode, on the single node in standalone mode).

use super::*;
use crate::datasource::InMemoryDataSource;
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

fn arrow_type(dt: &DataType) -> A {
    dt.clone().into()
}

fn scan(schema: Vec<ColumnInfo>, batches: Vec<RecordBatch>) -> Arc<dyn ExecutionPlan> {
    Arc::new(ScanExec {
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
    })
}

fn agg(
    input: Arc<dyn ExecutionPlan>,
    group_by: Vec<PlanExpr>,
    aggr_exprs: Vec<PlanExpr>,
    output_schema: Vec<ColumnInfo>,
) -> HashAggregateExec {
    HashAggregateExec {
        input,
        group_by,
        aggr_exprs,
        output_schema,
        output_order: None,
        estimated_groups: None,
        memory_pool: Arc::new(crate::memory_pool::UnboundedMemoryPool::new()),
    }
}

async fn run(plan: &dyn ExecutionPlan) -> Result<Vec<RecordBatch>, String> {
    let stream = plan.execute(0).await.map_err(|e| e.to_string())?;
    collect_stream(stream).await.map_err(|e| e.to_string())
}

fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

/// Serialize + deserialize through Arrow IPC — the encoding the Flight
/// exchange uses between worker (partial) and coordinator (final).
fn ipc_roundtrip(
    schema: &arrow::datatypes::SchemaRef,
    batches: &[RecordBatch],
) -> Vec<RecordBatch> {
    let mut buf = Vec::new();
    {
        let mut w = arrow::ipc::writer::StreamWriter::try_new(&mut buf, schema).unwrap();
        for b in batches {
            w.write(b).unwrap();
        }
        w.finish().unwrap();
    }
    arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(buf), None)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

/// One column type under test.
struct Case {
    label: &'static str,
    dt: DataType,
    /// Non-empty 3-row input for the `v` column.
    values: ArrayRef,
    min: ArrayRef,
    max: ArrayRef,
    /// `(declared SUM output type, expected non-empty SUM)`; `None` for
    /// non-summable types. The inner `None` means "only check the empty
    /// (NULL) SUM" — see the Decimal case.
    sum: Option<(DataType, Option<ArrayRef>)>,
}

fn dec(v: Vec<i128>) -> ArrayRef {
    Arc::new(
        Decimal128Array::from(v)
            .with_precision_and_scale(15, 2)
            .unwrap(),
    )
}

fn cases() -> Vec<Case> {
    let dec_dt = DataType::Decimal128 {
        precision: 15,
        scale: 2,
    };
    vec![
        Case {
            label: "Int32",
            dt: DataType::Int32,
            values: Arc::new(Int32Array::from(vec![5, 1, 9])),
            min: Arc::new(Int32Array::from(vec![1])),
            max: Arc::new(Int32Array::from(vec![9])),
            sum: Some((DataType::Int64, Some(Arc::new(Int64Array::from(vec![15]))))),
        },
        Case {
            label: "Int64",
            dt: DataType::Int64,
            values: Arc::new(Int64Array::from(vec![5, 1, 9])),
            min: Arc::new(Int64Array::from(vec![1])),
            max: Arc::new(Int64Array::from(vec![9])),
            sum: Some((DataType::Int64, Some(Arc::new(Int64Array::from(vec![15]))))),
        },
        Case {
            label: "Float64",
            dt: DataType::Float64,
            values: Arc::new(Float64Array::from(vec![5.5, 1.5, 9.0])),
            min: Arc::new(Float64Array::from(vec![1.5])),
            max: Arc::new(Float64Array::from(vec![9.0])),
            sum: Some((
                DataType::Float64,
                Some(Arc::new(Float64Array::from(vec![16.0]))),
            )),
        },
        Case {
            label: "Decimal128(15,2)",
            dt: dec_dt.clone(),
            values: dec(vec![500, 100, 900]),
            min: dec(vec![100]),
            max: dec(vec![900]),
            // SUM(decimal) over non-empty input emits precision 38 while
            // the planner declares (15,2) — a separate, pre-existing type
            // mismatch unrelated to empty input. Only the empty (NULL)
            // SUM is checked for decimals.
            sum: Some((dec_dt, None)),
        },
        Case {
            label: "Date32",
            dt: DataType::Date32,
            values: Arc::new(Date32Array::from(vec![10_000, 9_000, 11_000])),
            min: Arc::new(Date32Array::from(vec![9_000])),
            max: Arc::new(Date32Array::from(vec![11_000])),
            sum: None,
        },
        Case {
            label: "Utf8",
            dt: DataType::Utf8,
            values: Arc::new(StringArray::from(vec!["m", "b", "x"])),
            min: Arc::new(StringArray::from(vec!["b"])),
            max: Arc::new(StringArray::from(vec!["x"])),
            sum: None,
        },
    ]
}

/// Input table: `g Int32, v <case type>`.
fn input_schema(c: &Case) -> Vec<ColumnInfo> {
    vec![ci("g", DataType::Int32), ci("v", c.dt.clone())]
}

fn input_batch(c: &Case) -> RecordBatch {
    let schema = crate::datasource::column_info_to_arrow_schema(&input_schema(c));
    let g: ArrayRef = Arc::new(Int32Array::from(vec![1, 1, 1]));
    RecordBatch::try_new(schema, vec![g, c.values.clone()]).unwrap()
}

/// Global aggregate list + declared output schema, mirroring the
/// planner's return-type rules (COUNT → Int64, MIN/MAX → argument type,
/// SUM → widened numeric).
fn global_aggs(c: &Case) -> (Vec<PlanExpr>, Vec<ColumnInfo>) {
    let v = || Some(col(1, "v"));
    let mut exprs = vec![
        func("COUNT", None, false),
        func("COUNT", v(), false),
        func("MIN", v(), false),
        func("MAX", v(), false),
    ];
    let mut schema = vec![
        ci("count(*)", DataType::Int64),
        ci("count(v)", DataType::Int64),
        ci("min(v)", c.dt.clone()),
        ci("max(v)", c.dt.clone()),
    ];
    if let Some((sum_dt, _)) = &c.sum {
        exprs.push(func("SUM", v(), false));
        schema.push(ci("sum(v)", sum_dt.clone()));
    }
    (exprs, schema)
}

/// Final-stage rewrite of `global_aggs` (what the fragmenter's
/// `build_final_aggr_expr` produces): COUNT → SUM over the partial count;
/// MIN / MAX / SUM keep their name; each reads its partial column.
fn final_aggs(partial_schema: &[ColumnInfo], n_aggr: usize) -> Vec<PlanExpr> {
    let names = ["SUM", "SUM", "MIN", "MAX", "SUM"];
    (0..n_aggr)
        .map(|i| func(names[i], Some(col(i, &partial_schema[i].name)), false))
        .collect()
}

fn assert_typed_empty_row(label: &str, batches: &[RecordBatch], schema: &[ColumnInfo]) {
    assert_eq!(
        total_rows(batches),
        1,
        "{label}: global aggregate over empty input must emit exactly 1 row"
    );
    let b = batches.iter().find(|b| b.num_rows() == 1).unwrap();
    for (i, c) in schema.iter().enumerate() {
        let arr = b.column(i);
        assert_eq!(
            arr.data_type(),
            &arrow_type(&c.data_type),
            "{label}: column {} type",
            c.name
        );
        if c.name.starts_with("count") {
            assert_eq!(
                arr.as_primitive::<datatypes::Int64Type>().value(0),
                0,
                "{label}: {} over empty input must be 0",
                c.name
            );
        } else {
            assert!(
                arr.is_null(0),
                "{label}: {} over empty input must be NULL",
                c.name
            );
        }
    }
}

/// Scenario A — single-stage global aggregate over an empty input, through
/// every `HashAggregateExec` path: batch-aware (no DISTINCT) and legacy
/// per-row (DISTINCT present), single- and multi-partition input.
#[tokio::test]
async fn global_aggregate_over_empty_input_emits_typed_nulls() {
    for c in cases() {
        let (mut exprs, mut schema) = global_aggs(&c);
        if matches!(c.dt, DataType::Float64) {
            exprs.push(func("AVG", Some(col(1, "v")), false));
            schema.push(ci("avg(v)", DataType::Float64));
        }
        for distinct in [false, true] {
            let (mut exprs, mut schema) = (exprs.clone(), schema.clone());
            if distinct {
                exprs.push(func("COUNT", Some(col(1, "v")), true));
                schema.push(ci("count(DISTINCT v)", DataType::Int64));
            }
            for partitions in [1usize, 3] {
                let mut input = scan(input_schema(&c), vec![]);
                if partitions > 1 {
                    input = Arc::new(crate::RepartitionExec::new(
                        input,
                        crate::partitioning::Partitioning::RoundRobinBatch(partitions),
                    ));
                }
                let label = format!("{} distinct={distinct} partitions={partitions}", c.label);
                let plan = agg(input, vec![], exprs.clone(), schema.clone());
                let out = run(&plan).await.unwrap_or_else(|e| panic!("{label}: {e}"));
                assert_typed_empty_row(&label, &out, &schema);
            }
        }
    }
}

/// Scenarios B + C — distributed partial → IPC → final. One worker's scan
/// is empty (fully pruned); the other has rows (B) or is also empty (C).
/// The empty worker's partial must be a typed NULL row that survives IPC
/// and merges without disturbing the other worker's result.
#[tokio::test]
async fn distributed_partial_final_with_empty_worker() {
    for c in cases() {
        let (exprs, partial_schema) = global_aggs(&c);
        let arrow_partial = crate::datasource::column_info_to_arrow_schema(&partial_schema);
        let final_exprs = final_aggs(&partial_schema, exprs.len());

        let empty_partial = run(&agg(
            scan(input_schema(&c), vec![]),
            vec![],
            exprs.clone(),
            partial_schema.clone(),
        ))
        .await
        .unwrap_or_else(|e| panic!("{}: empty-worker partial failed: {e}", c.label));
        assert_typed_empty_row(c.label, &empty_partial, &partial_schema);
        let empty_partial = ipc_roundtrip(&arrow_partial, &empty_partial);
        assert_typed_empty_row(c.label, &empty_partial, &partial_schema);

        // C: every worker empty → final still emits the typed empty row.
        let only_empty = run(&agg(
            scan(partial_schema.clone(), empty_partial.clone()),
            vec![],
            final_exprs.clone(),
            partial_schema.clone(),
        ))
        .await
        .unwrap_or_else(|e| panic!("{}: final over empty partials failed: {e}", c.label));
        assert_typed_empty_row(c.label, &only_empty, &partial_schema);

        // B: one empty worker + one non-empty worker.
        let expect_sum = c.sum.as_ref().map(|(_, s)| s.clone());
        if matches!(expect_sum, Some(None)) {
            continue; // pre-existing decimal SUM precision mismatch; see `cases`.
        }
        let full_partial = run(&agg(
            scan(input_schema(&c), vec![input_batch(&c)]),
            vec![],
            exprs.clone(),
            partial_schema.clone(),
        ))
        .await
        .unwrap_or_else(|e| panic!("{}: full partial: {e}", c.label));
        let mut both = empty_partial.clone();
        both.extend(ipc_roundtrip(&arrow_partial, &full_partial));
        // Both orders: empty-first and empty-last.
        for order in [both.clone(), both.into_iter().rev().collect()] {
            let merged = run(&agg(
                scan(partial_schema.clone(), order),
                vec![],
                final_exprs.clone(),
                partial_schema.clone(),
            ))
            .await
            .unwrap_or_else(|e| panic!("{}: final merge failed: {e}", c.label));
            assert_eq!(total_rows(&merged), 1, "{}", c.label);
            let b = &merged[0];
            let three: ArrayRef = Arc::new(Int64Array::from(vec![3]));
            assert_eq!(
                b.column(0).as_ref(),
                three.as_ref(),
                "{}: count(*)",
                c.label
            );
            assert_eq!(
                b.column(1).as_ref(),
                three.as_ref(),
                "{}: count(v)",
                c.label
            );
            assert_eq!(b.column(2).as_ref(), c.min.as_ref(), "{}: min", c.label);
            assert_eq!(b.column(3).as_ref(), c.max.as_ref(), "{}: max", c.label);
            if let Some(Some(sum)) = &expect_sum {
                assert_eq!(b.column(4).as_ref(), sum.as_ref(), "{}: sum", c.label);
            }
        }
    }
}

/// Scenario D — GROUP BY: an empty input yields zero groups (no row, no
/// error) on both the partial and the final side, and a group whose values
/// are all NULL yields typed NULL MIN / MAX / SUM columns.
#[tokio::test]
async fn grouped_aggregate_empty_and_all_null_groups_are_typed() {
    for c in cases() {
        let (aggs, aggr_schema) = global_aggs(&c);
        let mut base_schema = vec![ci("g", DataType::Int32)];
        base_schema.extend(aggr_schema);
        let group_by = vec![col(0, "g")];

        for distinct in [false, true] {
            let mut exprs = aggs.clone();
            let mut schema = base_schema.clone();
            if distinct {
                exprs.push(func("COUNT", Some(col(1, "v")), true));
                schema.push(ci("count(DISTINCT v)", DataType::Int64));
            }
            let label = format!("{} grouped distinct={distinct}", c.label);

            // Empty input → zero groups, partial and final.
            let partial = run(&agg(
                scan(input_schema(&c), vec![]),
                group_by.clone(),
                exprs.clone(),
                schema.clone(),
            ))
            .await
            .unwrap_or_else(|e| panic!("{label}: empty grouped: {e}"));
            assert_eq!(total_rows(&partial), 0, "{label}: empty grouped aggregate");
            if !distinct {
                let arrow_partial = crate::datasource::column_info_to_arrow_schema(&schema);
                let partial = ipc_roundtrip(&arrow_partial, &partial);
                let final_exprs: Vec<PlanExpr> = final_aggs(&schema[1..], exprs.len())
                    .into_iter()
                    .map(|e| match e {
                        PlanExpr::Function { name, args, .. } => {
                            let PlanExpr::Column { index, name: n, .. } = &args[0] else {
                                unreachable!()
                            };
                            func(&name, Some(col(index + 1, n)), false)
                        }
                        other => other,
                    })
                    .collect();
                let fin = run(&agg(
                    scan(schema.clone(), partial),
                    vec![col(0, "g")],
                    final_exprs,
                    schema.clone(),
                ))
                .await
                .unwrap_or_else(|e| panic!("{label}: final over empty grouped partial: {e}"));
                assert_eq!(total_rows(&fin), 0, "{label}: final over empty partial");
            }

            // A NULL group key plus a group 7, all values NULL.
            let arrow_in = crate::datasource::column_info_to_arrow_schema(&input_schema(&c));
            let nulls = RecordBatch::try_new(
                arrow_in,
                vec![
                    Arc::new(Int32Array::from(vec![None, Some(7), Some(7)])),
                    new_null_array(&arrow_type(&c.dt), 3),
                ],
            )
            .unwrap();
            // Single partition; round-robin over 3 (two partitions empty →
            // parallel merge must skip them); hash on the group key (the
            // hash-partitioned path, also with empty partitions).
            let partitionings = [
                None,
                Some(crate::partitioning::Partitioning::RoundRobinBatch(3)),
                Some(crate::partitioning::Partitioning::Hash(group_by.clone(), 4)),
            ];
            for (partitions, partitioning) in partitionings.into_iter().enumerate() {
                let mut input = scan(input_schema(&c), vec![nulls.clone()]);
                if let Some(p) = partitioning {
                    input = Arc::new(crate::RepartitionExec::new(input, p));
                }
                let out = run(&agg(input, group_by.clone(), exprs.clone(), schema.clone()))
                    .await
                    .unwrap_or_else(|e| {
                        panic!("{label} partitions={partitions}: all-NULL grouped: {e}")
                    });
                assert_eq!(total_rows(&out), 2, "{label}: two groups (NULL, 7)");
                for b in &out {
                    for (i, c) in schema.iter().enumerate() {
                        assert_eq!(
                            b.column(i).data_type(),
                            &arrow_type(&c.data_type),
                            "{label}: {} type",
                            c.name
                        );
                        if i >= 3 && !c.name.starts_with("count") {
                            assert_eq!(
                                b.column(i).null_count(),
                                b.num_rows(),
                                "{label}: {}",
                                c.name
                            );
                        }
                    }
                }
            }
        }
    }
}

/// A GROUP BY whose key column is entirely NULL must still produce a key
/// column of the declared type — legacy per-row path (DISTINCT) and the
/// batch-aware path.
#[tokio::test]
async fn grouped_all_null_key_is_typed() {
    let in_schema = vec![ci("k", DataType::Utf8), ci("v", DataType::Int64)];
    let batch = RecordBatch::try_new(
        crate::datasource::column_info_to_arrow_schema(&in_schema),
        vec![
            new_null_array(&A::Utf8, 2),
            Arc::new(Int64Array::from(vec![1, 2])),
        ],
    )
    .unwrap();
    for distinct in [false, true] {
        let out_schema = vec![ci("k", DataType::Utf8), ci("cnt", DataType::Int64)];
        let out = run(&agg(
            scan(in_schema.clone(), vec![batch.clone()]),
            vec![col(0, "k")],
            vec![func("COUNT", Some(col(1, "v")), distinct)],
            out_schema,
        ))
        .await
        .unwrap_or_else(|e| panic!("distinct={distinct}: {e}"));
        assert_eq!(total_rows(&out), 1);
        assert_eq!(out[0].column(0).data_type(), &A::Utf8);
        assert!(out[0].column(0).is_null(0));
        assert_eq!(
            out[0]
                .column(1)
                .as_primitive::<datatypes::Int64Type>()
                .value(0),
            2
        );
    }
}

/// `StreamingHashAggregateExec` (unique-key fold path) with all-NULL
/// aggregate input must emit typed NULLs too.
#[tokio::test]
async fn streaming_hash_aggregate_all_null_values_typed() {
    let in_schema = vec![ci("k", DataType::Int64), ci("v", DataType::Int64)];
    let batch = RecordBatch::try_new(
        crate::datasource::column_info_to_arrow_schema(&in_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            new_null_array(&A::Int64, 2),
        ],
    )
    .unwrap();
    let out_schema = vec![ci("k", DataType::Int64), ci("min(v)", DataType::Int64)];
    let plan = StreamingHashAggregateExec {
        input: scan(in_schema, vec![batch]),
        group_by: vec![col(0, "k")],
        aggr_exprs: vec![func("MIN", Some(col(1, "v")), false)],
        output_schema: out_schema,
        unique_key_idx: 0,
        memory_pool: Arc::new(crate::memory_pool::UnboundedMemoryPool::new()),
    };
    let out = run(&plan).await.unwrap();
    assert_eq!(total_rows(&out), 2);
    for b in &out {
        assert_eq!(b.column(1).data_type(), &A::Int64);
        assert_eq!(b.column(1).null_count(), b.num_rows());
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
    let arr = scalars_to_array(&[v(100), ScalarValue::Null, v(250)], &dec_t).unwrap();
    let want: ArrayRef = Arc::new(
        Decimal128Array::from(vec![Some(100), None, Some(250)])
            .with_precision_and_scale(15, 2)
            .unwrap(),
    );
    assert_eq!(arr.as_ref(), want.as_ref());
}
