//! Window function physical operator.

use std::fmt;
use std::sync::Arc;

use arneb_common::error::ExecutionError;
use arneb_common::stream::{collect_stream, stream_from_batches, SendableRecordBatchStream};
use arneb_common::types::{ColumnInfo, ScalarValue};
use arneb_planner::WindowFunctionDef;
use arrow::array::{Array, ArrayRef, Int64Array, RecordBatch, UInt32Array};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use async_trait::async_trait;

use crate::aggregate::create_accumulator;
use crate::expression;
use crate::fast_hash::FastHasher;
use crate::operator::{scalars_to_array, ExecutionPlan};

/// Window function operator.
///
/// Materializes all input, computes window functions per partition, and
/// appends result columns. The input must arrive sorted by the PARTITION BY
/// then ORDER BY keys (the SQL planner places a Sort below each Window);
/// partitions and peer groups are detected as runs of adjacent rows.
#[derive(Debug)]
pub(crate) struct WindowExec {
    child: Arc<dyn ExecutionPlan>,
    functions: Vec<WindowFunctionDef>,
}

impl WindowExec {
    pub(crate) fn new(child: Arc<dyn ExecutionPlan>, functions: Vec<WindowFunctionDef>) -> Self {
        Self { child, functions }
    }
}

impl fmt::Display for WindowExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "WindowExec")
    }
}

/// Concatenate all record batches into one.
fn concat_batches(
    schema: &Arc<Schema>,
    batches: &[RecordBatch],
) -> Result<RecordBatch, ExecutionError> {
    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(schema.clone()));
    }
    let batch = arrow::compute::concat_batches(schema, batches)?;
    Ok(batch)
}

/// Compute a window function over a single combined batch.
fn compute_window_function(
    func: &WindowFunctionDef,
    batch: &RecordBatch,
    output_type: &ArrowDataType,
) -> Result<ArrayRef, ExecutionError> {
    let num_rows = batch.num_rows();
    let name_upper = func.name.to_uppercase();

    // Evaluate partition keys
    let partition_keys: Vec<ArrayRef> = func
        .partition_by
        .iter()
        .map(|e| expression::evaluate(e, batch, None))
        .collect::<Result<Vec<_>, _>>()?;

    // Determine partition boundaries (rows with same partition key values)
    let partition_ids = compute_partition_ids(&partition_keys, num_rows);

    match name_upper.as_str() {
        "ROW_NUMBER" => {
            let mut results = vec![0i64; num_rows];
            let mut prev_partition = u64::MAX;
            let mut counter = 0i64;
            for row in 0..num_rows {
                if partition_ids[row] != prev_partition {
                    counter = 0;
                    prev_partition = partition_ids[row];
                }
                counter += 1;
                results[row] = counter;
            }
            Ok(Arc::new(Int64Array::from(results)))
        }
        "RANK" => {
            let order_vals: Vec<ArrayRef> = func
                .order_by
                .iter()
                .map(|s| expression::evaluate(&s.expr, batch, None))
                .collect::<Result<Vec<_>, _>>()?;

            let mut results = vec![0i64; num_rows];
            let mut prev_partition = u64::MAX;
            let mut rank = 0i64;
            let mut counter = 0i64;

            for row in 0..num_rows {
                if partition_ids[row] != prev_partition {
                    prev_partition = partition_ids[row];
                    rank = 1;
                    counter = 1;
                } else {
                    counter += 1;
                    let same = row > 0 && same_order_values(&order_vals, row, row - 1);
                    if !same {
                        rank = counter;
                    }
                }
                results[row] = rank;
            }
            Ok(Arc::new(Int64Array::from(results)))
        }
        "DENSE_RANK" => {
            let order_vals: Vec<ArrayRef> = func
                .order_by
                .iter()
                .map(|s| expression::evaluate(&s.expr, batch, None))
                .collect::<Result<Vec<_>, _>>()?;

            let mut results = vec![0i64; num_rows];
            let mut prev_partition = u64::MAX;
            let mut rank = 0i64;

            for row in 0..num_rows {
                if partition_ids[row] != prev_partition {
                    prev_partition = partition_ids[row];
                    rank = 1;
                } else {
                    let same = row > 0 && same_order_values(&order_vals, row, row - 1);
                    if !same {
                        rank += 1;
                    }
                }
                results[row] = rank;
            }
            Ok(Arc::new(Int64Array::from(results)))
        }
        "SUM" | "AVG" | "COUNT" | "MIN" | "MAX" => {
            // Evaluated with the GROUP BY accumulators, so values and result
            // types match the non-window aggregates (Trino types).
            // COUNT(*) has no argument: any non-null column counts rows.
            let is_count_star = func.args.is_empty();
            let values: ArrayRef = match func.args.first() {
                Some(arg) => expression::evaluate(arg, batch, None)?,
                None => Arc::new(Int64Array::from(vec![0i64; num_rows])),
            };
            let order_vals: Vec<ArrayRef> = func
                .order_by
                .iter()
                .map(|s| expression::evaluate(&s.expr, batch, None))
                .collect::<Result<Vec<_>, _>>()?;
            let has_order = !order_vals.is_empty();

            // One value per peer group (or per partition without ORDER BY),
            // expanded to every row with `take`.
            let mut group_values: Vec<ScalarValue> = Vec::new();
            let mut row_group: Vec<u32> = Vec::with_capacity(num_rows);
            let mut acc = create_accumulator(&name_upper, is_count_star, false)?;
            let mut start = 0;
            while start < num_rows {
                let end = (start + 1..num_rows)
                    .find(|&i| partition_ids[i] != partition_ids[start])
                    .unwrap_or(num_rows);
                acc.reset();
                // Without ORDER BY the whole partition is one peer group.
                // With it, the default frame is RANGE UNBOUNDED PRECEDING ..
                // CURRENT ROW: peers (equal ORDER BY values) share the
                // running value after their whole peer group.
                let mut peer = start;
                while peer < end {
                    let peer_end = if has_order {
                        (peer + 1..end)
                            .find(|&i| !same_order_values(&order_vals, i, peer))
                            .unwrap_or(end)
                    } else {
                        end
                    };
                    acc.update_batch(&values.slice(peer, peer_end - peer))?;
                    let group = u32::try_from(group_values.len()).map_err(|_| {
                        ExecutionError::InvalidOperation(
                            "window input has too many peer groups".to_string(),
                        )
                    })?;
                    group_values.push(acc.evaluate()?);
                    row_group.extend(std::iter::repeat_n(group, peer_end - peer));
                    peer = peer_end;
                }
                start = end;
            }
            let groups = scalars_to_array(&group_values, output_type)?;
            if groups.data_type() != output_type {
                return Err(ExecutionError::InvalidOperation(format!(
                    "window {} produced {} but its declared type is {output_type}",
                    func.name,
                    groups.data_type()
                )));
            }
            Ok(arrow::compute::take(
                &groups,
                &UInt32Array::from(row_group),
                None,
            )?)
        }
        _ => Err(ExecutionError::InvalidOperation(format!(
            "unsupported window function: {}",
            func.name
        ))),
    }
}

fn compute_partition_ids(keys: &[ArrayRef], num_rows: usize) -> Vec<u64> {
    use std::hash::{Hash, Hasher};

    (0..num_rows)
        .map(|row| {
            let mut hasher = FastHasher::default();
            for key in keys {
                // NULL is its own partition, distinct from an empty string.
                key.is_null(row).hash(&mut hasher);
                let s = arrow::util::display::array_value_to_string(key, row).unwrap_or_default();
                s.hash(&mut hasher);
            }
            hasher.finish()
        })
        .collect()
}

fn same_order_values(order_vals: &[ArrayRef], row_a: usize, row_b: usize) -> bool {
    for arr in order_vals {
        if arr.is_null(row_a) || arr.is_null(row_b) {
            if arr.is_null(row_a) != arr.is_null(row_b) {
                return false;
            }
            continue;
        }
        let a = arrow::util::display::array_value_to_string(arr, row_a).unwrap_or_default();
        let b = arrow::util::display::array_value_to_string(arr, row_b).unwrap_or_default();
        if a != b {
            return false;
        }
    }
    true
}

#[async_trait]
impl ExecutionPlan for WindowExec {
    fn schema(&self) -> Vec<ColumnInfo> {
        let mut schema = self.child.schema();
        for f in &self.functions {
            let data_type = f.output_type(&schema);
            schema.push(ColumnInfo {
                name: f.output_name.clone(),
                data_type,
                nullable: true,
            });
        }
        schema
    }

    async fn execute(
        &self,
        _partition: usize,
    ) -> Result<SendableRecordBatchStream, ExecutionError> {
        let stream = self.child.execute(0).await?;
        let batches = collect_stream(stream)
            .await
            .map_err(|e| ExecutionError::InvalidOperation(format!("window collect: {e}")))?;

        let child_schema = self.child.schema();
        let child_arrow_schema = Arc::new(Schema::new(
            child_schema
                .iter()
                .map(|c| Field::new(&c.name, c.data_type.clone().into(), c.nullable))
                .collect::<Vec<_>>(),
        ));

        let combined = concat_batches(&child_arrow_schema, &batches)?;

        if combined.num_rows() == 0 {
            let output_schema = Arc::new(Schema::new(
                self.schema()
                    .iter()
                    .map(|c| Field::new(&c.name, c.data_type.clone().into(), c.nullable))
                    .collect::<Vec<_>>(),
            ));
            return Ok(stream_from_batches(output_schema, vec![]));
        }

        // Compute each window function and append as new column
        let mut columns: Vec<ArrayRef> = (0..combined.num_columns())
            .map(|i| combined.column(i).clone())
            .collect();

        let output_columns = self.schema();
        for (func, column) in self
            .functions
            .iter()
            .zip(&output_columns[combined.num_columns()..])
        {
            let output_type: ArrowDataType = column.data_type.clone().into();
            columns.push(compute_window_function(func, &combined, &output_type)?);
        }

        let output_fields: Vec<Field> = output_columns
            .iter()
            .map(|c| Field::new(&c.name, c.data_type.clone().into(), c.nullable))
            .collect();
        let output_schema = Arc::new(Schema::new(output_fields));
        let result_batch = RecordBatch::try_new(output_schema.clone(), columns)?;

        Ok(stream_from_batches(output_schema, vec![result_batch]))
    }

    fn display_name(&self) -> &str {
        "WindowExec"
    }
}
