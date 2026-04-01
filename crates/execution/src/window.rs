//! Window function physical operator.

use std::fmt;
use std::sync::Arc;

use arneb_common::error::ExecutionError;
use arneb_common::stream::{collect_stream, stream_from_batches, SendableRecordBatchStream};
use arneb_common::types::ColumnInfo;
use arneb_planner::WindowFunctionDef;
use arrow::array::{ArrayRef, Float64Array, Int64Array, RecordBatch};
use arrow::datatypes::{Field, Schema};
use async_trait::async_trait;

use crate::expression;
use crate::operator::ExecutionPlan;

/// Window function operator.
///
/// Materializes all input, sorts by partition+order keys, computes window
/// functions per partition, and appends result columns.
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
            // Aggregate window function — compute per partition
            let arg_arr = if func.args.is_empty() {
                None
            } else {
                Some(expression::evaluate(&func.args[0], batch, None)?)
            };

            // If ORDER BY is present, compute running aggregate; otherwise full partition
            let has_order = !func.order_by.is_empty();
            let mut results = vec![0f64; num_rows];
            let mut prev_partition = u64::MAX;
            let mut running_sum = 0f64;
            let mut running_count = 0i64;
            let mut running_min = f64::MAX;
            let mut running_max = f64::MIN;

            for (row, &pid) in partition_ids.iter().enumerate().take(num_rows) {
                if pid != prev_partition {
                    prev_partition = pid;
                    running_sum = 0.0;
                    running_count = 0;
                    running_min = f64::MAX;
                    running_max = f64::MIN;
                }

                if let Some(ref arr) = arg_arr {
                    if !arr.is_null(row) {
                        let val = get_f64_value(arr, row);
                        running_sum += val;
                        running_count += 1;
                        if val < running_min {
                            running_min = val;
                        }
                        if val > running_max {
                            running_max = val;
                        }
                    }
                } else {
                    running_count += 1;
                }

                if has_order {
                    // Running aggregate up to current row
                    results[row] = match name_upper.as_str() {
                        "SUM" => running_sum,
                        "AVG" => {
                            if running_count > 0 {
                                running_sum / running_count as f64
                            } else {
                                0.0
                            }
                        }
                        "COUNT" => running_count as f64,
                        "MIN" => running_min,
                        "MAX" => running_max,
                        _ => 0.0,
                    };
                } else {
                    results[row] = 0.0; // placeholder, will fill after partition scan
                }
            }

            if !has_order {
                // Full partition aggregate — need a second pass
                // First pass collected final values per partition; now fill all rows
                // Re-scan to compute per-partition totals
                let mut prev_pid = u64::MAX;
                let mut psum = 0f64;
                let mut pcount = 0i64;
                let mut pmin = f64::MAX;
                let mut pmax = f64::MIN;
                let mut partition_start = 0usize;

                #[allow(clippy::needless_range_loop)]
                for row in 0..=num_rows {
                    let pid = if row < num_rows {
                        partition_ids[row]
                    } else {
                        u64::MAX
                    };
                    if pid != prev_pid {
                        if row > 0 {
                            let val = match name_upper.as_str() {
                                "SUM" => psum,
                                "AVG" => {
                                    if pcount > 0 {
                                        psum / pcount as f64
                                    } else {
                                        0.0
                                    }
                                }
                                "COUNT" => pcount as f64,
                                "MIN" => pmin,
                                "MAX" => pmax,
                                _ => 0.0,
                            };
                            for item in results.iter_mut().take(row).skip(partition_start) {
                                *item = val;
                            }
                        }
                        prev_pid = pid;
                        psum = 0.0;
                        pcount = 0;
                        pmin = f64::MAX;
                        pmax = f64::MIN;
                        partition_start = row;
                    }
                    if row < num_rows {
                        if let Some(ref arr) = arg_arr {
                            if !arr.is_null(row) {
                                let val = get_f64_value(arr, row);
                                psum += val;
                                pcount += 1;
                                if val < pmin {
                                    pmin = val;
                                }
                                if val > pmax {
                                    pmax = val;
                                }
                            }
                        } else {
                            pcount += 1;
                        }
                    }
                }
            }

            if name_upper == "COUNT" {
                Ok(Arc::new(Int64Array::from(
                    results.iter().map(|v| *v as i64).collect::<Vec<_>>(),
                )))
            } else {
                Ok(Arc::new(Float64Array::from(results)))
            }
        }
        _ => Err(ExecutionError::InvalidOperation(format!(
            "unsupported window function: {}",
            func.name
        ))),
    }
}

fn compute_partition_ids(keys: &[ArrayRef], num_rows: usize) -> Vec<u64> {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    (0..num_rows)
        .map(|row| {
            let mut hasher = DefaultHasher::new();
            for key in keys {
                let s = arrow::util::display::array_value_to_string(key, row).unwrap_or_default();
                s.hash(&mut hasher);
            }
            hasher.finish()
        })
        .collect()
}

fn same_order_values(order_vals: &[ArrayRef], row_a: usize, row_b: usize) -> bool {
    for arr in order_vals {
        let a = arrow::util::display::array_value_to_string(arr, row_a).unwrap_or_default();
        let b = arrow::util::display::array_value_to_string(arr, row_b).unwrap_or_default();
        if a != b {
            return false;
        }
    }
    true
}

fn get_f64_value(arr: &ArrayRef, row: usize) -> f64 {
    if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
        return a.value(row) as f64;
    }
    if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
        return a.value(row);
    }
    if let Some(a) = arr.as_any().downcast_ref::<arrow::array::Int32Array>() {
        return a.value(row) as f64;
    }
    if let Some(a) = arr.as_any().downcast_ref::<arrow::array::Float32Array>() {
        return a.value(row) as f64;
    }
    0.0
}

#[async_trait]
impl ExecutionPlan for WindowExec {
    fn schema(&self) -> Vec<ColumnInfo> {
        let mut schema = self.child.schema();
        for f in &self.functions {
            let data_type = match f.name.to_uppercase().as_str() {
                "ROW_NUMBER" | "RANK" | "DENSE_RANK" | "COUNT" => {
                    arneb_common::types::DataType::Int64
                }
                _ => arneb_common::types::DataType::Float64,
            };
            schema.push(ColumnInfo {
                name: f.output_name.clone(),
                data_type,
                nullable: true,
            });
        }
        schema
    }

    async fn execute(&self) -> Result<SendableRecordBatchStream, ExecutionError> {
        let stream = self.child.execute().await?;
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

        for func in &self.functions {
            let result = compute_window_function(func, &combined)?;
            columns.push(result);
        }

        let output_fields: Vec<Field> = self
            .schema()
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
