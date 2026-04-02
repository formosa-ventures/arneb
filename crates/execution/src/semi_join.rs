//! Semi-join and anti-join physical operator.

use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;

use arneb_common::error::ExecutionError;
use arneb_common::stream::{collect_stream, stream_from_batches, SendableRecordBatchStream};
use arneb_common::types::ColumnInfo;
use arneb_planner::PlanExpr;
use arrow::array::{Array, ArrayRef, RecordBatch};
use arrow::datatypes::Schema;
use async_trait::async_trait;

use crate::expression;
use crate::operator::ExecutionPlan;

/// Semi-join (or anti-join) operator.
#[derive(Debug)]
pub(crate) struct SemiJoinExec {
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    left_key: PlanExpr,
    right_key: PlanExpr,
    anti: bool,
}

impl SemiJoinExec {
    pub(crate) fn new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        left_key: PlanExpr,
        right_key: PlanExpr,
        anti: bool,
    ) -> Self {
        Self {
            left,
            right,
            left_key,
            right_key,
            anti,
        }
    }
}

impl fmt::Display for SemiJoinExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.anti {
            write!(f, "AntiJoinExec")
        } else {
            write!(f, "SemiJoinExec")
        }
    }
}

fn hash_value(array: &ArrayRef, row: usize) -> Option<u64> {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    if array.is_null(row) {
        return None;
    }

    let mut hasher = DefaultHasher::new();
    let s = arrow::util::display::array_value_to_string(array, row).ok()?;
    s.hash(&mut hasher);
    Some(hasher.finish())
}

fn build_schema_from_column_info(cols: &[ColumnInfo]) -> Arc<Schema> {
    let fields: Vec<arrow::datatypes::Field> = cols
        .iter()
        .map(|c| arrow::datatypes::Field::new(&c.name, c.data_type.clone().into(), c.nullable))
        .collect();
    Arc::new(Schema::new(fields))
}

#[async_trait]
impl ExecutionPlan for SemiJoinExec {
    fn schema(&self) -> Vec<ColumnInfo> {
        self.left.schema()
    }

    async fn execute(&self) -> Result<SendableRecordBatchStream, ExecutionError> {
        // Build hash set from right side using right_key
        let right_stream = self.right.execute().await?;
        let right_batches = collect_stream(right_stream).await.map_err(|e| {
            ExecutionError::InvalidOperation(format!("failed to collect right input: {e}"))
        })?;

        let mut right_set: HashSet<u64> = HashSet::new();
        for batch in &right_batches {
            let right_keys = expression::evaluate(&self.right_key, batch, None)?;
            for row in 0..batch.num_rows() {
                if let Some(h) = hash_value(&right_keys, row) {
                    right_set.insert(h);
                }
            }
        }

        // Probe left side using left_key
        let left_stream = self.left.execute().await?;
        let left_batches = collect_stream(left_stream).await.map_err(|e| {
            ExecutionError::InvalidOperation(format!("failed to collect left input: {e}"))
        })?;

        let output_schema = build_schema_from_column_info(&self.left.schema());
        let mut result_batches = Vec::new();

        for batch in &left_batches {
            let left_keys = expression::evaluate(&self.left_key, batch, None)?;
            let mut indices = Vec::new();

            for row in 0..batch.num_rows() {
                let in_set = hash_value(&left_keys, row)
                    .map(|h| right_set.contains(&h))
                    .unwrap_or(false);
                let keep = if self.anti { !in_set } else { in_set };
                if keep {
                    indices.push(row as u32);
                }
            }

            if !indices.is_empty() {
                let idx_array = arrow::array::UInt32Array::from(indices);
                let columns: Vec<ArrayRef> = (0..batch.num_columns())
                    .map(|col| {
                        arrow::compute::take(batch.column(col), &idx_array, None).map_err(|e| {
                            ExecutionError::InvalidOperation(format!("take failed: {e}"))
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let filtered = RecordBatch::try_new(output_schema.clone(), columns)?;
                result_batches.push(filtered);
            }
        }

        Ok(stream_from_batches(output_schema, result_batches))
    }

    fn display_name(&self) -> &str {
        if self.anti {
            "AntiJoinExec"
        } else {
            "SemiJoinExec"
        }
    }
}
