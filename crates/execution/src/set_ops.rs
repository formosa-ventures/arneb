//! Set operation physical operators: UNION ALL, DISTINCT, INTERSECT, EXCEPT.

use std::collections::HashSet;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arneb_common::error::ExecutionError;
use arneb_common::stream::{collect_stream, stream_from_batches, SendableRecordBatchStream};
use arneb_common::types::ColumnInfo;
use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::Schema;
use async_trait::async_trait;

use crate::operator::ExecutionPlan;

fn build_schema(cols: &[ColumnInfo]) -> Arc<Schema> {
    let fields: Vec<arrow::datatypes::Field> = cols
        .iter()
        .map(|c| arrow::datatypes::Field::new(&c.name, c.data_type.clone().into(), c.nullable))
        .collect();
    Arc::new(Schema::new(fields))
}

fn hash_row(batch: &RecordBatch, row: usize) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    let mut hasher = DefaultHasher::new();
    for col in 0..batch.num_columns() {
        let s =
            arrow::util::display::array_value_to_string(batch.column(col), row).unwrap_or_default();
        s.hash(&mut hasher);
    }
    hasher.finish()
}

// -- UNION ALL --

#[derive(Debug)]
pub(crate) struct UnionAllExec {
    children: Vec<Arc<dyn ExecutionPlan>>,
}

impl UnionAllExec {
    pub(crate) fn new(children: Vec<Arc<dyn ExecutionPlan>>) -> Self {
        Self { children }
    }
}

impl fmt::Display for UnionAllExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "UnionAllExec")
    }
}

#[async_trait]
impl ExecutionPlan for UnionAllExec {
    fn schema(&self) -> Vec<ColumnInfo> {
        if self.children.is_empty() {
            vec![]
        } else {
            self.children[0].schema()
        }
    }

    async fn execute(&self) -> Result<SendableRecordBatchStream, ExecutionError> {
        let schema = build_schema(&self.schema());
        let mut all_batches = Vec::new();
        for child in &self.children {
            let stream = child.execute().await?;
            let batches = collect_stream(stream)
                .await
                .map_err(|e| ExecutionError::InvalidOperation(format!("union all collect: {e}")))?;
            all_batches.extend(batches);
        }
        Ok(stream_from_batches(schema, all_batches))
    }

    fn display_name(&self) -> &str {
        "UnionAllExec"
    }
}

// -- DISTINCT --

#[derive(Debug)]
pub(crate) struct DistinctExec {
    child: Arc<dyn ExecutionPlan>,
}

impl DistinctExec {
    pub(crate) fn new(child: Arc<dyn ExecutionPlan>) -> Self {
        Self { child }
    }
}

impl fmt::Display for DistinctExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DistinctExec")
    }
}

#[async_trait]
impl ExecutionPlan for DistinctExec {
    fn schema(&self) -> Vec<ColumnInfo> {
        self.child.schema()
    }

    async fn execute(&self) -> Result<SendableRecordBatchStream, ExecutionError> {
        let stream = self.child.execute().await?;
        let batches = collect_stream(stream)
            .await
            .map_err(|e| ExecutionError::InvalidOperation(format!("distinct collect: {e}")))?;

        let output_schema = build_schema(&self.child.schema());
        let mut seen: HashSet<u64> = HashSet::new();
        let mut result_batches = Vec::new();

        for batch in &batches {
            let mut indices = Vec::new();
            for row in 0..batch.num_rows() {
                let h = hash_row(batch, row);
                if seen.insert(h) {
                    indices.push(row as u32);
                }
            }
            if !indices.is_empty() {
                let idx_array = arrow::array::UInt32Array::from(indices);
                let columns: Vec<ArrayRef> = (0..batch.num_columns())
                    .map(|col| {
                        arrow::compute::take(batch.column(col), &idx_array, None)
                            .map_err(|e| ExecutionError::InvalidOperation(format!("take: {e}")))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                result_batches.push(RecordBatch::try_new(output_schema.clone(), columns)?);
            }
        }

        Ok(stream_from_batches(output_schema, result_batches))
    }

    fn display_name(&self) -> &str {
        "DistinctExec"
    }
}

// -- INTERSECT --

#[derive(Debug)]
pub(crate) struct IntersectExec {
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
}

impl IntersectExec {
    pub(crate) fn new(left: Arc<dyn ExecutionPlan>, right: Arc<dyn ExecutionPlan>) -> Self {
        Self { left, right }
    }
}

impl fmt::Display for IntersectExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IntersectExec")
    }
}

#[async_trait]
impl ExecutionPlan for IntersectExec {
    fn schema(&self) -> Vec<ColumnInfo> {
        self.left.schema()
    }

    async fn execute(&self) -> Result<SendableRecordBatchStream, ExecutionError> {
        let right_stream = self.right.execute().await?;
        let right_batches = collect_stream(right_stream)
            .await
            .map_err(|e| ExecutionError::InvalidOperation(format!("intersect right: {e}")))?;

        let mut right_set: HashSet<u64> = HashSet::new();
        for batch in &right_batches {
            for row in 0..batch.num_rows() {
                right_set.insert(hash_row(batch, row));
            }
        }

        let left_stream = self.left.execute().await?;
        let left_batches = collect_stream(left_stream)
            .await
            .map_err(|e| ExecutionError::InvalidOperation(format!("intersect left: {e}")))?;

        let output_schema = build_schema(&self.left.schema());
        let mut seen: HashSet<u64> = HashSet::new();
        let mut result_batches = Vec::new();

        for batch in &left_batches {
            let mut indices = Vec::new();
            for row in 0..batch.num_rows() {
                let h = hash_row(batch, row);
                if right_set.contains(&h) && seen.insert(h) {
                    indices.push(row as u32);
                }
            }
            if !indices.is_empty() {
                let idx_array = arrow::array::UInt32Array::from(indices);
                let columns: Vec<ArrayRef> = (0..batch.num_columns())
                    .map(|col| {
                        arrow::compute::take(batch.column(col), &idx_array, None)
                            .map_err(|e| ExecutionError::InvalidOperation(format!("take: {e}")))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                result_batches.push(RecordBatch::try_new(output_schema.clone(), columns)?);
            }
        }

        Ok(stream_from_batches(output_schema, result_batches))
    }

    fn display_name(&self) -> &str {
        "IntersectExec"
    }
}

// -- EXCEPT --

#[derive(Debug)]
pub(crate) struct ExceptExec {
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
}

impl ExceptExec {
    pub(crate) fn new(left: Arc<dyn ExecutionPlan>, right: Arc<dyn ExecutionPlan>) -> Self {
        Self { left, right }
    }
}

impl fmt::Display for ExceptExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ExceptExec")
    }
}

#[async_trait]
impl ExecutionPlan for ExceptExec {
    fn schema(&self) -> Vec<ColumnInfo> {
        self.left.schema()
    }

    async fn execute(&self) -> Result<SendableRecordBatchStream, ExecutionError> {
        let right_stream = self.right.execute().await?;
        let right_batches = collect_stream(right_stream)
            .await
            .map_err(|e| ExecutionError::InvalidOperation(format!("except right: {e}")))?;

        let mut right_set: HashSet<u64> = HashSet::new();
        for batch in &right_batches {
            for row in 0..batch.num_rows() {
                right_set.insert(hash_row(batch, row));
            }
        }

        let left_stream = self.left.execute().await?;
        let left_batches = collect_stream(left_stream)
            .await
            .map_err(|e| ExecutionError::InvalidOperation(format!("except left: {e}")))?;

        let output_schema = build_schema(&self.left.schema());
        let mut seen: HashSet<u64> = HashSet::new();
        let mut result_batches = Vec::new();

        for batch in &left_batches {
            let mut indices = Vec::new();
            for row in 0..batch.num_rows() {
                let h = hash_row(batch, row);
                if !right_set.contains(&h) && seen.insert(h) {
                    indices.push(row as u32);
                }
            }
            if !indices.is_empty() {
                let idx_array = arrow::array::UInt32Array::from(indices);
                let columns: Vec<ArrayRef> = (0..batch.num_columns())
                    .map(|col| {
                        arrow::compute::take(batch.column(col), &idx_array, None)
                            .map_err(|e| ExecutionError::InvalidOperation(format!("take: {e}")))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                result_batches.push(RecordBatch::try_new(output_schema.clone(), columns)?);
            }
        }

        Ok(stream_from_batches(output_schema, result_batches))
    }

    fn display_name(&self) -> &str {
        "ExceptExec"
    }
}
