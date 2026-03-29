//! Scalar subquery physical operator.

use std::fmt;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::{Field, Schema};
use async_trait::async_trait;
use trino_common::error::ExecutionError;
use trino_common::stream::{collect_stream, stream_from_batches, SendableRecordBatchStream};
use trino_common::types::ColumnInfo;

use crate::operator::ExecutionPlan;

/// Scalar subquery operator.
///
/// Executes child plan and extracts a single scalar value.
/// - 0 rows → NULL
/// - 1 row, 1 column → that value
/// - >1 row → error
#[derive(Debug)]
pub(crate) struct ScalarSubqueryExec {
    child: Arc<dyn ExecutionPlan>,
}

impl ScalarSubqueryExec {
    pub(crate) fn new(child: Arc<dyn ExecutionPlan>) -> Self {
        Self { child }
    }
}

impl fmt::Display for ScalarSubqueryExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ScalarSubqueryExec")
    }
}

#[async_trait]
impl ExecutionPlan for ScalarSubqueryExec {
    fn schema(&self) -> Vec<ColumnInfo> {
        let child_schema = self.child.schema();
        if child_schema.is_empty() {
            vec![ColumnInfo {
                name: "scalar_subquery".to_string(),
                data_type: trino_common::types::DataType::Utf8,
                nullable: true,
            }]
        } else {
            vec![ColumnInfo {
                name: child_schema[0].name.clone(),
                data_type: child_schema[0].data_type.clone(),
                nullable: true,
            }]
        }
    }

    async fn execute(&self) -> Result<SendableRecordBatchStream, ExecutionError> {
        let stream = self.child.execute().await?;
        let batches = collect_stream(stream).await.map_err(|e| {
            ExecutionError::InvalidOperation(format!("failed to collect subquery: {e}"))
        })?;

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        if total_rows > 1 {
            return Err(ExecutionError::InvalidOperation(
                "scalar subquery must return at most one row".to_string(),
            ));
        }

        let child_schema = self.child.schema();
        if child_schema.is_empty() {
            return Err(ExecutionError::InvalidOperation(
                "scalar subquery must return exactly one column".to_string(),
            ));
        }

        let output_field = Field::new(
            &child_schema[0].name,
            child_schema[0].data_type.clone().into(),
            true,
        );
        let output_schema = Arc::new(Schema::new(vec![output_field.clone()]));

        let result_batch = if total_rows == 0 {
            let null_array = arrow::array::new_null_array(output_field.data_type(), 1);
            RecordBatch::try_new(output_schema.clone(), vec![null_array])?
        } else {
            let batch = &batches[0];
            let col = batch.column(0);
            let idx = arrow::array::UInt32Array::from(vec![0u32]);
            let sliced = arrow::compute::take(col, &idx, None)?;
            RecordBatch::try_new(output_schema.clone(), vec![sliced])?
        };

        Ok(stream_from_batches(output_schema, vec![result_batch]))
    }

    fn display_name(&self) -> &str {
        "ScalarSubqueryExec"
    }
}
