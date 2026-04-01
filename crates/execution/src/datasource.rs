//! Data source abstraction for the execution engine.
//!
//! The [`DataSource`] trait defines how the execution engine reads data.
//! Connectors implement this trait; [`InMemoryDataSource`] is provided for testing.

use std::fmt::Debug;
use std::sync::Arc;

use arneb_common::error::ExecutionError;
use arneb_common::stream::{stream_from_batches, SendableRecordBatchStream};
use arneb_common::types::ColumnInfo;
use arrow::array::RecordBatch;
use async_trait::async_trait;

use crate::scan_context::ScanContext;

/// A source of tabular data for the execution engine.
///
/// Implementations produce [`RecordBatch`]es matching their declared schema.
/// The [`ScanContext`] carries optional pushdown hints (filters, projection, limit).
#[async_trait]
pub trait DataSource: Send + Sync + Debug {
    /// Returns the column schema of this data source.
    fn schema(&self) -> Vec<ColumnInfo>;

    /// Scans rows from this data source as an async stream.
    ///
    /// The `ctx` parameter carries optional pushdown hints. Implementations
    /// should apply as many hints as they support; callers must not rely on
    /// pushdown being applied (filters/projections above the scan remain).
    async fn scan(&self, ctx: &ScanContext) -> Result<SendableRecordBatchStream, ExecutionError>;
}

/// An in-memory data source backed by pre-built [`RecordBatch`]es.
#[derive(Debug, Clone)]
pub struct InMemoryDataSource {
    schema: Vec<ColumnInfo>,
    batches: Vec<RecordBatch>,
}

impl InMemoryDataSource {
    /// Creates a new in-memory data source.
    pub fn new(schema: Vec<ColumnInfo>, batches: Vec<RecordBatch>) -> Self {
        Self { schema, batches }
    }

    /// Creates an empty data source with the given schema and no rows.
    pub fn empty(schema: Vec<ColumnInfo>) -> Self {
        Self {
            schema,
            batches: vec![],
        }
    }

    /// Creates a data source from a single [`RecordBatch`], inferring the
    /// schema from the batch's Arrow schema.
    pub fn from_batch(batch: RecordBatch) -> Result<Self, ExecutionError> {
        let schema = batch
            .schema()
            .fields()
            .iter()
            .map(|f| {
                let data_type = arneb_common::types::DataType::try_from(f.data_type().clone())
                    .map_err(|e| ExecutionError::InvalidOperation(e.to_string()))?;
                Ok(ColumnInfo {
                    name: f.name().clone(),
                    data_type,
                    nullable: f.is_nullable(),
                })
            })
            .collect::<Result<Vec<_>, ExecutionError>>()?;
        Ok(Self {
            schema,
            batches: vec![batch],
        })
    }
}

#[async_trait]
impl DataSource for InMemoryDataSource {
    fn schema(&self) -> Vec<ColumnInfo> {
        self.schema.clone()
    }

    async fn scan(&self, _ctx: &ScanContext) -> Result<SendableRecordBatchStream, ExecutionError> {
        let arrow_schema = column_info_to_arrow_schema(&self.schema);
        Ok(stream_from_batches(arrow_schema, self.batches.clone()))
    }
}

/// Helper: build an Arrow [`Schema`](arrow::datatypes::Schema) from a slice
/// of [`ColumnInfo`].
pub(crate) fn column_info_to_arrow_schema(columns: &[ColumnInfo]) -> Arc<arrow::datatypes::Schema> {
    let fields: Vec<arrow::datatypes::Field> = columns.iter().map(|c| c.clone().into()).collect();
    Arc::new(arrow::datatypes::Schema::new(fields))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arneb_common::stream::collect_stream;
    use arneb_common::types::DataType;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};

    fn test_schema() -> Vec<ColumnInfo> {
        vec![
            ColumnInfo {
                name: "id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            },
            ColumnInfo {
                name: "name".to_string(),
                data_type: DataType::Utf8,
                nullable: true,
            },
        ]
    }

    #[tokio::test]
    async fn in_memory_empty() {
        let ds = InMemoryDataSource::empty(test_schema());
        assert_eq!(ds.schema().len(), 2);
        let stream = ds.scan(&ScanContext::default()).await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        assert!(batches.is_empty());
    }

    #[tokio::test]
    async fn in_memory_with_data() {
        let arrow_schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            ArrowDataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let ds = InMemoryDataSource::from_batch(batch).unwrap();
        assert_eq!(ds.schema().len(), 1);
        assert_eq!(ds.schema()[0].name, "id");
        let stream = ds.scan(&ScanContext::default()).await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
    }

    #[test]
    fn column_info_to_schema() {
        let schema = column_info_to_arrow_schema(&test_schema());
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "name");
    }
}
