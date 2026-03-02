//! Data source abstraction for the execution engine.
//!
//! The [`DataSource`] trait defines how the execution engine reads data.
//! Connectors (Change 6) will implement this trait; for now we provide
//! [`InMemoryDataSource`] for testing.

use std::fmt::Debug;
use std::sync::Arc;

use arrow::array::RecordBatch;
use trino_common::error::ExecutionError;
use trino_common::types::ColumnInfo;

/// A source of tabular data for the execution engine.
///
/// Implementations produce [`RecordBatch`]es matching their declared schema.
/// The catalog crate stays metadata-only; this trait lives here because
/// execution defines what it needs from data providers.
pub trait DataSource: Send + Sync + Debug {
    /// Returns the column schema of this data source.
    fn schema(&self) -> Vec<ColumnInfo>;

    /// Scans all rows from this data source.
    fn scan(&self) -> Result<Vec<RecordBatch>, ExecutionError>;
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
                let data_type = trino_common::types::DataType::try_from(f.data_type().clone())
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

impl DataSource for InMemoryDataSource {
    fn schema(&self) -> Vec<ColumnInfo> {
        self.schema.clone()
    }

    fn scan(&self) -> Result<Vec<RecordBatch>, ExecutionError> {
        Ok(self.batches.clone())
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
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
    use trino_common::types::DataType;

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

    #[test]
    fn in_memory_empty() {
        let ds = InMemoryDataSource::empty(test_schema());
        assert_eq!(ds.schema().len(), 2);
        let batches = ds.scan().unwrap();
        assert!(batches.is_empty());
    }

    #[test]
    fn in_memory_with_data() {
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
        let batches = ds.scan().unwrap();
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
