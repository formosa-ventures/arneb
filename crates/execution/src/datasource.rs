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
///
/// Sources expose data in one or more independent **partitions**. Each
/// partition is a separate stream that the execution engine may scan in
/// parallel; partition indices range from `0` to `partition_count() - 1`.
/// Single-partition sources (in-memory, single Parquet file, CSV) return
/// `partition_count() = 1` and accept only `partition = 0`.
#[async_trait]
pub trait DataSource: Send + Sync + Debug {
    /// Returns the column schema of this data source.
    fn schema(&self) -> Vec<ColumnInfo>;

    /// Returns the number of independent partitions exposed by this source.
    /// Defaults to `1` (a single sequential stream). Multi-file sources
    /// (e.g. Hive tables backed by N parquet files) override this to
    /// expose per-file partitions.
    fn partition_count(&self) -> usize {
        1
    }

    /// Scans rows from one partition as an async stream.
    ///
    /// The `ctx` parameter carries optional pushdown hints. Implementations
    /// should apply as many hints as they support; callers must not rely on
    /// pushdown being applied (filters/projections above the scan remain).
    ///
    /// `partition` must be less than [`partition_count`](Self::partition_count);
    /// implementations may return `ExecutionError::InvalidOperation` for an
    /// out-of-range index.
    async fn scan(
        &self,
        ctx: &ScanContext,
        partition: usize,
    ) -> Result<SendableRecordBatchStream, ExecutionError>;
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

    async fn scan(
        &self,
        ctx: &ScanContext,
        partition: usize,
    ) -> Result<SendableRecordBatchStream, ExecutionError> {
        if partition != 0 {
            return Err(ExecutionError::InvalidOperation(format!(
                "InMemoryDataSource: partition {partition} out of range (single-partition source)"
            )));
        }
        // Honor projection pushdown. The CP fast path in
        // `crates/execution/src/planner.rs` sets `ctx.projection` to
        // the column indices it actually needs and rewrites upstream
        // column references to the projected order — so the source
        // MUST emit exactly those columns in that order. Real Parquet
        // sources already do this via
        // `ParquetRecordBatchStreamBuilder::with_projection`; the in-
        // memory source slices manually.
        let (output_schema, output_batches) = if let Some(indices) = &ctx.projection {
            let schema_subset: Vec<ColumnInfo> =
                indices.iter().map(|&i| self.schema[i].clone()).collect();
            let arrow_subset = column_info_to_arrow_schema(&schema_subset);
            let mut new_batches: Vec<RecordBatch> = Vec::with_capacity(self.batches.len());
            for b in &self.batches {
                let cols: Vec<arrow::array::ArrayRef> =
                    indices.iter().map(|&i| b.column(i).clone()).collect();
                new_batches.push(RecordBatch::try_new(arrow_subset.clone(), cols)?);
            }
            (arrow_subset, new_batches)
        } else {
            (
                column_info_to_arrow_schema(&self.schema),
                self.batches.clone(),
            )
        };
        Ok(stream_from_batches(output_schema, output_batches))
    }
}

/// Helper: build an Arrow [`Schema`](arrow::datatypes::Schema) from a slice
/// of [`ColumnInfo`].
pub fn column_info_to_arrow_schema(columns: &[ColumnInfo]) -> Arc<arrow::datatypes::Schema> {
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
        assert_eq!(ds.partition_count(), 1);
        let stream = ds.scan(&ScanContext::default(), 0).await.unwrap();
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
        let stream = ds.scan(&ScanContext::default(), 0).await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
    }

    #[tokio::test]
    async fn in_memory_rejects_out_of_range_partition() {
        let ds = InMemoryDataSource::empty(test_schema());
        let err = match ds.scan(&ScanContext::default(), 1).await {
            Err(e) => e,
            Ok(_) => panic!("expected partition-out-of-range error"),
        };
        assert!(format!("{err}").contains("partition 1 out of range"));
    }

    #[test]
    fn column_info_to_schema() {
        let schema = column_info_to_arrow_schema(&test_schema());
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "name");
    }
}
