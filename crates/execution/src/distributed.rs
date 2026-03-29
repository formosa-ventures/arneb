//! Distributed execution operators: shuffle, broadcast, merge.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, RecordBatch};
use arrow::compute;
use arrow::datatypes::{self, DataType as ArrowDataType};
use async_trait::async_trait;
use trino_common::error::ExecutionError;
use trino_common::stream::{collect_stream, stream_from_batches, SendableRecordBatchStream};
use trino_common::types::ColumnInfo;
use trino_planner::SortExpr;

use crate::datasource::column_info_to_arrow_schema;
use crate::expression;
use crate::operator::ExecutionPlan;

// ===========================================================================
// ShuffleWriteExec
// ===========================================================================

/// Hash-partitions input rows and writes them to an OutputBuffer.
///
/// This is a terminal operator — it doesn't produce a stream. Instead,
/// `execute()` returns an empty stream after writing all data.
#[derive(Debug)]
pub struct ShuffleWriteExec {
    /// Input plan to read from.
    pub input: Arc<dyn ExecutionPlan>,
    /// Column indices to hash for partitioning.
    pub partition_columns: Vec<usize>,
    /// Number of output partitions.
    pub num_partitions: usize,
}

impl ShuffleWriteExec {
    /// Hash-partition a batch of rows. Returns a Vec of (partition_id, row_indices).
    pub fn partition_batch(&self, batch: &RecordBatch) -> Result<Vec<Vec<u32>>, ExecutionError> {
        let mut partitions: Vec<Vec<u32>> = vec![Vec::new(); self.num_partitions];

        for row in 0..batch.num_rows() {
            // Check for nulls in partition columns — assign to partition 0.
            let has_null = self
                .partition_columns
                .iter()
                .any(|&col| batch.column(col).is_null(row));

            let partition = if has_null {
                0
            } else {
                let hash = hash_row(batch, &self.partition_columns, row)?;
                (hash % self.num_partitions as u64) as usize
            };

            partitions[partition].push(row as u32);
        }

        Ok(partitions)
    }
}

#[async_trait]
impl ExecutionPlan for ShuffleWriteExec {
    fn schema(&self) -> Vec<ColumnInfo> {
        self.input.schema()
    }

    async fn execute(&self) -> Result<SendableRecordBatchStream, ExecutionError> {
        // Collect input and partition — actual writing to OutputBuffer is done
        // by the TaskManager that wraps this operator.
        let stream = self.input.execute().await?;
        let batches = collect_stream(stream)
            .await
            .map_err(|e| ExecutionError::InvalidOperation(format!("shuffle collect: {e}")))?;

        // For now, return all batches as-is (the partition_batch method is
        // available for the TaskManager to use when writing to OutputBuffer).
        let schema = column_info_to_arrow_schema(&self.schema());
        Ok(stream_from_batches(schema, batches))
    }

    fn display_name(&self) -> &str {
        "ShuffleWriteExec"
    }
}

// ===========================================================================
// BroadcastExec
// ===========================================================================

/// Replicates all input data — the output stream contains all input batches.
/// The actual replication to multiple partitions is handled by the TaskManager.
#[derive(Debug)]
pub struct BroadcastExec {
    /// Input plan to read from.
    pub input: Arc<dyn ExecutionPlan>,
}

#[async_trait]
impl ExecutionPlan for BroadcastExec {
    fn schema(&self) -> Vec<ColumnInfo> {
        self.input.schema()
    }

    async fn execute(&self) -> Result<SendableRecordBatchStream, ExecutionError> {
        // Just pass through — broadcasting (writing to all partitions) is
        // handled by the TaskManager.
        self.input.execute().await
    }

    fn display_name(&self) -> &str {
        "BroadcastExec"
    }
}

// ===========================================================================
// MergeExec
// ===========================================================================

/// K-way merge of multiple pre-sorted input streams into one sorted stream.
#[derive(Debug)]
pub struct MergeExec {
    /// Multiple input plans, each producing sorted data.
    pub inputs: Vec<Arc<dyn ExecutionPlan>>,
    /// Sort key specification.
    pub order_by: Vec<SortExpr>,
    /// Output schema.
    pub output_schema: Vec<ColumnInfo>,
}

#[async_trait]
impl ExecutionPlan for MergeExec {
    fn schema(&self) -> Vec<ColumnInfo> {
        self.output_schema.clone()
    }

    async fn execute(&self) -> Result<SendableRecordBatchStream, ExecutionError> {
        // Collect all inputs into sorted batch lists.
        let mut all_batches: Vec<RecordBatch> = Vec::new();

        for input in &self.inputs {
            let stream = input.execute().await?;
            let batches = collect_stream(stream)
                .await
                .map_err(|e| ExecutionError::InvalidOperation(format!("merge collect: {e}")))?;
            all_batches.extend(batches);
        }

        if all_batches.is_empty() {
            let schema = column_info_to_arrow_schema(&self.output_schema);
            return Ok(stream_from_batches(schema, vec![]));
        }

        // Concatenate all batches and sort.
        let schema = all_batches[0].schema();
        let combined = if all_batches.len() == 1 {
            all_batches.into_iter().next().unwrap()
        } else {
            compute::concat_batches(&schema, all_batches.iter())?
        };

        if combined.num_rows() == 0 {
            return Ok(stream_from_batches(schema, vec![]));
        }

        // Sort using the same logic as SortExec.
        let sort_columns: Vec<arrow::compute::SortColumn> = self
            .order_by
            .iter()
            .map(|s| {
                let col = expression::evaluate(&s.expr, &combined, None)?;
                Ok(arrow::compute::SortColumn {
                    values: col,
                    options: Some(arrow::compute::SortOptions {
                        descending: !s.asc,
                        nulls_first: s.nulls_first,
                    }),
                })
            })
            .collect::<Result<_, ExecutionError>>()?;

        let indices = compute::lexsort_to_indices(&sort_columns, None)?;

        let sorted_columns: Vec<ArrayRef> = (0..combined.num_columns())
            .map(|i| compute::take(combined.column(i), &indices, None).map_err(Into::into))
            .collect::<Result<_, ExecutionError>>()?;

        let result = RecordBatch::try_new(schema.clone(), sorted_columns)?;
        Ok(stream_from_batches(schema, vec![result]))
    }

    fn display_name(&self) -> &str {
        "MergeExec"
    }
}

// ===========================================================================
// Hash helpers
// ===========================================================================

fn hash_row(batch: &RecordBatch, key_indices: &[usize], row: usize) -> Result<u64, ExecutionError> {
    use std::hash::Hasher;
    let mut hasher = std::collections::hash_map::DefaultHasher::new();

    for &col_idx in key_indices {
        let col = batch.column(col_idx);
        hash_value(col, row, &mut hasher)?;
    }

    Ok(hasher.finish())
}

fn hash_value(
    arr: &ArrayRef,
    index: usize,
    hasher: &mut impl std::hash::Hasher,
) -> Result<(), ExecutionError> {
    use std::hash::Hash;
    match arr.data_type() {
        ArrowDataType::Int32 => {
            arr.as_primitive::<datatypes::Int32Type>()
                .value(index)
                .hash(hasher);
        }
        ArrowDataType::Int64 => {
            arr.as_primitive::<datatypes::Int64Type>()
                .value(index)
                .hash(hasher);
        }
        ArrowDataType::Utf8 => {
            arr.as_string::<i32>().value(index).hash(hasher);
        }
        dt => {
            return Err(ExecutionError::InvalidOperation(format!(
                "unsupported shuffle key type: {dt:?}"
            )));
        }
    }
    Ok(())
}

// ===========================================================================
// DistributionStrategy
// ===========================================================================

/// Strategy for distributing join data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DistributionStrategy {
    /// Broadcast the smaller side to all workers.
    Broadcast,
    /// Hash-partition both sides on join keys.
    HashPartition,
}

impl DistributionStrategy {
    /// Choose strategy based on estimated row count.
    /// If the smaller side has fewer rows than the threshold, broadcast.
    pub fn choose(smaller_side_rows: Option<usize>, threshold: usize) -> Self {
        match smaller_side_rows {
            Some(rows) if rows <= threshold => Self::Broadcast,
            _ => Self::HashPartition,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datasource::InMemoryDataSource;
    use crate::operator::ScanExec;
    use crate::scan_context::ScanContext;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    fn make_source(ids: Vec<i32>, names: Vec<&str>) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(names)),
            ],
        )
        .unwrap();
        let ds = InMemoryDataSource::new(
            vec![
                ColumnInfo {
                    name: "id".into(),
                    data_type: trino_common::types::DataType::Int32,
                    nullable: false,
                },
                ColumnInfo {
                    name: "name".into(),
                    data_type: trino_common::types::DataType::Utf8,
                    nullable: false,
                },
            ],
            vec![batch],
        );
        Arc::new(ScanExec {
            source: Arc::new(ds),
            _table_name: "test".into(),
            scan_context: ScanContext::default(),
        })
    }

    #[tokio::test]
    async fn shuffle_partition_distribution() {
        let source = make_source(vec![1, 2, 3, 4, 5, 6], vec!["a", "b", "c", "d", "e", "f"]);
        let shuffle = ShuffleWriteExec {
            input: source,
            partition_columns: vec![0],
            num_partitions: 3,
        };

        let stream = shuffle.input.execute().await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        let partitions = shuffle.partition_batch(&batches[0]).unwrap();

        // All 6 rows should be distributed across 3 partitions.
        let total: usize = partitions.iter().map(|p| p.len()).sum();
        assert_eq!(total, 6);
        assert_eq!(partitions.len(), 3);
    }

    #[tokio::test]
    async fn broadcast_passes_through() {
        let source = make_source(vec![1, 2, 3], vec!["a", "b", "c"]);
        let broadcast = BroadcastExec { input: source };

        let stream = broadcast.execute().await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
    }

    #[tokio::test]
    async fn merge_two_sorted_inputs() {
        let src1 = make_source(vec![1, 3, 5], vec!["a", "c", "e"]);
        let src2 = make_source(vec![2, 4, 6], vec!["b", "d", "f"]);

        let merge = MergeExec {
            inputs: vec![src1, src2],
            order_by: vec![SortExpr {
                expr: trino_planner::PlanExpr::Column {
                    index: 0,
                    name: "id".into(),
                },
                asc: true,
                nulls_first: false,
            }],
            output_schema: vec![
                ColumnInfo {
                    name: "id".into(),
                    data_type: trino_common::types::DataType::Int32,
                    nullable: false,
                },
                ColumnInfo {
                    name: "name".into(),
                    data_type: trino_common::types::DataType::Utf8,
                    nullable: false,
                },
            ],
        };

        let stream = merge.execute().await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 6);

        let ids = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        // Should be sorted: 1, 2, 3, 4, 5, 6
        for i in 0..5 {
            assert!(ids.value(i) <= ids.value(i + 1), "not sorted at index {i}");
        }
    }

    #[tokio::test]
    async fn merge_empty_inputs() {
        let merge = MergeExec {
            inputs: vec![],
            order_by: vec![],
            output_schema: vec![ColumnInfo {
                name: "id".into(),
                data_type: trino_common::types::DataType::Int32,
                nullable: false,
            }],
        };

        let stream = merge.execute().await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        assert!(batches.is_empty());
    }

    #[test]
    fn distribution_strategy_broadcast() {
        assert_eq!(
            DistributionStrategy::choose(Some(100), 10_000),
            DistributionStrategy::Broadcast
        );
    }

    #[test]
    fn distribution_strategy_hash() {
        assert_eq!(
            DistributionStrategy::choose(Some(100_000), 10_000),
            DistributionStrategy::HashPartition
        );
    }

    #[test]
    fn distribution_strategy_unknown_defaults_to_hash() {
        assert_eq!(
            DistributionStrategy::choose(None, 10_000),
            DistributionStrategy::HashPartition
        );
    }
}

// ===========================================================================
// ExchangeExec — reads from a remote worker's OutputBuffer via Flight RPC
// ===========================================================================

/// Physical operator that reads data from a remote worker via Arrow Flight.
#[derive(Debug)]
pub struct ExchangeExec {
    /// Remote worker's Flight RPC address (e.g., "http://127.0.0.1:9091").
    remote_address: String,
    /// Task ID on the remote worker.
    task_id: String,
    /// Partition to fetch.
    partition_id: u32,
    /// Expected output schema.
    schema_info: Vec<ColumnInfo>,
}

impl ExchangeExec {
    /// Create a new ExchangeExec for reading from a remote worker.
    pub fn new(
        remote_address: String,
        task_id: String,
        partition_id: u32,
        schema_info: Vec<ColumnInfo>,
    ) -> Self {
        Self {
            remote_address,
            task_id,
            partition_id,
            schema_info,
        }
    }
}

impl std::fmt::Display for ExchangeExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ExchangeExec({}:{}:{})",
            self.remote_address, self.task_id, self.partition_id
        )
    }
}

#[async_trait]
impl ExecutionPlan for ExchangeExec {
    fn schema(&self) -> Vec<ColumnInfo> {
        self.schema_info.clone()
    }

    async fn execute(&self) -> Result<SendableRecordBatchStream, ExecutionError> {
        // ExchangeExec requires the orchestration layer (QueryCoordinator in server crate)
        // to inject pre-fetched data. This placeholder returns an error if called directly.
        Err(ExecutionError::InvalidOperation(format!(
            "ExchangeExec({}:{}) not yet wired to remote data source",
            self.remote_address, self.task_id
        )))
    }

    fn display_name(&self) -> &str {
        "ExchangeExec"
    }
}
