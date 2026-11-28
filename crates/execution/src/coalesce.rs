//! `CoalescePartitionsExec`: merges N input partitions into 1 output stream.
//!
//! Each input partition is driven by its own `tokio::spawn`ed task. The
//! spawned tasks push their batches into a bounded `mpsc::channel`; the
//! returned `Stream` forwards batches in arrival order (no ordering
//! guarantee across partitions — operators that need ordering wrap a
//! sort-merge instead).
//!
//! Bounded channel capacity (default 4 batches per partition × N) gives
//! back-pressure: when downstream stalls, the channel fills, the
//! producer tasks `await` on `send` and stop reading the source.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arneb_common::error::{ArnebError, ExecutionError};
use arneb_common::stream::{RecordBatchStream, SendableRecordBatchStream};
use arneb_common::types::ColumnInfo;
use arrow::array::RecordBatch;
use async_trait::async_trait;
use futures::Stream;
use futures::StreamExt;

use crate::operator::ExecutionPlan;
use crate::partitioning::Partitioning;
use crate::task_guard::TaskGroup;

const DEFAULT_CHANNEL_CAPACITY: usize = 4;

/// Single-output operator that drains every input partition concurrently
/// and forwards batches into one merged stream.
#[derive(Debug)]
pub struct CoalescePartitionsExec {
    input: Arc<dyn ExecutionPlan>,
}

impl CoalescePartitionsExec {
    /// Wrap `input` so its multiple output partitions are merged into one
    /// sequential stream.
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        Self { input }
    }
}

#[async_trait]
impl ExecutionPlan for CoalescePartitionsExec {
    fn schema(&self) -> Vec<ColumnInfo> {
        self.input.schema()
    }

    fn display_name(&self) -> &str {
        "CoalescePartitionsExec"
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn required_input_partitioning(&self) -> Vec<Partitioning> {
        vec![self.input.output_partitioning()]
    }

    fn inject_dynamic_filter(&self, filter: arneb_planner::PlanExpr, target_index: usize) {
        // Schema-preserving: same column layout, pass through.
        self.input.inject_dynamic_filter(filter, target_index);
    }

    fn is_leaf_scan_subtree(&self) -> bool {
        self.input.is_leaf_scan_subtree()
    }

    async fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream, ExecutionError> {
        if partition != 0 {
            return Err(ExecutionError::InvalidOperation(format!(
                "CoalescePartitionsExec is single-output; partition {partition} out of range"
            )));
        }

        let n = self.input.output_partitioning().partition_count();
        // Fast path: nothing to coalesce.
        if n <= 1 {
            return self.input.execute(0).await;
        }

        let capacity = DEFAULT_CHANNEL_CAPACITY * n;
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<RecordBatch, ArnebError>>(capacity);
        let schema = crate::datasource::column_info_to_arrow_schema(&self.schema());

        // Dropping the output stream aborts the tasks, so an input still
        // waiting for its first batch is dropped too (#335).
        let mut tasks = TaskGroup::default();
        for p in 0..n {
            let tx = tx.clone();
            let input = Arc::clone(&self.input);
            tasks.spawn(async move {
                match input.execute(p).await {
                    Ok(mut stream) => {
                        while let Some(item) = stream.next().await {
                            if tx.send(item).await.is_err() {
                                // Receiver dropped — graceful shutdown.
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(Err(ArnebError::Execution(e))).await;
                    }
                }
            });
        }
        drop(tx); // channel closes when all spawned senders drop

        Ok(Box::pin(CoalesceStream {
            rx,
            schema,
            _tasks: tasks,
        }))
    }
}

struct CoalesceStream {
    rx: tokio::sync::mpsc::Receiver<Result<RecordBatch, ArnebError>>,
    schema: arrow::datatypes::SchemaRef,
    _tasks: TaskGroup,
}

impl Stream for CoalesceStream {
    type Item = Result<RecordBatch, ArnebError>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

impl RecordBatchStream for CoalesceStream {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task_guard::tests::{dropped_soon, StalledExec};

    #[tokio::test]
    async fn dropping_the_stream_drops_an_input_that_never_produced() {
        let (input, dropped) = StalledExec::new(2);
        let stream = CoalescePartitionsExec::new(input).execute(0).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(!dropped.load(std::sync::atomic::Ordering::SeqCst));
        drop(stream);
        assert!(dropped_soon(&dropped).await);
    }
}
