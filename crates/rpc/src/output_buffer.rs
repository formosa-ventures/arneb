//! Bounded, partition-aware output buffer for task results.
//!
//! Each task writes its output to an [`OutputBuffer`] with one or more
//! partitions. Remote consumers (via Flight or ExchangeClient) read from
//! specific partitions.

use std::sync::Arc;

use arrow::array::RecordBatch;
use tokio::sync::mpsc;
use trino_common::error::ExecutionError;

/// A bounded buffer where a task writes output RecordBatches partitioned
/// by index, and remote consumers read from specific partitions.
pub struct OutputBuffer {
    senders: Vec<mpsc::Sender<RecordBatch>>,
    receivers: Vec<Option<mpsc::Receiver<RecordBatch>>>,
    schema: Arc<arrow::datatypes::Schema>,
}

impl OutputBuffer {
    /// Creates a new output buffer with the given number of partitions
    /// and per-partition channel capacity.
    pub fn new(
        num_partitions: usize,
        capacity: usize,
        schema: Arc<arrow::datatypes::Schema>,
    ) -> Self {
        let mut senders = Vec::with_capacity(num_partitions);
        let mut receivers = Vec::with_capacity(num_partitions);

        for _ in 0..num_partitions {
            let (tx, rx) = mpsc::channel(capacity);
            senders.push(tx);
            receivers.push(Some(rx));
        }

        Self {
            senders,
            receivers,
            schema,
        }
    }

    /// Creates a single-partition buffer.
    pub fn single(capacity: usize, schema: Arc<arrow::datatypes::Schema>) -> Self {
        Self::new(1, capacity, schema)
    }

    /// Returns the output schema.
    pub fn schema(&self) -> Arc<arrow::datatypes::Schema> {
        self.schema.clone()
    }

    /// Returns the number of partitions.
    pub fn num_partitions(&self) -> usize {
        self.senders.len()
    }

    /// Write a batch to the specified partition.
    /// Returns error if the partition is invalid or the receiver has been dropped.
    pub async fn write_batch(
        &self,
        partition_id: usize,
        batch: RecordBatch,
    ) -> Result<(), ExecutionError> {
        let sender = self.senders.get(partition_id).ok_or_else(|| {
            ExecutionError::InvalidOperation(format!(
                "partition {partition_id} out of range (max {})",
                self.senders.len()
            ))
        })?;
        sender.send(batch).await.map_err(|_| {
            ExecutionError::InvalidOperation(format!(
                "output buffer partition {partition_id} receiver dropped"
            ))
        })
    }

    /// Take the receiver for a partition. Can only be called once per partition.
    /// The receiver yields RecordBatches as they are written.
    pub fn take_receiver(&mut self, partition_id: usize) -> Option<mpsc::Receiver<RecordBatch>> {
        self.receivers.get_mut(partition_id)?.take()
    }

    /// Signal that no more data will be written. Drops all senders.
    pub fn finish(self) {
        // Dropping senders closes the channels, signaling EOF to receivers.
        drop(self.senders);
    }

    /// Close all senders without consuming self. Signals EOF to all receivers.
    pub fn close(&mut self) {
        self.senders.clear();
    }
}

impl std::fmt::Debug for OutputBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OutputBuffer")
            .field("num_partitions", &self.senders.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]))
    }

    fn test_batch(schema: &Arc<Schema>, values: Vec<i32>) -> RecordBatch {
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(values))]).unwrap()
    }

    #[tokio::test]
    async fn single_partition_write_read() {
        let schema = test_schema();
        let mut buf = OutputBuffer::single(32, schema.clone());
        let mut rx = buf.take_receiver(0).unwrap();

        buf.write_batch(0, test_batch(&schema, vec![1, 2, 3]))
            .await
            .unwrap();

        // Read from receiver.
        let batch = rx.recv().await.unwrap();
        assert_eq!(batch.num_rows(), 3);
    }

    #[tokio::test]
    async fn multi_partition_write_read() {
        let schema = test_schema();
        let mut buf = OutputBuffer::new(3, 32, schema.clone());
        let mut rx0 = buf.take_receiver(0).unwrap();
        let mut rx1 = buf.take_receiver(1).unwrap();
        let mut rx2 = buf.take_receiver(2).unwrap();

        buf.write_batch(0, test_batch(&schema, vec![1]))
            .await
            .unwrap();
        buf.write_batch(1, test_batch(&schema, vec![2]))
            .await
            .unwrap();
        buf.write_batch(2, test_batch(&schema, vec![3]))
            .await
            .unwrap();

        assert_eq!(rx0.recv().await.unwrap().num_rows(), 1);
        assert_eq!(rx1.recv().await.unwrap().num_rows(), 1);
        assert_eq!(rx2.recv().await.unwrap().num_rows(), 1);
    }

    #[tokio::test]
    async fn finish_closes_channels() {
        let schema = test_schema();
        let mut buf = OutputBuffer::single(32, schema.clone());
        let mut rx = buf.take_receiver(0).unwrap();

        buf.write_batch(0, test_batch(&schema, vec![1]))
            .await
            .unwrap();
        buf.finish();

        // Should get the batch, then None (EOF).
        let batch = rx.recv().await.unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert!(rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn invalid_partition_returns_error() {
        let schema = test_schema();
        let buf = OutputBuffer::single(32, schema.clone());
        let result = buf.write_batch(5, test_batch(&schema, vec![1])).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn take_receiver_only_once() {
        let schema = test_schema();
        let mut buf = OutputBuffer::single(32, schema);
        assert!(buf.take_receiver(0).is_some());
        assert!(buf.take_receiver(0).is_none()); // second call returns None
    }
}
