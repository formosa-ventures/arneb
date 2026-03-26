//! Record batch stream abstractions for async streaming execution.

use std::pin::Pin;
use std::task::{Context, Poll};

use arrow::array::RecordBatch;
use futures::stream::{self, Stream};

use crate::error::TrinoError;

/// A stream of [`RecordBatch`]es with a known schema.
pub trait RecordBatchStream: Stream<Item = Result<RecordBatch, TrinoError>> + Send + Unpin {
    /// Returns the Arrow schema of batches produced by this stream.
    fn schema(&self) -> arrow::datatypes::SchemaRef;
}

/// A boxed, sendable [`RecordBatchStream`].
pub type SendableRecordBatchStream = Pin<Box<dyn RecordBatchStream>>;

/// Adapter that wraps a `Vec<RecordBatch>` into a [`SendableRecordBatchStream`].
struct VecBatchStream {
    schema: arrow::datatypes::SchemaRef,
    inner: stream::Iter<std::vec::IntoIter<Result<RecordBatch, TrinoError>>>,
}

impl Stream for VecBatchStream {
    type Item = Result<RecordBatch, TrinoError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl RecordBatchStream for VecBatchStream {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.schema.clone()
    }
}

/// Creates a [`SendableRecordBatchStream`] from a schema and pre-computed batches.
pub fn stream_from_batches(
    schema: arrow::datatypes::SchemaRef,
    batches: Vec<RecordBatch>,
) -> SendableRecordBatchStream {
    let items: Vec<Result<RecordBatch, TrinoError>> = batches.into_iter().map(Ok).collect();
    Box::pin(VecBatchStream {
        schema,
        inner: stream::iter(items),
    })
}

/// Collects all batches from a stream into a `Vec<RecordBatch>`.
pub async fn collect_stream(
    mut stream: SendableRecordBatchStream,
) -> Result<Vec<RecordBatch>, TrinoError> {
    use futures::StreamExt;
    let mut batches = Vec::new();
    while let Some(result) = stream.next().await {
        batches.push(result?);
    }
    Ok(batches)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
    use futures::StreamExt;
    use std::sync::Arc;

    fn test_schema() -> arrow::datatypes::SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            "id",
            ArrowDataType::Int32,
            false,
        )]))
    }

    fn test_batch(schema: &arrow::datatypes::SchemaRef) -> RecordBatch {
        RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn stream_from_batches_non_empty() {
        let schema = test_schema();
        let batch = test_batch(&schema);
        let mut stream = stream_from_batches(schema.clone(), vec![batch]);

        assert_eq!(stream.schema(), schema);
        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(first.num_rows(), 3);
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn stream_from_batches_empty() {
        let schema = test_schema();
        let mut stream = stream_from_batches(schema.clone(), vec![]);

        assert_eq!(stream.schema(), schema);
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn collect_stream_success() {
        let schema = test_schema();
        let batch = test_batch(&schema);
        let stream = stream_from_batches(schema, vec![batch]);

        let batches = collect_stream(stream).await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
    }

    #[tokio::test]
    async fn collect_stream_error_propagation() {
        let schema = test_schema();
        let items: Vec<Result<RecordBatch, TrinoError>> = vec![Err(TrinoError::Execution(
            crate::error::ExecutionError::InvalidOperation("test error".to_string()),
        ))];
        let stream: SendableRecordBatchStream = Box::pin(VecBatchStream {
            schema,
            inner: futures::stream::iter(items),
        });

        let result = collect_stream(stream).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("test error"));
    }
}
