## ADDED Requirements

### Requirement: RecordBatchStream trait
The system SHALL define a `RecordBatchStream` trait that extends `Stream<Item = Result<RecordBatch, ArnebError>> + Send + Unpin`. The trait SHALL include a `schema()` method that returns `Arc<arrow::datatypes::Schema>` so consumers can inspect the output schema without polling the stream.

#### Scenario: Stream provides schema before polling
- **WHEN** a `RecordBatchStream` is created for a data source with columns (id: Int32, name: Utf8)
- **THEN** calling `schema()` returns an `Arc<Schema>` with fields matching those columns without consuming any batches

### Requirement: SendableRecordBatchStream type alias
The system SHALL define `SendableRecordBatchStream` as `Pin<Box<dyn RecordBatchStream>>`. This type SHALL be the standard return type for all async execution operations across the `common`, `execution`, and `protocol` crates.

#### Scenario: SendableRecordBatchStream is object-safe and sendable
- **WHEN** a `SendableRecordBatchStream` is created from any `RecordBatchStream` implementation
- **THEN** it can be sent across tokio task boundaries and used as a trait object

### Requirement: stream_from_batches adapter
The system SHALL provide a `stream_from_batches(schema: Arc<Schema>, batches: Vec<RecordBatch>) -> SendableRecordBatchStream` function that wraps a materialized vector of record batches into a stream. The stream SHALL yield each batch in order, then terminate.

#### Scenario: Converting batches to a stream
- **WHEN** `stream_from_batches(schema, vec![batch1, batch2])` is called
- **THEN** polling the returned stream yields `batch1`, then `batch2`, then `None`

#### Scenario: Empty batch vector
- **WHEN** `stream_from_batches(schema, vec![])` is called
- **THEN** polling the returned stream yields `None` immediately
- **AND** `schema()` still returns the provided schema

### Requirement: collect_stream adapter
The system SHALL provide an `async fn collect_stream(stream: SendableRecordBatchStream) -> Result<Vec<RecordBatch>, ArnebError>` function that materializes all batches from a stream into a vector. It SHALL propagate the first error encountered.

#### Scenario: Collecting a successful stream
- **WHEN** `collect_stream(stream)` is awaited on a stream that yields 3 batches
- **THEN** it returns `Ok(vec![batch1, batch2, batch3])`

#### Scenario: Collecting a stream with an error
- **WHEN** `collect_stream(stream)` is awaited on a stream that yields 1 batch then an error
- **THEN** it returns `Err(...)` with the error from the stream
