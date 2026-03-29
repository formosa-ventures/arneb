## MODIFIED Requirements

### Requirement: DataSource trait
The system SHALL define a `DataSource` trait with `schema()` and `async fn scan()` methods. The `scan()` method SHALL return `Result<SendableRecordBatchStream, ExecutionError>` instead of `Result<Vec<RecordBatch>, ExecutionError>`. The trait SHALL require `Send + Sync + Debug` bounds and use `#[async_trait]` to enable dynamic dispatch via `Arc<dyn DataSource>`.

#### Scenario: Getting data source schema
- **WHEN** `source.schema()` is called on a data source with columns (id: Int32, name: Utf8)
- **THEN** it returns a `Vec<ColumnInfo>` with two entries matching those column definitions

#### Scenario: Scanning all rows as a stream
- **WHEN** `source.scan().await` is called on a data source containing 3 rows
- **THEN** it returns `Ok(stream)` where collecting the stream yields batches with a total of 3 rows

### Requirement: InMemoryDataSource
The system SHALL provide an `InMemoryDataSource` implementation of `DataSource` backed by pre-built `RecordBatch`es. Its `scan()` method SHALL return a stream wrapping the stored batches via `stream_from_batches()`. It SHALL support construction from explicit schema + batches, from a single batch (inferring schema), or as an empty source.

#### Scenario: Creating from a batch
- **WHEN** `InMemoryDataSource::from_batch(batch)` is called with a RecordBatch containing 3 rows
- **THEN** `scan().await` returns a stream that yields that batch

#### Scenario: Empty data source
- **WHEN** `InMemoryDataSource::empty(schema)` is called
- **THEN** `scan().await` returns a stream that yields no batches
- **AND** `schema()` returns the provided schema

### Requirement: Arrow schema conversion helper
The system SHALL provide a `column_info_to_arrow_schema()` function that converts a `&[ColumnInfo]` into an `Arc<arrow::datatypes::Schema>`.

#### Scenario: Converting column info to Arrow schema
- **WHEN** `column_info_to_arrow_schema(&[ColumnInfo { name: "id", data_type: Int32, nullable: false }])` is called
- **THEN** it returns an `Arc<Schema>` with one field named "id" of type Int32, non-nullable
