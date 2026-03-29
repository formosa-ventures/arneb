## ADDED Requirements

### Requirement: OutputBuffer struct
The system SHALL implement an `OutputBuffer` struct that manages per-partition bounded channels for `RecordBatch` data. It SHALL be constructed with a `partition_count: usize` and `buffer_capacity: usize` (default 32 batches per partition).

### Requirement: write_batch
The system SHALL implement `async write_batch(partition_id: usize, batch: RecordBatch) -> Result<()>` that sends a RecordBatch to the specified partition's channel. It SHALL return `Err` if the partition_id is out of range. It SHALL block (await) if the channel is full, providing natural backpressure.

#### Scenario: Writing to a valid partition
- **WHEN** `write_batch(0, batch)` is called on a buffer with 2 partitions
- **THEN** the batch is sent to partition 0's channel

#### Scenario: Writing to an invalid partition
- **WHEN** `write_batch(5, batch)` is called on a buffer with 2 partitions
- **THEN** `Err` is returned indicating invalid partition ID

### Requirement: read_stream
The system SHALL implement `read_stream(partition_id: usize) -> Result<RecordBatchReceiver>` that returns the receiving end of the partition's channel. The receiver SHALL yield `RecordBatch` values until the channel is closed. It SHALL return `Err` if the partition_id is out of range or if the stream has already been taken.

#### Scenario: Reading from a partition
- **WHEN** a writer sends 3 batches to partition 0 and then calls `finish()`
- **AND** a reader calls `read_stream(0)` and consumes all messages
- **THEN** the reader receives exactly 3 batches and then the stream ends

### Requirement: finish
The system SHALL implement `finish(partition_id: usize)` that closes the sender side of a partition's channel, signaling no more data. It SHALL also implement `finish_all()` that closes all partitions.

#### Scenario: Finishing a single partition
- **WHEN** `finish(0)` is called
- **THEN** the reader for partition 0 receives end-of-stream after consuming remaining batches
- **AND** other partitions remain open

#### Scenario: Finishing all partitions
- **WHEN** `finish_all()` is called
- **THEN** all partition readers receive end-of-stream

### Requirement: Bounded capacity
The system SHALL enforce bounded capacity per partition using `tokio::sync::mpsc::channel(capacity)`. Writers SHALL await when the buffer is full, providing backpressure to upstream operators.

#### Scenario: Backpressure when buffer full
- **WHEN** the buffer capacity is 2 and 2 batches are written without being consumed
- **AND** a 3rd batch is written
- **THEN** the write blocks until a batch is consumed by the reader
