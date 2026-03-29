## ADDED Requirements

### Requirement: BroadcastOperator struct
The system SHALL implement a `BroadcastOperator` that implements the `ExecutionPlan` trait. It SHALL collect all input batches from its child operator, then write a complete copy of all batches to each of N output buffer partitions.

#### Scenario: Broadcast to all partitions
- **WHEN** `BroadcastOperator` is configured with 4 partitions and receives 3 batches totaling 50 rows
- **THEN** each of the 4 output buffer partitions receives all 3 batches (50 rows each)
- **AND** the data in each partition is identical

#### Scenario: Broadcast empty input
- **WHEN** `BroadcastOperator` receives zero rows from its child
- **THEN** each output buffer partition receives an empty batch with the correct schema

### Requirement: Terminal operator semantics
The `BroadcastOperator` SHALL produce no output batches from `execute()`, similar to `ShuffleWriteOperator`. It returns an empty `Vec<RecordBatch>`. Its purpose is to replicate data to OutputBuffer partitions.

#### Scenario: Execute returns empty
- **WHEN** `BroadcastOperator` executes on input data
- **THEN** `execute()` returns `Ok(vec![])` (no output batches)
- **AND** all OutputBuffer partitions contain the full replicated data

### Requirement: Memory-bounded usage
The `BroadcastOperator` SHALL hold all input data in memory before replicating. The total memory usage is `input_size * num_partitions`. This operator SHOULD only be used for small tables as determined by the distribution strategy.

#### Scenario: Large input warning
- **WHEN** `BroadcastOperator` processes input exceeding the broadcast threshold
- **THEN** the operator logs a warning about memory usage but proceeds with execution
