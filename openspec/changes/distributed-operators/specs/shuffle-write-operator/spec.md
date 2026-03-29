## ADDED Requirements

### Requirement: ShuffleWriteOperator struct
The system SHALL implement a `ShuffleWriteOperator` that implements the `ExecutionPlan` trait. It SHALL accept partition column indices, a number of partitions, and a reference to an `OutputBuffer`. The operator SHALL hash each row's partition column values using murmur3, compute `hash % num_partitions`, and write each row to the corresponding OutputBuffer partition.

#### Scenario: Hash partitioning by single column
- **WHEN** `ShuffleWriteOperator` is configured with 4 partitions on column `customer_id` and receives 100 rows
- **THEN** each row is written to the partition determined by `murmur3(customer_id) % 4`
- **AND** the total number of rows across all 4 partitions equals 100

#### Scenario: Hash partitioning by multiple columns
- **WHEN** `ShuffleWriteOperator` is configured with partition columns `[region, product_type]`
- **THEN** the hash is computed over the concatenated values of both columns for each row

### Requirement: Null partition key handling
The system SHALL assign rows with null values in any partition column to partition 0. This ensures deterministic placement of null-keyed rows.

#### Scenario: Null in partition column
- **WHEN** a row has a null value in the partition column
- **THEN** the row is assigned to partition 0

### Requirement: Terminal operator semantics
The `ShuffleWriteOperator` SHALL produce no output batches from `execute()`. It returns an empty `Vec<RecordBatch>`. Its purpose is to write to the OutputBuffer as a side effect.

#### Scenario: Execute returns empty
- **WHEN** `ShuffleWriteOperator` executes on input data
- **THEN** `execute()` returns `Ok(vec![])` (no output batches)
- **AND** the OutputBuffer partitions contain the partitioned data

### Requirement: Batch slicing
The system SHALL slice input `RecordBatch` rows into per-partition batches using Arrow's `filter_record_batch` with boolean masks. Each partition receives only the rows assigned to it.

#### Scenario: Efficient batch slicing
- **WHEN** a batch of 1000 rows is partitioned into 4 partitions
- **THEN** each partition receives a `RecordBatch` containing only its assigned rows
- **AND** no row is duplicated or lost
