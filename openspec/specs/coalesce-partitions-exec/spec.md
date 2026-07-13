## ADDED Requirements

### Requirement: CoalescePartitionsExec operator

The system SHALL provide a `CoalescePartitionsExec` operator in `crates/execution/src/coalesce.rs` that takes a single child producing `N` input partitions and produces exactly 1 output partition. The output emits batches in arbitrary order from any of the input partitions as soon as they become available.

```rust
pub struct CoalescePartitionsExec {
    input: Arc<dyn ExecutionPlan>,
}

impl CoalescePartitionsExec {
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self;
}
```

`CoalescePartitionsExec::output_partitioning()` SHALL return `UnknownPartitioning(1)`. `CoalescePartitionsExec::required_input_partitioning()` SHALL return `vec![UnknownPartitioning(input.output_partitioning().partition_count())]` (it accepts any partition shape).

#### Scenario: Coalesce preserves all rows

- **GIVEN** a 4-partition input emitting a total of 1000 rows distributed across partitions
- **WHEN** `CoalescePartitionsExec(input).execute(0)` is drained
- **THEN** the output contains exactly 1000 rows (set equality with the input)

#### Scenario: Coalesce makes no order guarantee

- **GIVEN** a 4-partition input where partition `i` emits rows tagged `(i, k)` for `k ∈ [0, K)`
- **WHEN** `CoalescePartitionsExec(input).execute(0)` is drained
- **THEN** the output may interleave rows from different input partitions arbitrarily
- **AND** SQL with `ORDER BY` must rely on a `SortExec` that produces sorted output (CoalescePartitions does NOT preserve order)

### Requirement: Coalesce is concurrent

The system SHALL drain input partitions concurrently using `futures::stream::select_all` (or an equivalent multi-way `Select`). A slow input partition SHALL NOT block fast input partitions; the output stream emits batches from any input that has a ready batch.

#### Scenario: Slow partition does not block fast partition

- **GIVEN** a 2-partition input where partition 0 sleeps 100 ms before emitting, partition 1 emits immediately
- **WHEN** `CoalescePartitionsExec(input).execute(0)` is polled
- **THEN** the first batch from partition 1 is yielded before the first batch from partition 0

### Requirement: Coalesce is used at the top-level result boundary

The query runner (in `crates/server/src/coordinator.rs` and the pgwire result encoder) SHALL wrap the root operator in a `CoalescePartitionsExec` whenever the root's `output_partitioning().partition_count() > 1`, so that pgwire encoding sees a single sequential `SendableRecordBatchStream`.

#### Scenario: Pgwire sees a single stream

- **GIVEN** a 14-partition top-level scan
- **WHEN** the query runner prepares the stream for pgwire
- **THEN** the root operator is wrapped in `CoalescePartitionsExec(root)` and pgwire reads from a single stream
