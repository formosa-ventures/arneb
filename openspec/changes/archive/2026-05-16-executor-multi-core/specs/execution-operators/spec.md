## MODIFIED Requirements

### Requirement: ExecutionPlan trait

The system SHALL define an `ExecutionPlan` trait with the following contract:

```rust
pub trait ExecutionPlan: Send + Sync + Debug {
    fn schema(&self) -> Vec<ColumnInfo>;

    fn output_partitioning(&self) -> Partitioning;

    fn required_input_partitioning(&self) -> Vec<Partitioning>;

    async fn execute(
        &self,
        partition: usize,
    ) -> Result<SendableRecordBatchStream, ExecutionError>;

    fn display_name(&self) -> &str;
}
```

The new `execute(partition: usize)` signature SHALL drive one independent output partition per call. Each call to `execute(i)` returns the stream for partition `i`, where `0 <= i < self.output_partitioning().partition_count()`.

`output_partitioning()` SHALL describe how rows are distributed across the operator's output partitions. `required_input_partitioning()` SHALL return one `Partitioning` per child, declaring the partition shape each child must satisfy. The physical planner inserts `RepartitionExec` to bridge mismatches.

All operators SHALL implement these methods. Operators with no per-partition logic SHALL default to `output_partitioning = self.input.output_partitioning()` (passthrough) and accept any input shape.

#### Scenario: Calling execute on each partition

- **GIVEN** an operator with `output_partitioning = RoundRobinBatch(4)`
- **WHEN** `execute(0)`, `execute(1)`, `execute(2)`, `execute(3)` are each called
- **THEN** each returns an independent `SendableRecordBatchStream` driving one partition

#### Scenario: Out-of-range partition index

- **WHEN** `execute(N)` is called with `N >= self.output_partitioning().partition_count()`
- **THEN** the call returns `Err(ExecutionError::InvalidPartition { requested: N, max: count - 1 })`

### Requirement: ScanExec

The system SHALL implement a `ScanExec` operator that reads data from an `Arc<dyn DataSource>` partitioned by the data source. `ScanExec::output_partitioning()` SHALL return `UnknownPartitioning(source.partition_count())`. `ScanExec::execute(partition)` SHALL forward to `source.scan(&ctx, partition)`.

#### Scenario: Multi-file scan exposes per-file partitions

- **GIVEN** a Parquet table with 8 files registered as a single `DataSource` reporting `partition_count() = 8`
- **WHEN** `ScanExec::output_partitioning()` is queried
- **THEN** it returns `UnknownPartitioning(8)`

#### Scenario: Per-partition execute reads one file

- **GIVEN** an 8-partition scan
- **WHEN** `ScanExec::execute(3)` is called
- **THEN** it returns the stream for file index 3 only

### Requirement: ProjectionExec

The system SHALL implement a `ProjectionExec` operator that evaluates a list of `PlanExpr` per input partition. It is stateless per partition: `output_partitioning() == input.output_partitioning()`, and `execute(i)` calls `input.execute(i)` and applies the projection to each batch.

#### Scenario: Projection is per-partition stateless

- **GIVEN** a 4-partition input
- **WHEN** `ProjectionExec::execute(2)` is called
- **THEN** it reads from `input.execute(2)` only and applies the projection to those batches

### Requirement: FilterExec

The system SHALL implement a `FilterExec` operator that applies a boolean predicate per input partition. Stateless per partition: `output_partitioning() == input.output_partitioning()`, and `execute(i)` calls `input.execute(i)` and filters each batch.

#### Scenario: Filter is per-partition stateless

- **GIVEN** a 4-partition input
- **WHEN** `FilterExec::execute(2)` is called
- **THEN** it reads from `input.execute(2)` only and filters those batches

### Requirement: LimitExec

The system SHALL split `LimitExec` into `LocalLimitExec` (per-partition cap of `n` rows) and `GlobalLimitExec` (final cap of `n` rows after coalesce). The physical planner SHALL emit `GlobalLimitExec(CoalescePartitionsExec(LocalLimitExec(...)))` for any `LIMIT n` clause.

#### Scenario: Local limit caps per partition

- **GIVEN** a 4-partition input with `LIMIT 100`
- **WHEN** the plan executes
- **THEN** each partition emits at most 100 rows before stopping
- **AND** the global coalesce + cap yields exactly 100 rows total

### Requirement: SortExec

The system SHALL split sort into `SortExec` (per-partition sort) and `SortMergeExec` (k-way merge). The physical planner SHALL emit `SortMergeExec(SortExec(...))` for any `ORDER BY` clause. `SortExec` is stateful per partition (must collect all input batches for that partition before sorting).

#### Scenario: Per-partition sort yields one sorted stream per partition

- **GIVEN** a 4-partition input with `ORDER BY col_a`
- **WHEN** `SortExec::execute(i)` is called
- **THEN** the stream contains batches sorted by `col_a` within partition `i` only

#### Scenario: SortMerge produces global order

- **GIVEN** a `SortMergeExec` over per-partition sorted input
- **WHEN** `execute(0)` is called (`SortMergeExec` has `output_partitioning = UnknownPartitioning(1)`)
- **THEN** the output is fully sorted by `col_a` across all partitions

### Requirement: HashJoinExec

The system SHALL implement `HashJoinExec` with the build side coalesced and the probe side parallel. Build hash table is constructed once and shared via `Arc<HashTable>`. Probe runs per partition: `execute(i)` probes partition `i` of the probe-side input against the shared hash table.

#### Scenario: Hash join probe is per-partition

- **GIVEN** a 4-partition probe side and a coalesced build side
- **WHEN** `HashJoinExec::execute(2)` is called
- **THEN** it probes only partition 2 of the probe-side input against the shared hash table

### Requirement: HashAggregateExec

The system SHALL split hash aggregate into `PartialAggregateExec` (per-partition partial state) and `FinalAggregateExec` (per-partition merge). The physical planner SHALL emit the two-phase plan with hash repartition by group keys between them, OR fall back to single-phase `HashAggregateExec(CoalescePartitionsExec(...))` based on cardinality estimate.

#### Scenario: Two-phase aggregate is per-partition

- **GIVEN** a high-cardinality `GROUP BY` with `target_partitions = 4`
- **WHEN** `PartialAggregateExec::execute(2)` is called
- **THEN** it builds the partial hash map for partition 2 only

## ADDED Requirements

### Requirement: Partitioning metadata on every operator

Every implementor of `ExecutionPlan` SHALL provide a meaningful `output_partitioning()`. Operators that wrap a single input SHALL default to passing through the input's partitioning unless they explicitly change it (sort/repartition/coalesce).

#### Scenario: Passthrough operators preserve partitioning

- **GIVEN** a `FilterExec` over `RoundRobinBatch(4)` input
- **WHEN** `output_partitioning()` is queried
- **THEN** it returns `RoundRobinBatch(4)` (passthrough)

### Requirement: Top-level query runner spawns per-partition tasks

The query runner in `crates/server/src/coordinator.rs` SHALL spawn one `tokio::task` per output partition of the root operator and merge them via `CoalescePartitionsExec` before encoding for pgwire.

```rust
let root = wrap_root_with_coalesce(physical_plan);
// root now has output_partitioning = UnknownPartitioning(1)
let stream = root.execute(0).await?;
```

The wrap is a no-op when the root already has `partition_count = 1`.

#### Scenario: 14-partition plan merges to single stream

- **GIVEN** a physical plan whose root reports `output_partitioning = UnknownPartitioning(14)`
- **WHEN** the query runner prepares the stream
- **THEN** it wraps the root in `CoalescePartitionsExec`, calls `execute(0)` on the wrapped root, and pgwire sees a single sequential stream

### Requirement: Determinism within partition

The system SHALL preserve order within a partition (consistent with single-thread behavior). Across partitions, the final `CoalescePartitionsExec` makes no order guarantee — SQL without `ORDER BY` is inherently unordered, and SQL with `ORDER BY` flows through `SortMergeExec` which preserves order.

#### Scenario: ORDER BY produces deterministic output

- **GIVEN** any query with `ORDER BY` and `target_partitions > 1`
- **WHEN** the query is run twice on the same data
- **THEN** both runs produce identical row ordering
