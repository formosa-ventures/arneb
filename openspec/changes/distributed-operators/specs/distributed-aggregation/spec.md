## ADDED Requirements

### Requirement: PartialHashAggregateExec
The system SHALL implement a `PartialHashAggregateExec` operator that performs partial aggregation on each worker. It SHALL use the same group-by keys as the original aggregation and produce intermediate aggregate states (partial counts, partial sums, etc.) rather than final results.

#### Scenario: Partial aggregation on worker
- **WHEN** `PartialHashAggregateExec` processes 10,000 rows with GROUP BY `region` and SUM(`revenue`)
- **THEN** it outputs one row per distinct `region` value with partial sum results
- **AND** the number of output rows is less than or equal to the number of distinct `region` values

#### Scenario: Partial COUNT
- **WHEN** `PartialHashAggregateExec` computes COUNT(*) grouped by `category` on a worker's partition
- **THEN** each output row contains the partial count for that category on this worker

### Requirement: FinalHashAggregateExec
The system SHALL implement a `FinalHashAggregateExec` operator that combines partial aggregate results from multiple workers. It SHALL merge partial states (sum partial sums, sum partial counts, etc.) for each group-by key to produce final aggregate values.

#### Scenario: Final aggregation combining partials
- **WHEN** `FinalHashAggregateExec` receives partial results from 3 workers for GROUP BY `region` and SUM(`revenue`)
- **THEN** it produces one row per distinct `region` with the global sum across all workers

#### Scenario: Final AVG from partial SUM and COUNT
- **WHEN** `FinalHashAggregateExec` computes AVG by combining partial SUM and partial COUNT
- **THEN** the final AVG equals `total_sum / total_count` across all workers

### Requirement: Two-phase aggregation plan generation
The system SHALL always use two-phase aggregation for distributed queries with GROUP BY. Phase 1 executes `PartialHashAggregateExec` on each worker. A shuffle exchange on group-by keys follows. Phase 2 executes `FinalHashAggregateExec` on the receiving workers.

#### Scenario: Two-phase plan structure
- **WHEN** a distributed query contains `SELECT region, SUM(revenue) FROM sales GROUP BY region`
- **THEN** the plan contains: worker fragments with `PartialHashAggregateExec` → `ShuffleWriteOperator` on `region` → coordinator/worker fragment with `FinalHashAggregateExec`

#### Scenario: Global aggregation without GROUP BY
- **WHEN** a distributed query contains `SELECT COUNT(*) FROM sales` (no GROUP BY)
- **THEN** each worker produces a single partial count row
- **AND** the coordinator combines them into a single final count
