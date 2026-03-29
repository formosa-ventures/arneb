## ADDED Requirements

### Requirement: MergeOperator struct
The system SHALL implement a `MergeOperator` that implements the `ExecutionPlan` trait. It SHALL perform a K-way merge of multiple pre-sorted input streams using a `BinaryHeap`. Each input stream is an `ExchangeClient` providing sorted `RecordBatch` data.

#### Scenario: Merge two sorted streams
- **WHEN** `MergeOperator` merges stream A `[1, 3, 5]` and stream B `[2, 4, 6]` sorted by value ASC
- **THEN** `execute()` returns batches with values in order `[1, 2, 3, 4, 5, 6]`

#### Scenario: Merge with one empty stream
- **WHEN** `MergeOperator` merges a non-empty stream and an empty stream
- **THEN** `execute()` returns all rows from the non-empty stream in sorted order

### Requirement: Multi-key sort support
The `MergeOperator` SHALL support multiple sort keys with configurable direction (ASC/DESC) and null ordering (NULLS FIRST/NULLS LAST) per key. Comparison uses lexicographic ordering across all sort keys.

#### Scenario: Multi-key merge with mixed directions
- **WHEN** `MergeOperator` merges streams sorted by `(region ASC, revenue DESC)`
- **THEN** the merged output maintains correct ordering: ascending by region, descending by revenue within each region

#### Scenario: Null ordering
- **WHEN** `MergeOperator` merges streams with null values in sort keys and NULLS LAST is specified
- **THEN** null-valued rows appear after all non-null rows in the merged output

### Requirement: Row-level comparison
The system SHALL implement a comparator for `BinaryHeap` entries that compares the current row from each stream. Each heap entry SHALL track the stream index, current batch, current row position, and sort key values.

#### Scenario: Heap-based merge ordering
- **WHEN** 4 streams each have 100 sorted rows
- **THEN** the merge produces 400 rows in globally sorted order
- **AND** the merge operation is O(N log K) where N is total rows and K is number of streams
