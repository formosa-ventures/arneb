## ADDED Requirements

### Requirement: Probe-side parallel hash join

The system SHALL implement parallel `HashJoinExec` with the build side coalesced into a single hash table and the probe side running per-partition. The build side SHALL be wrapped in a `CoalescePartitionsExec` so that a single hash table is built once, then shared (via `Arc`) with all probe partitions.

```rust
// Conceptual plan structure (post-planner):
HashJoinExec(probe, Arc<HashTable>, conditions) {
    output_partitioning: probe.output_partitioning(),
    required_input_partitioning: vec![
        probe.output_partitioning(),       // probe side: any partition shape
        UnknownPartitioning(1),            // build side: single coalesced
    ],
}
```

The build hash table SHALL be constructed once on the coordinating task and SHALL be wrapped in `Arc<HashTable>` for shared read access by all probe tasks.

#### Scenario: Parallel probe gives same output as serial

- **GIVEN** an inner-join query joining 6M rows on the probe side with 1.5M on build, partitioned 14-way on probe
- **WHEN** the query is executed
- **THEN** the row count and content match (set equality) the same query executed with `target_partitions = 1`

#### Scenario: Hash table built once

- **GIVEN** a 14-partition probe side
- **WHEN** the join executes
- **THEN** the build-side hash table is constructed exactly once (verified via a debug counter on `HashTable::new`), not 14 times

#### Scenario: LEFT join with parallel probe handles unmatched rows

- **GIVEN** a `LEFT JOIN` where 30% of probe rows have no matching build row, partitioned 4-way on probe
- **WHEN** the join executes
- **THEN** every unmatched probe row appears in the output exactly once with NULLs for the build side columns

#### Scenario: Empty build side

- **GIVEN** a `LEFT JOIN` where the build side has zero rows, probe partitioned 4-way
- **WHEN** the join executes
- **THEN** every probe row appears in the output with NULLs for build columns (no panic, no deadlock)

### Requirement: Build-side coalesce is automatic

The physical planner SHALL automatically wrap the build side in `CoalescePartitionsExec` when the join is constructed and the build side's `output_partitioning().partition_count() > 1`. The user does not need to mark which side is build vs. probe explicitly.

#### Scenario: Build coalesce inserted

- **GIVEN** a join where the right (build) side is a 14-partition scan
- **WHEN** the physical planner runs
- **THEN** the plan has `HashJoinExec(probe, CoalescePartitionsExec(build), ...)`

### Requirement: Per-partition probe is independent

Each probe partition SHALL execute its hash-table lookups concurrently with all other probe partitions. The build hash table SHALL be `Send + Sync` and `Arc`-shareable.

#### Scenario: Probe partitions are concurrent

- **GIVEN** a 4-partition probe side with `tokio::spawn` per partition
- **WHEN** all four probe tasks run concurrently
- **THEN** they share read-only access to the build hash table without locks (the table is read-only after construction)

### Requirement: Multi-build-side parallel join is out of scope (v1)

The system MAY support parallel build-side construction in a follow-up change. In v1, build is always coalesced. This decision SHALL be documented in `design.md`.

#### Scenario: Build remains single-threaded in v1

- **GIVEN** any join query
- **WHEN** the physical planner runs
- **THEN** the build hash table is constructed on a single task
