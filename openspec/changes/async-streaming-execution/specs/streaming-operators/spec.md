## ADDED Requirements

### Requirement: Streaming ScanExec
The system SHALL implement `ScanExec` such that `execute()` awaits `DataSource::scan()` and returns the resulting stream directly. No intermediate materialization SHALL occur.

#### Scenario: Scanning streams data through
- **WHEN** `ScanExec` wraps a data source that produces 3 batches
- **THEN** `execute().await` returns a stream that yields those 3 batches in order

### Requirement: Streaming FilterExec
The system SHALL implement `FilterExec` such that `execute()` returns a stream that applies the predicate to each input batch as it arrives. Each input batch SHALL be filtered independently. Batches that are empty after filtering SHALL be skipped in the output stream.

#### Scenario: Filtering batch-at-a-time
- **WHEN** `FilterExec` receives an input stream of 2 batches, each with rows matching and not matching the predicate
- **THEN** the output stream yields filtered batches as input batches arrive, without waiting for all input

#### Scenario: Batch entirely filtered out
- **WHEN** a batch contains no rows matching the predicate
- **THEN** that batch is skipped and the stream proceeds to the next input batch

### Requirement: Streaming ProjectionExec
The system SHALL implement `ProjectionExec` such that `execute()` returns a stream that evaluates projection expressions on each input batch as it arrives. Each input batch SHALL be projected independently.

#### Scenario: Projecting batch-at-a-time
- **WHEN** `ProjectionExec` receives an input stream of 2 batches
- **THEN** the output stream yields projected batches one-for-one with input batches

### Requirement: Streaming LimitExec
The system SHALL implement `LimitExec` such that `execute()` returns a stream that tracks cumulative row counts across batches, applying OFFSET and LIMIT. The stream SHALL terminate early once the limit is reached, without consuming further input batches.

#### Scenario: Early termination at limit
- **WHEN** `LimitExec` with limit=5 receives an input stream where the first 2 batches contain 10 rows total
- **THEN** the output stream yields at most 5 rows and terminates without polling further input batches

#### Scenario: Offset skips initial rows
- **WHEN** `LimitExec` with offset=3, limit=2 receives batches
- **THEN** the output stream skips the first 3 rows and yields the next 2 rows

### Requirement: Collecting SortExec
The system SHALL implement `SortExec` such that `execute()` collects all input batches from the child stream, concatenates and sorts them, then returns the sorted result as a stream.

#### Scenario: Sort collects then streams
- **WHEN** `SortExec` receives an input stream of 3 batches
- **THEN** it collects all 3 batches, sorts the combined data, and returns a stream of the sorted result

### Requirement: Collecting HashAggregateExec
The system SHALL implement `HashAggregateExec` such that `execute()` collects all input batches from the child stream, performs hash-based grouping and aggregation, then returns the aggregated result as a stream.

#### Scenario: Aggregate collects then streams
- **WHEN** `HashAggregateExec` receives an input stream
- **THEN** it collects all batches, computes aggregates, and returns a stream containing the result

### Requirement: Collecting NestedLoopJoinExec
The system SHALL implement `NestedLoopJoinExec` such that `execute()` collects all batches from both left and right child streams, performs the join, then returns the joined result as a stream.

#### Scenario: Join collects both sides then streams
- **WHEN** `NestedLoopJoinExec` receives left and right input streams
- **THEN** it collects all batches from both sides, computes the join, and returns a stream of the result

### Requirement: Streaming ExplainExec
The system SHALL implement `ExplainExec` such that `execute()` returns a single-batch stream containing the formatted logical plan text. No child execution SHALL occur.

#### Scenario: Explain produces single-batch stream
- **WHEN** `ExplainExec::execute().await` is called
- **THEN** it returns a stream that yields exactly one batch with the plan text, then terminates
