# table-statistics

**Status**: ADDED
**Crate**: common

## Overview

TableStatistics struct with row count and per-column statistics (min, max, null count, distinct count) for future cost-based optimization decisions.

## ADDED Requirements

### Requirement: TableStatistics struct captures table-level statistics

#### Scenario: Statistics with known row count

- WHEN a TableStatistics is created with `row_count: Some(1000)`
- THEN `statistics.row_count` returns `Some(1000)`

#### Scenario: Statistics with unknown row count

- WHEN a TableStatistics is created with `row_count: None`
- THEN `statistics.row_count` returns `None`

### Requirement: ColumnStatistics captures per-column statistics

#### Scenario: Fully populated column statistics

- WHEN a ColumnStatistics is created with min_value, max_value, null_count, and distinct_count
- THEN all fields are accessible and return their respective `Some(value)`

#### Scenario: Partially known column statistics

- WHEN a ColumnStatistics is created with only `null_count: Some(50)`
- AND all other fields are None
- THEN `null_count` returns `Some(50)`
- AND `min_value`, `max_value`, `distinct_count` all return `None`

### Requirement: TableStatistics includes per-column statistics vector

#### Scenario: Table with column statistics

- WHEN a TableStatistics has `column_statistics: vec![col_a_stats, col_b_stats]`
- THEN `statistics.column_statistics[0]` returns statistics for column a
- AND `statistics.column_statistics[1]` returns statistics for column b

#### Scenario: Table with no column statistics

- WHEN a TableStatistics has an empty `column_statistics` vector
- THEN the statistics are valid but contain no per-column information
