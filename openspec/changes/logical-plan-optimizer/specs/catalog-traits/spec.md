# catalog-traits

**Status**: MODIFIED
**Crate**: catalog

## Overview

Extend TableProvider with an optional statistics() method so optimizer rules and cost-based decisions can access table-level and column-level statistics.

## MODIFIED Requirements

### Requirement: TableProvider provides optional statistics

#### Scenario: Table provider with statistics available

- WHEN `statistics()` is called on a TableProvider that has statistics
- THEN it returns `Some(TableStatistics)` with available row count and column statistics

#### Scenario: Table provider without statistics

- WHEN `statistics()` is called on a TableProvider that does not support statistics
- THEN it returns `None`
- AND the optimizer proceeds without statistics-dependent optimizations

### Requirement: Default implementation returns None

#### Scenario: Existing TableProvider implementations

- WHEN an existing TableProvider does not override `statistics()`
- THEN the default trait method returns `None`
- AND the existing implementation continues to compile without changes
