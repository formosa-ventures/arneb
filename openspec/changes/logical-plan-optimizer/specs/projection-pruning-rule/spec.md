# projection-pruning-rule

**Status**: ADDED
**Crate**: planner

## Overview

Remove unused columns from TableScan by tracking which columns are actually referenced by downstream operators and inserting minimal Projections.

## ADDED Requirements

### Requirement: Unused columns are pruned from TableScan

#### Scenario: Query selects a subset of table columns

- WHEN a query is `SELECT a, b FROM t` and table `t` has columns [a, b, c, d]
- THEN the optimized plan's TableScan only reads columns [a, b]
- AND a Projection is inserted above the TableScan if needed to select only [a, b]

#### Scenario: All columns are used

- WHEN a query references all columns of a table
- THEN the plan is unchanged
- AND no additional Projection is inserted

### Requirement: Column references in expressions are tracked

#### Scenario: Filter references additional columns

- WHEN a query is `SELECT a FROM t WHERE b > 10`
- THEN both columns [a, b] are tracked as referenced
- AND the TableScan reads [a, b] (not all columns)

#### Scenario: Join condition references columns

- WHEN a join condition references columns from both sides
- THEN those columns are included in the referenced set for each respective TableScan

### Requirement: Multi-expression column tracking

#### Scenario: Expression references multiple columns

- WHEN a projection expression is `a + b AS total`
- THEN both columns [a, b] are tracked as referenced
- AND both are included in the pruned TableScan

### Requirement: Pruning works through multiple plan layers

#### Scenario: Projection above Filter above TableScan

- WHEN the plan is Projection([a]) → Filter(b > 10) → TableScan([a, b, c, d])
- THEN the referenced columns are [a, b]
- AND the TableScan is pruned to read only [a, b]
