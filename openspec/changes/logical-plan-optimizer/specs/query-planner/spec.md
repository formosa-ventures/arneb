# query-planner

**Status**: MODIFIED
**Crate**: planner

## Overview

Wire the LogicalOptimizer into the QueryPlanner pipeline so that logical plans are optimized before physical planning.

## MODIFIED Requirements

### Requirement: QueryPlanner applies optimization after logical planning

#### Scenario: Standard query flow with optimizer

- WHEN a SQL query is planned via QueryPlanner
- THEN the pipeline is: parse → logical plan → **optimize** → physical plan
- AND the optimizer applies all registered rules in order

#### Scenario: Optimizer is configurable

- WHEN a QueryPlanner is constructed
- THEN it accepts an optional LogicalOptimizer
- AND if no optimizer is provided, a default optimizer with all standard rules is used

### Requirement: EXPLAIN shows optimized plan

#### Scenario: EXPLAIN with optimization

- WHEN `EXPLAIN SELECT a FROM t WHERE 1 = 1` is executed
- THEN the output shows the optimized logical plan (with the tautology filter removed)

#### Scenario: EXPLAIN shows optimization effect

- WHEN `EXPLAIN SELECT a FROM t WHERE true AND b > 5` is executed
- THEN the output shows the simplified predicate (`b > 5` without `true AND`)
