## Context

Currently all connectors perform full table scans. The DataSource::scan() method returns all rows and all columns regardless of query needs. After Change 1 (async-streaming-execution), scan() returns a stream but still reads everything.

## Goals / Non-Goals

**Goals:**
- Define ScanContext with filter/projection/limit pushdown information
- Create optimizer framework for rewriting physical plans
- Implement projection and filter pushdown rules
- Update Parquet connector to read only needed columns and filter row groups
- Update CSV and memory connectors with basic pushdown

**Non-Goals:**
- Complex predicate pushdown (e.g., LIKE, IN subquery) — only simple comparisons
- Cost-based optimization — deterministic rule-based only
- Predicate pushdown across joins — that's for the logical optimizer (Change 4)

## Decisions

1. **ScanContext in common crate**: Contains `filters: Vec<PlanExpr>`, `projection: Option<Vec<usize>>`, `limit: Option<usize>`. Kept in common so both execution and connectors can reference it.

2. **ConnectorCapabilities as a struct, not trait**: Simple struct with boolean fields `supports_filter_pushdown`, `supports_projection_pushdown`, `supports_limit_pushdown`. Avoids trait object complexity.

3. **PhysicalPlanOptimizer pipeline**: Ordered `Vec<Box<dyn OptimizationRule>>`. Each rule takes `Arc<dyn ExecutionPlan>` and returns a rewritten plan. Rules applied in order: ProjectionPushdown first, then FilterPushdown.

4. **Projection pushdown for Parquet**: Use Arrow Parquet reader's `with_projection()` to read only needed columns. For row groups, use statistics-based filtering (min/max) when filter predicates reference indexed columns.

5. **Graceful degradation**: If a connector doesn't support pushdown, the optimizer leaves ScanExec unchanged. Filters/projections remain as separate operators above the scan.

## Risks / Trade-offs

- **Predicate evaluation duplication**: If a connector partially evaluates a filter, the FilterExec above must still verify (in case connector approximated). This is standard in pushdown systems.
- **ScanContext coupling**: Adding ScanContext to DataSource changes all connector implementations. Mitigated by making all ScanContext fields optional.
