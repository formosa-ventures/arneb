## MODIFIED Requirements

### Requirement: Analyzer pipeline

Arneb SHALL provide an `Analyzer` struct that holds an ordered `Vec<Box<dyn AnalysisPass>>` and runs them sequentially. The default pipeline ordering SHALL be:

1. `TypeCoercion`
2. `JoinReorder`

Future passes may be inserted, but `JoinReorder` SHALL always run after `TypeCoercion` (because cost estimation depends on resolved types).

```rust
pub struct Analyzer { passes: Vec<Box<dyn AnalysisPass>> }

impl Analyzer {
    pub fn default_pipeline() -> Self;
    pub fn new(passes: Vec<Box<dyn AnalysisPass>>) -> Self;
    pub fn run(&self, plan: LogicalPlan, ctx: &mut AnalyzerContext) -> Result<LogicalPlan, PlanError>;
}
```

#### Scenario: Default pipeline includes TypeCoercion then JoinReorder

- **WHEN** `Analyzer::default_pipeline()` is invoked
- **THEN** the returned pipeline contains `TypeCoercion` at index 0 and `JoinReorder` at index 1

#### Scenario: Passes run in declared order

- **GIVEN** an `Analyzer::new(vec![A, B, C])`
- **WHEN** `run(plan)` is invoked
- **THEN** `A::analyze` runs first, its output is passed to `B::analyze`, whose output is passed to `C::analyze`

#### Scenario: Pipeline short-circuits on error

- **GIVEN** an `Analyzer::new(vec![A, B, C])` where `B` returns an error
- **WHEN** `run(plan)` is invoked
- **THEN** `C::analyze` is not called
- **AND** the error returned from `B` is returned unchanged

### Requirement: AnalyzerContext state

Arneb SHALL provide an `AnalyzerContext` struct that passes carry shared state across one analyzer run.

```rust
pub struct AnalyzerContext {
    pub param_types: HashMap<ParamId, DataType>,
    pub session: SessionConfig,
    pub catalog_stats: Arc<CatalogStats>,
    pub hints: HintSet,
}
```

The new `catalog_stats` field SHALL carry the per-query statistics snapshot populated by `QueryPlanner::plan_query`. The new `hints` field SHALL carry parsed query-level hints (e.g. `NO_REORDER`) so individual passes can opt-out of work.

#### Scenario: catalog_stats threads from planner to JoinReorder

- **GIVEN** a `QueryPlanner` that has populated `ctx.catalog_stats` for 4 tables
- **WHEN** `JoinReorder::analyze(plan, ctx)` runs
- **THEN** it can access stats for every table referenced by `TableScan` nodes in the plan via `ctx.catalog_stats.get(...)`

#### Scenario: hints carry NO_REORDER

- **GIVEN** a SQL statement with leading `/*+ NO_REORDER */`
- **WHEN** the planner parses hints into `ctx.hints`
- **THEN** `ctx.hints.contains(Hint::NoReorder)` is `true`
