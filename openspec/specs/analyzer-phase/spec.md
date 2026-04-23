# analyzer-phase Specification

## Purpose
TBD - created by archiving change planner-type-coercion. Update Purpose after archive.
## Requirements
### Requirement: AnalysisPass trait

Arneb SHALL provide an `AnalysisPass` trait in `crates/planner/src/analyzer/mod.rs` with the following contract:

```rust
pub trait AnalysisPass: Send + Sync {
    fn name(&self) -> &'static str;
    fn analyze(&self, plan: LogicalPlan, ctx: &mut AnalyzerContext) -> Result<LogicalPlan, PlanError>;
}
```

Each pass MAY return a `PlanError` when it detects a semantic defect. This is the key contract distinction from `LogicalRule` (optimizer rules MUST NOT introduce new errors).

#### Scenario: Pass reports plan-time error

- **WHEN** an `AnalysisPass::analyze` implementation encounters a construct it cannot reconcile
- **THEN** it returns `Err(PlanError::...)` with a descriptive variant
- **AND** the error is surfaced to the caller unchanged by the `Analyzer` runner

### Requirement: Analyzer pipeline

Arneb SHALL provide an `Analyzer` struct that holds an ordered `Vec<Box<dyn AnalysisPass>>` and runs them sequentially.

```rust
pub struct Analyzer { passes: Vec<Box<dyn AnalysisPass>> }

impl Analyzer {
    pub fn default_pipeline() -> Self;
    pub fn new(passes: Vec<Box<dyn AnalysisPass>>) -> Self;
    pub fn run(&self, plan: LogicalPlan, ctx: &mut AnalyzerContext) -> Result<LogicalPlan, PlanError>;
}
```

#### Scenario: Default pipeline includes TypeCoercion

- **WHEN** `Analyzer::default_pipeline()` is invoked
- **THEN** the returned pipeline's first pass is `TypeCoercion`

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
}
```

The context is created per query and discarded after the pipeline completes. Passes read and write it.

#### Scenario: Parameter types accumulate across passes

- **GIVEN** `TypeCoercion` infers `$1: Date32` during its analyze phase
- **WHEN** a later pass reads `ctx.param_types[&1]`
- **THEN** it returns `Some(Date32)`

### Requirement: QueryPlanner invokes Analyzer before optimization

`crates/planner/src/planner.rs::QueryPlanner::plan_query` SHALL invoke the `Analyzer::default_pipeline()` on the `LogicalPlan` it builds, before passing the result to `LogicalOptimizer`.

#### Scenario: Planning pipeline order

- **WHEN** `plan_query(statement)` runs for a SELECT
- **THEN** the call order is: `plan_query` → Analyzer → LogicalOptimizer → return

#### Scenario: Analyzer error surfaces to caller

- **GIVEN** a query `SELECT col FROM t WHERE wrongtype_col <= DATE '1998-01-01'` where `wrongtype_col` is a non-date column
- **WHEN** `plan_query` runs
- **THEN** it returns `Err(PlanError::TypeMismatch { .. })`
- **AND** the optimizer is never invoked

### Requirement: Analyzer is testable in isolation

Unit tests SHALL construct an `Analyzer` with a test pipeline and invoke `run` on a synthetic `LogicalPlan` without going through SQL parsing.

#### Scenario: Unit test of coercion pass

- **GIVEN** a manually built `LogicalPlan::Filter { predicate: BinaryOp(Column(Utf8), LtEq, Literal(Date32)) }`
- **WHEN** passed through `Analyzer::new(vec![TypeCoercion::new()])`
- **THEN** the output plan's predicate has a `Cast` inserted according to the coercion matrix

