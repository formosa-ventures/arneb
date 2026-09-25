## ADDED Requirements

### Requirement: Batch-aware invocation
`ScalarFunction` SHALL provide `invoke(&self, args: &[ArrayRef], num_rows: usize)`, defaulting to `evaluate(args)`. The expression evaluator SHALL call `invoke` with the batch row count so that nullary functions produce one value per row.

#### Scenario: Nullary function length
- **WHEN** `NOW()` is evaluated against a 3-row batch
- **THEN** the result has 3 rows

### Requirement: Function aliases
`FunctionRegistry` SHALL provide `register_alias(alias, target)` so one implementation can be looked up under several names. The default registry SHALL register `CEILING`, `POW`, `RAND`, `DAY_OF_MONTH`, `DOW`, `DOY`, `WEEK_OF_YEAR`, `YOW`, `CURRENT_TIMESTAMP`, `LOCALTIMESTAMP`.

#### Scenario: Alias lookup
- **WHEN** `get("dow")` is called on the default registry
- **THEN** it returns the `DAY_OF_WEEK` function

### Requirement: Planner and registry return types agree
For every built-in scalar function, the planner's `function_return_type` SHALL return the same type as `ScalarFunction::return_type` for the same argument types. This SHALL be enforced by a unit test in the execution crate.

#### Scenario: Agreement test
- **WHEN** `planner_and_registry_return_types_agree` runs
- **THEN** every sampled `(function, argument types)` pair yields identical planned and executed types
