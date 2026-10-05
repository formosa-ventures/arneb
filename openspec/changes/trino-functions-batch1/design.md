## Context

Scalar functions live in `crates/execution/src/functions/` behind the `ScalarFunction` trait and a case-insensitive `FunctionRegistry`. The planner cannot depend on the executor, so the analyzer keeps its own return-type table (`function_return_type` in `crates/planner/src/analyzer/mod.rs`), and `ProjectionExec` casts every evaluated column to that planned type. Literals reach functions as full-length arrays (`NULL` as `NullArray`, integers as `Int64`).

## Decisions

### Single source of truth for return types is enforced by a test

The planner table and each `ScalarFunction::return_type` are kept in agreement by `planner_and_registry_return_types_agree` (execution crate), which plans `PlanExpr::Function` over typed columns and compares against the registry. A mismatch would otherwise silently cast results (e.g. a `DOUBLE` truncated to `BIGINT`). `GREATEST`/`LEAST` share `variadic_supertype` between the two sides for the same reason.

### `IF` in the parser, `TRY` in the evaluator

`IF(c, a, b)` is desugared to `CASE WHEN c THEN a ELSE b END`, like the existing `COALESCE`/`NULLIF` desugaring: it inherits lazy per-branch semantics and CASE type unification for free. `TRY` has to observe its argument's *evaluation errors*, which a registry function (that only sees evaluated arrays) cannot, so `expression::evaluate` special-cases it: evaluate the whole batch; on error, re-evaluate row by row on 1-row slices and NULL the failing rows. The slow path only runs for batches that actually contain an error.

### Nullary functions get the batch length

`ScalarFunction::invoke(args, num_rows)` (default: `evaluate(args)`) is the evaluator's entry point. Nullary functions override it; before this change `current_date` produced a 1-element array regardless of batch size.

### Vectorisation

Arrow kernels are used wherever one exists: `unary`/`binary` for all numeric functions, `date_part` for every date-part function and `EXTRACT`, `starts_with`, `regexp_is_match_scalar` (constant pattern), `cmp` + `zip` for `GREATEST`/`LEAST`, `cast` for argument normalisation. String and calendar functions without a kernel iterate the Arrow buffers directly (no `ScalarValue` boxing) and hoist per-pattern work (regex compilation, format parsing) out of the loop by caching the last pattern.

### Regex dialect

Rust `regex` instead of a Java-compatible engine: guaranteed linear time, already in the dependency tree. Look-around and back-references are rejected with a clear error. Trino replacement strings (`$1`, `${name}`, `\$`) are translated to `regex` syntax so `$1a` keeps its Java meaning (group 1 followed by `a`).

### Time zones

There is no session time zone yet. All timestamp-producing functions return `Timestamp(Microsecond, None)` holding UTC wall-clock time, and date parts are computed in UTC. This is documented as the main semantic difference from Trino.

### `DATE_DIFF` month arithmetic

Month-based units follow Joda-Time (which Trino uses): compute the calendar-month difference, then step back one if adding that many months (with end-of-month clamping) overshoots the end point. So `2024-01-31 → 2024-02-29` is 1 month.

## Risks

- `TRY` catches every evaluation error, a superset of Trino's list. Acceptable: scalar evaluation only raises the classes Trino catches.
- `DATE_PARSE` uses chrono parsing; exotic inputs (e.g. variable-width fractions for `%f`) may be stricter than Trino. Wrap in `TRY` when parsing untrusted data.
