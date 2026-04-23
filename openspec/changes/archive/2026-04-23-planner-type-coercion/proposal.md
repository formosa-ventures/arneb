## Why

Arneb's type handling today is a runtime band-aid:

- `crates/execution/src/expression.rs::wider_numeric_type` silently widens numeric operands at evaluation time, but knows nothing about `Utf8↔Date32`, `Utf8↔Timestamp`, `Decimal↔Int` or `Decimal↔Float` combinations that appear in real TPC-H / TPC-DS / user-submitted SQL.
- When types don't fit its matrix, it produces a generic `cannot coerce X and Y to a common type` error — with no location, no hint, no chance to fail at plan time instead of mid-stream.
- `PlanExpr::Cast { Literal(Utf8), Date32 }` nodes (produced by native `DATE '...'` literals) are not constant-folded, so parquet predicate pushdown cannot see through them and row-group pruning is lost.
- CASE/COALESCE/IN/UNION all have no type reconciliation logic; multi-branch queries either work by accident (all arms same type) or crash at runtime.

Trino, Spark Catalyst, DuckDB, and PostgreSQL all solve this the same way: a **planner-level Analyzer phase** applies an *implicit coercion matrix* that rewrites the logical plan by inserting explicit `Cast` nodes. Failures surface at plan time with precise source locations. Execution sees only already-aligned types and contains zero coercion logic.

Arneb needs the same architecture — not as a bolt-on to the existing optimizer (analysis is not optimization), but as a dedicated Analyzer phase that lives between `QueryPlanner` and `LogicalOptimizer`. This also creates the seat for future analysis passes (function resolution, subquery decorrelation, constant-type narrowing) that Arneb will need as it grows.

This change delivers two things:

1. The **Analyzer phase infrastructure** — a pluggable sequence of analysis passes the planner runs before optimization.
2. The **Type Coercion pass** — the first (and most impactful) analyzer pass, with a coercion matrix matching Trino's semantics.

As a side effect it extends `ConstantFolding` to fold `Cast(Literal)` into a pre-typed literal, deletes the runtime `wider_numeric_type` hack, and adds parameter-type inference so extended-query-protocol placeholders participate in coercion.

## What Changes

- Introduce a new `Analyzer` module under `crates/planner/src/analyzer/` with an `AnalysisPass` trait and an ordered pipeline. `QueryPlanner::plan_query` runs the full analyzer pipeline before handing the plan to `LogicalOptimizer`.
- Implement `TypeCoercion` as the first pass. It walks `LogicalPlan` and `PlanExpr` trees and, at every site where operand types must agree (`BinaryOp`, `UnaryOp`, `Between`, `InList`, `CaseExpr`, `Function`, `JoinCondition`, set-op column alignment), inserts `PlanExpr::Cast` nodes according to the coercion matrix.
- Define the **coercion matrix** (data-driven lookup) covering:
  - Numeric widening: `Int32↔Int64`, `Int↔Float`, `Int↔Decimal`, `Float↔Decimal`, `Decimal(p1,s1)↔Decimal(p2,s2)` common supertype
  - Date/Time from string literals: `Utf8→Date32`, `Utf8→Timestamp(unit,tz)` — **literal-side only** (asymmetric safety)
  - Identity/reflexive
- Classify every coercion as `Safety::AlwaysSafe` (column-column allowed) or `Safety::LiteralOnly` (only when the source operand is a literal or folded-literal). Column-column conversions that lose information are rejected at plan time.
- Coverage includes **CASE / COALESCE / NULLIF** multi-branch supertype unification, **IN list** element unification, **UNION / INTERSECT / EXCEPT** column-wise unification, and **function argument** coercion keyed on a per-function signature table.
- Coverage includes **arithmetic** (`+ - * / %`) coercion — TPC-H's `price * (1 - discount)` exercises this on every query.
- Extend `ConstantFolding` optimizer rule to fold `Cast(Literal(_), target_type)` into `Literal(target_type(...))` when Arrow `cast_with_options` supports a compile-time cast. Predicate pushdown (`parquet_pushdown.rs::extract_column_literal_comparison`) then matches the folded form and row-group pruning works for date/timestamp predicates.
- Delete `wider_numeric_type` and the `coerce_numeric_pair` helper from `crates/execution/src/expression.rs` once the full regression suite passes. Execution `compare_op` / `arithmetic_op` operate on pre-aligned types only.
- Add **parameter-type inference**: when the extended-query protocol binds `$n`, propagate types forward through the analyzer. MVP approach: unify `$n` with the type of its sibling expression in the surrounding operator. Report inferred parameter types back via the pgwire `ParameterDescription` message.
- Plan-time errors use `Location` from `ast-source-spans` (soft dependency — coercion pass ships with `Option<Location>` populated from `PlanExpr::best_span()`; rich rendering activates once that change lands).

## Capabilities

### New Capabilities

- `analyzer-phase`: pluggable analysis-pass pipeline between `QueryPlanner` and `LogicalOptimizer`.
- `type-coercion`: implicit-cast matrix + rewrites that align operand types before optimization.
- `param-type-inference`: forward type propagation for extended-query-protocol parameters.

### Modified Capabilities

- `query-planner` (`crates/planner/src/planner.rs`): `plan_query` invokes the analyzer pipeline.
- `logical-plan` (`crates/planner/src/plan.rs`): `PlanExpr` tree walkers extended for analyzer traversal.
- `expression-evaluator` (`crates/execution/src/expression.rs`): `wider_numeric_type` and `coerce_numeric_pair` deleted; `compare_op` / `arithmetic_op` assume pre-aligned types.
- `pg-messages` (`crates/protocol`): `ParameterDescription` uses inferred types from the analyzer.

## Impact

- **Behavior change (correctness)**: queries that relied on runtime coercion for unsupported pairs now succeed at plan time instead of crashing at runtime. Queries that were semantically invalid but accidentally compiled now fail at plan time with clear errors — test suites will need to update any such assertions.
- **Behavior change (errors)**: every coercion-related runtime error becomes a plan-time `PlanError::TypeMismatch` with `Location`, renderable as a rustc-style diagnostic once `ast-source-spans` merges.
- **Behavior change (performance)**: predicate pushdown through folded casts enables row-group pruning on date/timestamp predicates — measurable speedup on TPC-H Q06, Q12, Q14, Q19.
- **Deletions**: `wider_numeric_type`, `coerce_numeric_pair` removed from `crates/execution/src/expression.rs`. Any downstream code calling these (none exists today) would break.
- **Dependency**: soft-depends on `ast-source-spans` for rich error rendering. Coercion ships with `Option<Location>` unconditionally; rich format activates when the span change is merged.
- **Out of scope — deferred to future changes**:
  - Full function-signature type inference (e.g., `GREATEST`, `LEAST`, `CASE` WHEN-result returning varying types — the MVP uses a minimal supertype rule)
  - Decimal scale/precision widening policy (TPC-H standard uses DECIMAL(15,2); we pick pragmatic defaults, reserve a spec rule for the policy)
  - Strict vs. lax mode (Trino's ANSI mode toggle) — MVP only ships lax mode matching Trino's default
