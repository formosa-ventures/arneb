# Design: planner-type-coercion

## Goals

- Align Arneb's semantics with Trino / Spark Catalyst / DuckDB: implicit type coercion happens at plan time; execution sees only pre-aligned types.
- Produce precise, source-located errors for incompatible operands (via `ast-source-spans`).
- Enable predicate pushdown through typed literals by folding `Cast(Literal)` at plan time.
- Lay the architectural seat for future analysis passes (function resolution, subquery decorrelation) by introducing a proper `Analyzer` phase.
- Support extended-query-protocol parameters through forward type inference.

## Non-goals

- **Strict / ANSI mode toggle**: Trino's `system.session.ansi_mode` switch is a follow-up.
- **Full function-signature resolver**: Arneb's current scalar functions have fixed signatures; advanced overload resolution (Trino's `ResolvedFunction`) is a separate change.
- **DuckDB-style cardinality-aware casting**: we don't touch query costing.
- **Implicit numeric-to-string coercion**: Trino only does the reverse. We match that.

## Architecture: introduce an Analyzer phase

### Current pipeline

```
SQL ──► Parser ──► AST ──► QueryPlanner ──► LogicalPlan ──► LogicalOptimizer ──► physical
                              (types not aligned)           (SimplifyFilters,
                                                              ConstantFolding)
```

### New pipeline

```
SQL ──► Parser ──► AST ──► QueryPlanner ──► LogicalPlan ──► Analyzer ──► LogicalPlan ──► LogicalOptimizer ──► physical
                             (raw)                         (TypeCoercion)  (aligned)      (rewrites for perf)
```

### Analyzer module layout

```
crates/planner/src/analyzer/
├── mod.rs                 # AnalysisPass trait, Analyzer pipeline runner
├── type_coercion.rs       # The TypeCoercion pass
├── coercion_matrix.rs     # Data-driven matrix + supertype lookup
├── param_inference.rs     # Forward type propagation for $n params
└── walker.rs              # Shared plan/expression traversal helpers
```

```rust
pub trait AnalysisPass: Send + Sync {
    fn name(&self) -> &'static str;
    fn analyze(&self, plan: LogicalPlan, ctx: &mut AnalyzerContext) -> Result<LogicalPlan, PlanError>;
}

pub struct Analyzer {
    passes: Vec<Box<dyn AnalysisPass>>,
}

impl Analyzer {
    pub fn default_pipeline() -> Self {
        Self {
            passes: vec![
                Box::new(TypeCoercion::new()),
                // Box::new(FunctionResolution::new()),       // future
                // Box::new(SubqueryDecorrelation::new()),    // future
            ],
        }
    }

    pub fn run(&self, mut plan: LogicalPlan, ctx: &mut AnalyzerContext) -> Result<LogicalPlan, PlanError> {
        for pass in &self.passes {
            plan = pass.analyze(plan, ctx)?;
        }
        Ok(plan)
    }
}

pub struct AnalyzerContext {
    pub param_types: HashMap<ParamId, DataType>,     // inferred during this run
    // Reserved for future passes: symbol tables, function registry, session state
}
```

Analysis passes differ from `LogicalRule` (the optimizer's trait) deliberately: they may fail (returning `PlanError`) because they verify semantic correctness; optimizer rules by contract preserve semantics and never introduce new errors.

## Coercion matrix

Encoded as a const lookup table. Each entry:

```rust
struct CoercionRule {
    from: TypeId,      // source data type (pattern or exact)
    to: TypeId,        // target data type
    safety: Safety,
    cast_kind: CastKind,
}

pub enum Safety {
    AlwaysSafe,        // column↔column OK: no data loss
    LiteralOnly,       // only when source is a (folded) literal
    PrecisionLoss,     // numeric widening with possible precision loss (e.g., Int64 → Float64)
}
```

### Rules (MVP matrix)

| From | To | Safety | Notes |
|---|---|---|---|
| `Int32` | `Int64` | AlwaysSafe | widening |
| `Int32` | `Float64` | PrecisionLoss | large Int32 loses precision — still allowed, Trino matches |
| `Int64` | `Float64` | PrecisionLoss | same |
| `Int32`, `Int64` | `Decimal(p, s)` | AlwaysSafe when `p - s ≥ 10 (Int32) or 19 (Int64)` | else PrecisionLoss |
| `Float32` | `Float64` | AlwaysSafe | widening |
| `Float*` | `Decimal(p, s)` | PrecisionLoss | Trino allows |
| `Decimal(p1, s1)` | `Decimal(p2, s2)` | AlwaysSafe when `p2 ≥ p1 && s2 ≥ s1` | else PrecisionLoss |
| `Utf8` | `Date32` | LiteralOnly | parses YYYY-MM-DD |
| `Utf8` | `Timestamp(unit, tz)` | LiteralOnly | parses ISO-8601 |
| Any T | T | AlwaysSafe | identity |

### Common supertype function

```rust
fn common_supertype(a: &DataType, b: &DataType, ctx: CoercionSite) -> Option<DataType>;
```

- `CoercionSite::Binary { is_literal: (bool, bool) }` — determines if `LiteralOnly` rules apply
- `CoercionSite::CaseBranch` — both arms treated as column-like (LiteralOnly disallowed between arms, only for literal → column promotions)
- `CoercionSite::UnionColumn` — strict matching, column-column only

Supertype algorithm: for each type pair, walk the matrix forward and back; pick the smallest common target. Same algorithm Trino uses internally (`TypeCoercion::getCommonSuperType`).

### Asymmetric literal safety (D4)

Critical correctness design: `Utf8 → Date32` is **never** applied to a column. Concretely:

```
Column(Utf8) <= Column(Date32)    ──► PlanError: cannot apply operator '<=' to Utf8 and Date32
Column(Utf8) <= Literal(Utf8)     ──► same type, no coercion
Column(Date32) <= Literal(Utf8)    ──► LiteralOnly: rewrite to Column(Date32) <= Cast(Literal(Utf8), Date32)
```

Detection of "literal side": `PlanExpr::Literal(_)` OR `PlanExpr::Cast { expr: Literal, .. }` OR any expression that `ConstantFolding` would reduce to a literal. The matrix is queried with `(type_a, type_b, is_a_literal, is_b_literal)`.

## Constant folding extension

Current `ConstantFolding` (in `crates/planner/src/optimizer.rs`) handles arithmetic on literals. Extend it to:

```rust
fn fold_cast(expr: PlanExpr) -> PlanExpr {
    match expr {
        PlanExpr::Cast { expr: inner, data_type, span } => {
            match *inner {
                PlanExpr::Literal { value, .. } => {
                    match cast_scalar(&value, &data_type) {
                        Ok(new_value) => PlanExpr::Literal { value: new_value, span },
                        Err(_) => PlanExpr::Cast { expr: Box::new(inner_literal(value)), data_type, span },
                    }
                }
                other => PlanExpr::Cast { expr: Box::new(other), data_type, span },
            }
        }
        other => other,
    }
}
```

`cast_scalar` uses Arrow's `cast_with_options` against a single-element array, captures any parse error, and emits a `PlanError` at plan time (so an invalid `DATE '1998-13-45'` fails early instead of mid-query).

Post-folding, the plan tree for `l_shipdate <= DATE '1998-12-01'` looks like:

```
BinaryOp(LtEq,
    Column { name: "l_shipdate", type: Date32 },
    Literal { value: Date32(10561), span: ... }    ← was Cast(Literal(Utf8), Date32)
)
```

`parquet_pushdown::extract_column_literal_comparison` already matches `(Column, Literal)`, so pushdown + row-group pruning work without changes.

## Execution simplification (D5: delete runtime hack)

After coercion, `compare_op` and `arithmetic_op` receive pre-aligned arrays. `coerce_numeric_pair` and `wider_numeric_type` become unreachable dead code.

Deletion plan:

1. Land analyzer + coercion matrix + ConstantFolding extension.
2. Run full test suite + TPC-H both paths + fuzz tests (if any).
3. If green, delete `coerce_numeric_pair` and `wider_numeric_type`.
4. Re-run tests as a regression guard.
5. If anything breaks, it exposes a coercion-matrix gap we must fix in the analyzer rather than re-add the runtime hack.

This is the user's explicit preference — test-gated deletion, not belt-and-suspenders.

## Parameter type inference (D8: progressive)

When a query contains `$n` placeholders (extended query protocol `Parse` message), the planner produces `PlanExpr::Parameter { index: n, type_hint: Option<DataType> }`. The analyzer resolves types:

```
WHERE l_shipdate <= $1
                    │
                    ▼ analyzer sees sibling is Date32
                    │
                    ▼ AnalyzerContext.param_types[1] = Date32
                    │
                    ▼ rewrite to PlanExpr::Parameter { index: 1, type: Some(Date32) }
```

Rules:

- If `$n` is a BinaryOp operand, unify with the other side's type (if known).
- If `$n` is an IN list element, unify with the tested expression's type.
- If `$n` is an argument to a function with a known signature, unify with the parameter's expected type.
- If unresolvable after one pass, default to `Utf8` (matching Trino's `unknown` → `varchar` fallback).

The resolved `param_types` map is returned to the pgwire layer so `ParameterDescription` can send concrete OIDs.

## Error format

Every `TypeMismatch` error includes `location: Option<Location>` populated from the offending `PlanExpr::best_span()`:

```rust
PlanError::TypeMismatch {
    op: "<=",
    left_type: DataType::Utf8,
    right_type: DataType::Date32,
    location: plan_expr.best_span().map(|s| s.start),
}
```

When `ast-source-spans` merges, `render_plan_error` turns this into:

```
error: cannot apply operator '<=' to Utf8 and Date32
  ┌─ query.sql:3:19
  │
3 │   WHERE l_shipdate <= DATE '1998-12-01'
  │                    ^^ here
  │
  = hint: columns of type Utf8 and Date32 cannot be compared directly;
           consider CAST(l_shipdate AS DATE) if the column stores date-formatted strings
```

## Traversal strategy

Single top-down walk of `LogicalPlan`; at each operator:

1. Recurse into input plans first (so their output schemas are type-stable).
2. Compute input schema (already a concept: `LogicalPlan::schema()`).
3. For each expression on this operator, walk it post-order; at each node needing type agreement, apply coercion.
4. Rewrite the operator with the new expressions; compute its output schema.

Failure at any step short-circuits the walk with the error carrying the most-derived span.

## Open questions

- **Decimal arithmetic policy**: `Decimal(p1,s1) + Decimal(p2,s2)` — Trino uses `Decimal(min(38, max(p1-s1, p2-s2) + max(s1,s2) + 1), max(s1,s2))`. Adopt verbatim or simplify? The existing `wider_numeric_type` already implements this; we carry the formula into the coercion matrix.
- **Parameter inference ambiguity**: `SELECT $1 + 1` — is `$1` Int32, Int64, Decimal, or Float? Proposal: pick Int32 as default, override if sibling constrains tighter. Document as known limitation.
- **Deferred fuzz coverage**: should we ship coercion without proptest coverage and add it next? I'd say yes — proptest for type inference is a separate change.

## Migration and risk

- **Risk of over-strict coercion**: some Arneb queries relied on the runtime hack's forgiving behavior. MVP ships with a **comprehensive test suite** before deleting the hack (task list gates deletion behind regression sign-off).
- **Risk of under-folding**: if `ConstantFolding` misses a `Cast(Literal)` case, pushdown degrades to FilterExec (slower but correct). Add unit tests for every literal type.
- **Rollback plan**: if production regressions surface, `Analyzer::default_pipeline()` can be replaced with `Analyzer::empty()` temporarily while fixes land — the old runtime hack remains until final cleanup (reversed order of deletion vs. rollout).

## Rejected alternatives

- **Optimizer rule (`TypeCoercion: LogicalRule`)**: rejected. Coercion is analysis; it may fail. Optimizer rules by convention preserve semantics and never error. Forcing coercion into the optimizer muddies the contract.
- **Inline coercion in `plan_expr`**: rejected. Mixes structural AST→Plan translation with semantic type alignment; `plan_expr` is already 1732-line territory, adding more concerns hurts reviewability.
- **Runtime coercion expansion**: rejected explicitly by user direction (and Trino architecture). Runtime coercion hides bugs, duplicates effort per batch, and produces position-free errors.

## Dependency on ast-source-spans

- **Hard dep**: no. Coercion ships with `Option<Location>` populated from `PlanExpr::best_span()`; values may be `None` until the span change plumbs spans into `PlanExpr`.
- **Soft dep**: error rendering quality. Without `ast-source-spans`, errors fall back to the `thiserror` `Display` output (position-free, but with column names and types). Both changes can proceed in parallel; final error polish happens when both are merged.
