## Context

These three bugs surfaced during the `planner-type-coercion` change's
values-equivalence follow-up (archived task #53). The workflow ran each
TPC-H query through Arneb and Trino on SF1 Hive/MinIO, then diffed the
CSV output cell-by-cell with `compare.py` at relative tolerance
`1e-9`. Twelve of sixteen queries were values-identical; the four
failures (Q07, Q08, Q14, Q16) traced to defects entirely unrelated to
type coercion, so the change deferred them with a `TODO.md` writeup
and the note "Filed for a separate change."

The fixes are small (planner-side resolution + a missing DISTINCT path
in execution) but the discovery cost was high — running the diff
required the full Docker stack and a side-by-side SQL run. Wrapping
the three under a single OpenSpec change lets one verification pass
cover all three and keeps the historical trail honest.

This design records the actual root causes uncovered during
implementation, which in two cases turned out to be more involved than
the original `TODO.md` hypothesis.

## Goals / Non-Goals

**Goals:**
- Bring TPC-H 16/16 queries to values-identical with Trino on SF1
  (relative tolerance 1e-9).
- Add scenarios to `query-planner` and `accumulators` that pin the
  fixed behaviour so future regressions are caught at unit-test time.
- Keep every fix surgical: only the resolution / accumulation code
  paths change. No optimizer, analyzer, or operator-shape changes.

**Non-Goals:**
- The remaining TPC-H queries Q15 / Q17 / Q18 / Q20 / Q21 / Q22 (not
  yet in the benchmark suite) are out of scope. The bench currently
  exercises 16 queries; this change closes that 16-set.
- No write-path or DDL changes, no protocol changes.
- DISTINCT semantics for non-COUNT aggregates: `SUM(DISTINCT x)` and
  `AVG(DISTINCT x)` happen to fall out of the wrapper for free, but
  they are not exercised by TPC-H 1..16, so we leave them as
  unspecified-but-implemented (no spec scenario yet).

## Decisions

### Decision 1 — PB-001: carry the source qualifier through Aggregate

**Symptom.** `SELECT n1.n_name, n2.n_name, COUNT(*) FROM nation n1
JOIN ... JOIN nation n2 ... GROUP BY n1.n_name, n2.n_name` projects
both `n_name`s as the *same* alias's value.

**Root cause.** After planning the `Aggregate` node, the planner
rebuilds `PlanningContext` to reflect post-aggregate columns. Before
this change it set every column's qualifier to `None`:

```rust
ctx = PlanningContext::new();
for col in &schema {
    ctx.columns.push((None, col.clone()));   // <-- qualifier dropped
}
```

`resolve_column` then matched on column name only. For a self-join
both group-by columns share the column name `n_name`, so the SELECT
list's `n1.n_name` and `n2.n_name` both resolved to slot 0
(`AmbiguousReference` should fire, but the resolver returns the first
hit when both qualifier sides are `None`).

**Fix.** When rebuilding the post-aggregate context, attach to each
group-by slot the qualifier extracted from the *original* GROUP BY
AST node (which still carries `n1` / `n2`). Aggregate-output slots
(after the group-by slots) get `None`. A small helper
`group_by_qualifier(&ast::Expr) -> Option<String>` returns the
`col_ref.table.clone()` for qualified column refs and `None`
otherwise.

```rust
ctx = PlanningContext::new();
for (i, col) in schema.iter().enumerate() {
    let qualifier = if i < body.group_by.len() {
        group_by_qualifier(&body.group_by[i])
    } else {
        None
    };
    ctx.columns.push((qualifier, col.clone()));
}
```

**Alternative considered.** Carry the qualifier on the `ColumnInfo`
itself. Rejected: `ColumnInfo` is a connector-facing schema type
shared across the workspace; pinning planner-internal alias info on it
would leak abstraction. The `PlanningContext` already stores
`(Option<String>, ColumnInfo)` tuples for exactly this purpose — the
fix is to populate, not redesign.

**Risk.** Group-by expressions that are not bare column refs (e.g.
`GROUP BY UPPER(t.col)`) get `None`. That's the same as today's
behaviour; no regression. If a future query like `SELECT t.x FROM ...
GROUP BY UPPER(t.x)` needs qualifier preservation, a separate change
can teach `group_by_qualifier` to recurse.

### Decision 2 — PB-002: exact-match using a normalized AST formatter

**Symptom.** `SELECT SUM(CASE..), SUM(t.col) FROM t` projects both
SUMs as the first SUM's value. Also: `SUM(x * (1 - y))` (TPC-H Q14)
fails to find its aggregate slot at all and falls through to
`column_not_found`.

**Root cause.** Two compounding bugs in `find_aggregate_index`:

1. The exact-match arm formatted the AST expression with
   `format!("{expr}")`, which produces `SUM(t.col)` and `SUM(x * (1 -
   y))`. The stored aggregate column name comes from `PlanExpr`
   Display — which is *unqualified* (`SUM(col)`) and *flattens
   parens* (because `plan_expr` unwraps `ast::Expr::Nested`, and
   there is no `PlanExpr::Nested` variant). So exact-match misses.
2. When exact-match missed, the function fell through to a name-prefix
   fallback: "any column whose name starts with `SUM`". With two SUMs
   in the schema, this returned slot 0 for both — explaining the
   collision.

**Fix.** Two changes, both in `crates/planner/src/planner.rs`:

1. Replace `format!("{expr}")` in `find_aggregate_index` with a new
   `format_ast_unqualified(expr)` that mirrors `PlanExpr` Display:
   strips column qualifiers (`t.col` → `col`) and unwraps `Nested`
   nodes. Covers `BinaryOp`, `UnaryOp`, `Function`, `Cast`, `Between`,
   `InList`, `IsNull`, `IsNotNull`, `Case`, `Column`, `Literal`, with
   a Display fallback for variants that don't carry qualifiers
   (subqueries, parameters).
2. Delete the name-prefix fallback entirely. Keep the "exactly one
   aggregate slot in the schema, unambiguous" last-ditch guard,
   because it isn't a name-collision risk and it covers the legitimate
   implicit-aggregate case.

**Why a hand-rolled formatter, not `PlanExpr` Display.** At the call
site we don't yet have a `PlanExpr` — `find_aggregate_index` is asked
to locate the slot *before* the SELECT expression is planned. We
can't plan-then-display because planning depends on the resolution we
are trying to do. A second formatter that targets the same Display
shape is the cheapest correct fix.

**Risk.** The formatter must stay in sync with `PlanExpr` Display.
The relevant variants are stable (TPC-H exercises all of them) and
mismatch surfaces immediately as `column_not_found` at unit-test time,
so drift is observable. Future Display-affecting changes to
`PlanExpr` need a paired update here — a one-line comment in
`format_ast_unqualified` documents the contract.

### Decision 3 — PB-003: thread the `distinct` flag end-to-end

**Symptom.** `COUNT(DISTINCT col)` returns the same value as
`COUNT(col)` — i.e. DISTINCT is a no-op. In TPC-H Q16 the inflation
combines with `ORDER BY supplier_cnt DESC` and the rest of the
output diverges.

**Root cause.** The `distinct` flag is carried correctly in
`PlanExpr::Function { distinct, .. }` from parse through to physical
planning, but two downstream sites drop it:

- `crates/execution/src/operator.rs` — the `AggrInfo` struct
  (planner-to-executor view of an aggregate expression) has fields
  `name / args / is_count_star` but no `distinct` field. The pattern
  match on `PlanExpr::Function` ignores the flag.
- `crates/execution/src/aggregate.rs::create_accumulator` — signature
  is `(func_name, is_count_star) -> Box<dyn Accumulator>`. No
  awareness of `distinct`. All call sites comply.

So even though the rest of the engine "knew" the aggregate was
DISTINCT, the accumulator path silently degraded.

**Fix.** Three coordinated edits:

1. Add `distinct: bool` to `AggrInfo`. Populate it from
   `PlanExpr::Function::distinct` at construction time.
2. Change `create_accumulator(name, is_count_star, distinct)`. Build
   the base accumulator (`CountAccumulator`, `SumAccumulator`,
   `AvgAccumulator`, `MinAccumulator`, `MaxAccumulator`) as before,
   then if `distinct && !is_count_star && upper != "MIN" && upper !=
   "MAX"`, wrap it in `DistinctAccumulator`. (`COUNT(*)`, MIN, and
   MAX are no-ops under DISTINCT.)
3. Implement `DistinctAccumulator`. Internally: a `HashSet<String>`
   of dedup keys + a wrapped `Box<dyn Accumulator>`. `update_batch`
   walks the input array, skips NULLs (SQL semantics: NULLs never
   count under DISTINCT), and forwards only first-seen values to the
   wrapped accumulator via `arrow::compute::take`. `evaluate` and
   `reset` delegate, with `reset` also clearing the `HashSet`.

**Dedup key design.** A `dedup_key(arr, index)` function produces a
type-prefixed string per Arrow scalar:
- `Int32` → `"i32:{value}"`
- `Int64` → `"i64:{value}"`
- `Float32/64` → `"fXX:{value.to_bits()}"` (avoid `NaN==NaN` issues
  and exact-bit identity for floats)
- `Utf8 / LargeUtf8` → `"s:{value}"`
- `Boolean` → `"b:{value}"`
- `Date32 / Date64` → `"dXX:{value}"`
- `Decimal128(p,s)` → `"dec128:{p}:{s}:{raw_i128}"`
- `Timestamp(unit,tz)` → `"ts:{unit:?}:{tz:?}:{raw_i64}"`

The type prefix prevents cross-type collisions (e.g. `Int32(1)` vs
`Int64(1)` vs `Utf8("1")`). Unsupported types return
`ExecutionError::InvalidOperation("DISTINCT not supported for type
{dt:?}")`.

**Alternatives considered.**
- **Use `ScalarValue` as the HashSet key.** `ScalarValue` doesn't
  implement `Hash` for all variants (notably floats), and adding it
  would touch a shared common type. The local string-key approach
  keeps the surface inside `aggregate.rs`.
- **Sort+unique within each batch.** Rejected: misses duplicates
  across batches, which is the common case.
- **Bring in a `hashbrown::HashSet<&[u8]>` of raw array bytes.**
  Faster, but requires per-Arrow-type byte slicing logic that's
  larger than the string-key dispatch. Acceptable to revisit if
  DISTINCT becomes a hot path.

**Risk / cost.** The HashSet grows with cardinality. For TPC-H Q16
the highest-cardinality DISTINCT group is bounded by
`partsupp.ps_suppkey` cardinality per `(p_brand, p_type, p_size)`
group — well under 10k for SF1. Larger workloads could blow memory,
but the existing aggregate path is already memory-bound. Replacing
the string key with a typed `enum`-keyed set is a follow-up if a
benchmark flags it.

## Test Strategy

- **Unit (planner).** Three new tests in
  `crates/planner/src/planner.rs`:
  - `test_self_join_alias_projection_does_not_collapse` — PB-001
  - `test_aggregate_with_nested_parens_resolves` — PB-002 part A
    (`SUM(x*(1-y))` resolves to a Column slot)
  - `test_two_same_function_aggregates_do_not_collide` — PB-002 part
    B (`SUM(CASE..) + SUM(t.col)` resolve to distinct slots, and the
    Aggregate node carries two aggregate exprs)
- **Unit (execution).** Three new tests in
  `crates/execution/src/aggregate.rs` and
  `crates/execution/src/operator.rs`:
  - `count_distinct_over_arrays_drops_duplicates_and_nulls` —
    `DistinctAccumulator` unit
  - `count_distinct_resets_between_groups` — reset semantics
  - `count_distinct_groups_deduplicates` — full `HashAggregateExec`
    integration with `distinct: true`
- **Workspace.** `cargo test --workspace` must remain green.
- **End-to-end.** `trino-diff` skill against SF1 Hive must report
  16/16 queries values-identical at relative tolerance 1e-9.

## Risks

- **PB-002 formatter drift.** If a future PR changes `PlanExpr`
  Display without updating `format_ast_unqualified`, the symptom
  resurfaces as `column_not_found` on benign-looking queries. The
  unit test for nested parens is the canary; the inline comment on
  `format_ast_unqualified` calls this out.
- **PB-001 covers bare column refs only.** Group-by expressions
  involving function calls or arithmetic on a qualified column will
  still drop the qualifier through Aggregate. No current TPC-H query
  triggers this; widening the helper is a separate change.
- **PB-003 string key allocation cost.** Per-row `format!()` is
  measurable on hot DISTINCT paths. Acceptable for SF1 correctness;
  follow-up benchmark gate if SF100 reveals it.

## Migration / Rollout

No migration. All three are internal corrections; no API, wire-
protocol, or persistent-state changes. Rollout: merge the change,
re-run TPC-H 16-set, archive.
