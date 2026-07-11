//! Cost-based join-reorder analyzer pass.
//!
//! Walks the [`LogicalPlan`] looking for inner-join chains and rewrites
//! them so the smallest filtered cardinality leaf joins first (greedy
//! Selinger-style heuristic). The pass is conservative: any plan shape it
//! cannot confidently rewrite — outer joins, non-equi conditions, joins
//! with subqueries inline, multi-leaf predicates — is returned unchanged.
//!
//! Behaviour:
//! - `LogicalPlan::Join { join_type: Inner, .. }` chains are flattened
//!   into a `(leaves, edges)` graph (where each edge is one `On(expr)`
//!   atom from the original tree's conditions, possibly AND-split).
//! - Leaves are reordered greedily by ascending estimated cardinality;
//!   tie-broken by lower NDV product on the join key.
//! - The plan is re-emitted as a left-deep tree.
//! - Column indices are re-resolved by name against each join's new
//!   input schema (left ++ right).
//! - If at any step we cannot place an edge (e.g., dangling predicate),
//!   the pass aborts the rewrite and returns the original sub-plan.
//!
//! The pass also recurses into subquery sub-plans (`ScalarSubquery`,
//! `InSubquery`, `ExistsSubquery`) so they are reordered independently.

use arneb_common::error::PlanError;
use arneb_common::types::{ColumnInfo, TableReference};
use arneb_sql_parser::ast::{BinaryOp, JoinType};

use crate::analyzer::{AnalysisPass, AnalyzerContext, Hint};
use crate::cost::{
    estimated_cardinality, selective_dim_first_enabled, selinger_cost, CatalogStats,
    SELECTIVE_DIM_CHAIN_SMALL_ROWS, SELECTIVE_DIM_TINY_FILTER_ROWS,
};
use crate::plan::{JoinCondition, LogicalPlan, PlanExpr};

const SELECTIVE_DIM_PREFACT_MAX_ROWS: u64 = 1_000_000;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Selinger-style cost-based join reorderer.
///
/// Runs after `TypeCoercion` in the analyzer pipeline. Conservative when
/// unsure — never produces a worse plan than the input under the cost model.
#[derive(Debug)]
pub struct JoinReorder {
    config: ReorderConfig,
}

/// Tuning knobs for the join reorderer.
#[derive(Debug, Clone)]
pub struct ReorderConfig {
    /// Maximum number of tables for which full enumeration is attempted.
    /// Beyond this, the greedy heuristic is used. v1 always uses greedy.
    pub dp_max_tables: usize,
    /// Fallback row count when a `TableScan` has no statistics.
    pub default_table_size: u64,
}

impl Default for ReorderConfig {
    fn default() -> Self {
        Self {
            dp_max_tables: 8,
            default_table_size: 10_000,
        }
    }
}

/// Annotation produced by the reorderer, attached to the new plan via
/// a side-channel on `AnalyzerContext` (added in Phase 2.7 for
/// `EXPLAIN ANALYZE` rendering).
#[derive(Debug, Default, Clone)]
pub struct ReorderAnnotation {
    /// Whether reordering changed the plan.
    pub applied: bool,
    /// Tables in the order they appeared in the original SQL.
    pub original_order: Vec<TableReference>,
    /// Tables in the order chosen by the reorderer.
    pub chosen_order: Vec<TableReference>,
}

impl JoinReorder {
    /// Construct with default tuning.
    pub fn new() -> Self {
        Self {
            config: ReorderConfig::default(),
        }
    }

    /// Construct with custom tuning.
    pub fn with_config(config: ReorderConfig) -> Self {
        Self { config }
    }
}

impl Default for JoinReorder {
    fn default() -> Self {
        Self::new()
    }
}

impl AnalysisPass for JoinReorder {
    fn name(&self) -> &'static str {
        "JoinReorder"
    }

    fn analyze(
        &self,
        plan: LogicalPlan,
        ctx: &mut AnalyzerContext,
    ) -> Result<LogicalPlan, PlanError> {
        // Keep a pristine copy of the input plan. The reorder threads each
        // reordered chain's leaf-origin old→new column permutation up through
        // the consuming operators (see `transform_remap`), so parent
        // Projection/Filter/Sort/Aggregate refs AND the reordered join
        // conditions remap by leaf-origin — never by name (name lookup can't
        // disambiguate a self-join's duplicate columns, e.g. TPC-H Q07/Q08
        // `nation n1`/`nation n2`, which is how the F-Perf-RN attempt silently
        // misrouted `n2.n_name`). `indices_consistent` is the safety net: if
        // any column ref fails to resolve to a same-named column in its
        // operator's input schema, the rewrite produced an inconsistent tree
        // and we fall back to this pristine snapshot rather than ship it.
        let original = plan.clone();
        let reordered = if ctx.hints.contains(Hint::NoReorder) {
            recurse_subqueries_only(plan, &ctx.catalog_stats, &self.config)
        } else {
            transform_remap(plan, &ctx.catalog_stats, &self.config).0
        };
        if indices_consistent(&reordered) {
            Ok(reordered)
        } else {
            Ok(original)
        }
    }
}

// ---------------------------------------------------------------------------
// Core walker
// ---------------------------------------------------------------------------

fn transform(plan: LogicalPlan, stats: &CatalogStats, config: &ReorderConfig) -> LogicalPlan {
    // Thin wrapper for call sites that don't need the permutation table
    // (nested-leaf reorder, subquery reorder): the table is meaningful only
    // to the operator that DIRECTLY consumes the reordered chain's output,
    // and those call sites re-establish a fresh column space anyway.
    transform_remap(plan, stats, config).0
}

/// `orig_index -> new_index` lookup for a subtree whose output columns are a
/// permutation of an earlier (original) layout. `table[oi] = ni`; length ==
/// the subtree's output schema width. Built leaf-origin when a chain reorders;
/// propagated/composed upward so the operator that consumes the chain output
/// remaps its column refs by ORIGINAL index (unambiguous even for self-join
/// duplicate names) instead of by name.
type RemapTable = Vec<usize>;

/// Reorder inner-join chains and thread each chain's leaf-origin permutation
/// up to the consuming operator. Returns the rewritten plan plus an optional
/// remap table: `Some` when THIS subtree's output is a permuted reordered-chain
/// output (a parent that references these columns must remap through it),
/// `None` when no permutation escapes (e.g. a Projection/Aggregate re-emits a
/// fresh column space and seals the permutation).
fn transform_remap(
    plan: LogicalPlan,
    stats: &CatalogStats,
    config: &ReorderConfig,
) -> (LogicalPlan, Option<RemapTable>) {
    if let Some((rewritten, table)) = try_reorder_chain(&plan, stats, config) {
        return (rewritten, Some(table));
    }
    recurse_remap(plan, stats, config)
}

/// Apply a child's remap table to one expression. Column indices present in
/// the table are remapped to their new position; indices beyond the table
/// (e.g. a Window/AssignUniqueId column appended past the permuted input,
/// whose width is preserved by the permutation) keep their value. `None`
/// table is a no-op.
fn remap_one(expr: PlanExpr, table: &Option<RemapTable>) -> PlanExpr {
    match table {
        Some(t) => rewrite_expr(&expr, &mut |_name, idx| {
            Some(t.get(idx).copied().unwrap_or(idx))
        })
        .expect("identity remap never aborts"),
        None => expr,
    }
}

fn remap_exprs(exprs: Vec<PlanExpr>, table: &Option<RemapTable>) -> Vec<PlanExpr> {
    match table {
        Some(_) => exprs.into_iter().map(|e| remap_one(e, table)).collect(),
        None => exprs,
    }
}

/// Compose left and right child remap tables into the join's combined
/// (left ++ right) output space. Original combined index `oi < lw` lives in
/// the left child (→ `left[oi]`); otherwise in the right child (→ `lw +
/// right[oi - lw]`). Reorder preserves each side's width, so `lw` is both the
/// original and new left width. Returns `None` when neither side permuted.
fn compose_join_table(
    left: &Option<RemapTable>,
    lw: usize,
    right: &Option<RemapTable>,
    rw: usize,
) -> Option<RemapTable> {
    if left.is_none() && right.is_none() {
        return None;
    }
    let mut out = Vec::with_capacity(lw + rw);
    for i in 0..lw {
        out.push(left.as_ref().and_then(|t| t.get(i).copied()).unwrap_or(i));
    }
    for j in 0..rw {
        out.push(lw + right.as_ref().and_then(|t| t.get(j).copied()).unwrap_or(j));
    }
    Some(out)
}

/// Single-step recursion that threads remap tables. Mirrors `recurse_children`
/// but, when a child returns a permutation, remaps THIS node's own column refs
/// through it and decides what to expose upward:
///   - schema-preserving (Filter/Sort/Limit/Distinct/Explain/Window/
///     AssignUniqueId): remap own refs, PROPAGATE the child's table (output
///     columns stay permuted; appended columns keep their index);
///   - schema-producing (Projection/Aggregate/*Aggregate): remap own refs,
///     SEAL with `None` (output is a fresh column space);
///   - Join: compose left/right tables, remap the condition, expose the
///     composed table;
///   - Semi/Anti join: remap keys/residual, expose the LEFT table (output is
///     the left schema);
///   - set ops / subquery / DML wrappers: seal each branch (valid SQL projects
///     every arm, so no permutation escapes a stacking operator).
fn recurse_remap(
    plan: LogicalPlan,
    stats: &CatalogStats,
    config: &ReorderConfig,
) -> (LogicalPlan, Option<RemapTable>) {
    use LogicalPlan as L;
    let tr = |p: LogicalPlan| transform_remap(p, stats, config);
    match plan {
        L::TableScan { .. }
        | L::CreateTable { .. }
        | L::DropTable { .. }
        | L::DeleteFrom { .. }
        | L::DropView { .. }
        | L::ExchangeNode { .. }
        | L::OneRow => (plan, None),

        L::Projection {
            input,
            exprs,
            schema,
        } => {
            let (input, t) = tr(*input);
            let exprs = remap_exprs(exprs, &t);
            (
                L::Projection {
                    input: Box::new(input),
                    exprs,
                    schema,
                },
                None,
            )
        }

        L::Filter { input, predicate } => {
            let (input, t) = tr(*input);
            let predicate = remap_one(predicate, &t);
            (
                L::Filter {
                    input: Box::new(input),
                    predicate,
                },
                t,
            )
        }

        L::Sort { input, order_by } => {
            let (input, t) = tr(*input);
            let order_by = order_by
                .into_iter()
                .map(|s| crate::plan::SortExpr {
                    expr: remap_one(s.expr, &t),
                    ..s
                })
                .collect();
            (
                L::Sort {
                    input: Box::new(input),
                    order_by,
                },
                t,
            )
        }

        L::Limit {
            input,
            limit,
            offset,
        } => {
            let (input, t) = tr(*input);
            (
                L::Limit {
                    input: Box::new(input),
                    limit,
                    offset,
                },
                t,
            )
        }

        L::Explain { input, analyze } => {
            let (input, t) = tr(*input);
            (
                L::Explain {
                    input: Box::new(input),
                    analyze,
                },
                t,
            )
        }

        L::Distinct { input } => {
            let (input, t) = tr(*input);
            (
                L::Distinct {
                    input: Box::new(input),
                },
                t,
            )
        }

        L::Aggregate {
            input,
            group_by,
            aggr_exprs,
            schema,
        } => {
            let (input, t) = tr(*input);
            let group_by = remap_exprs(group_by, &t);
            let aggr_exprs = remap_exprs(aggr_exprs, &t);
            (
                L::Aggregate {
                    input: Box::new(input),
                    group_by,
                    aggr_exprs,
                    schema,
                },
                None,
            )
        }

        L::PartialAggregate {
            input,
            group_by,
            aggr_exprs,
            schema,
        } => {
            let (input, t) = tr(*input);
            let group_by = remap_exprs(group_by, &t);
            let aggr_exprs = remap_exprs(aggr_exprs, &t);
            (
                L::PartialAggregate {
                    input: Box::new(input),
                    group_by,
                    aggr_exprs,
                    schema,
                },
                None,
            )
        }

        L::FinalAggregate {
            input,
            group_by,
            aggr_exprs,
            schema,
        } => {
            let (input, t) = tr(*input);
            let group_by = remap_exprs(group_by, &t);
            let aggr_exprs = remap_exprs(aggr_exprs, &t);
            (
                L::FinalAggregate {
                    input: Box::new(input),
                    group_by,
                    aggr_exprs,
                    schema,
                },
                None,
            )
        }

        L::Window { input, functions } => {
            let (input, t) = tr(*input);
            let functions = functions
                .into_iter()
                .map(|mut f| {
                    f.args = remap_exprs(f.args, &t);
                    f.partition_by = remap_exprs(f.partition_by, &t);
                    f.order_by = f
                        .order_by
                        .into_iter()
                        .map(|s| crate::plan::SortExpr {
                            expr: remap_one(s.expr, &t),
                            ..s
                        })
                        .collect();
                    f
                })
                .collect();
            // Window APPENDS result columns to its (possibly permuted) input;
            // those columns keep their index (width preserved), so the input
            // table — read with `unwrap_or(idx)` for appended indices —
            // resolves both regions. Propagate it.
            (
                L::Window {
                    input: Box::new(input),
                    functions,
                },
                t,
            )
        }

        L::Join {
            left,
            right,
            join_type,
            condition,
            ..
        } => {
            let (left, lt) = tr(*left);
            let (right, rt) = tr(*right);
            let lw = left.schema().len();
            let rw = right.schema().len();
            let combined = compose_join_table(&lt, lw, &rt, rw);
            let condition = match condition {
                JoinCondition::On(expr) => JoinCondition::On(remap_one(expr, &combined)),
                JoinCondition::None => JoinCondition::None,
            };
            (
                L::Join {
                    left: Box::new(left),
                    right: Box::new(right),
                    join_type,
                    condition,
                    dynamic_filter_ids: Vec::new(),
                },
                combined,
            )
        }

        L::SemiJoin {
            left,
            right,
            left_key,
            right_key,
            residual,
            ..
        } => {
            let (left, lt) = tr(*left);
            let (right, rt) = tr(*right);
            let lw = left.schema().len();
            let rw = right.schema().len();
            let left_key = remap_one(left_key, &lt);
            let right_key = remap_one(right_key, &rt);
            let residual = residual.map(|r| remap_one(r, &compose_join_table(&lt, lw, &rt, rw)));
            // Semi join emits LEFT rows → output is the left schema.
            (
                L::SemiJoin {
                    left: Box::new(left),
                    right: Box::new(right),
                    left_key,
                    right_key,
                    residual,
                    dynamic_filter_ids: Vec::new(),
                },
                lt,
            )
        }

        L::AntiJoin {
            left,
            right,
            left_key,
            right_key,
            residual,
        } => {
            let (left, lt) = tr(*left);
            let (right, rt) = tr(*right);
            let lw = left.schema().len();
            let rw = right.schema().len();
            let left_key = remap_one(left_key, &lt);
            let right_key = remap_one(right_key, &rt);
            let residual = residual.map(|r| remap_one(r, &compose_join_table(&lt, lw, &rt, rw)));
            (
                L::AntiJoin {
                    left: Box::new(left),
                    right: Box::new(right),
                    left_key,
                    right_key,
                    residual,
                },
                lt,
            )
        }

        L::ScalarSubquery { subplan } => (
            L::ScalarSubquery {
                subplan: Box::new(tr(*subplan).0),
            },
            None,
        ),

        L::UnionAll { inputs } => (
            L::UnionAll {
                inputs: inputs.into_iter().map(|i| tr(i).0).collect(),
            },
            None,
        ),

        L::Intersect { left, right } => (
            L::Intersect {
                left: Box::new(tr(*left).0),
                right: Box::new(tr(*right).0),
            },
            None,
        ),

        L::Except { left, right } => (
            L::Except {
                left: Box::new(tr(*left).0),
                right: Box::new(tr(*right).0),
            },
            None,
        ),

        L::CreateTableAsSelect { name, source } => (
            L::CreateTableAsSelect {
                name,
                source: Box::new(tr(*source).0),
            },
            None,
        ),

        L::InsertInto { table, source } => (
            L::InsertInto {
                table,
                source: Box::new(tr(*source).0),
            },
            None,
        ),

        L::CreateView { name, sql, plan } => (
            L::CreateView {
                name,
                sql,
                plan: Box::new(tr(*plan).0),
            },
            None,
        ),

        L::AssignUniqueId { input, id_column } => {
            let (input, t) = tr(*input);
            // Appends an id column past the (possibly permuted) input; its
            // index is preserved, so propagating the input table is correct.
            (
                L::AssignUniqueId {
                    input: Box::new(input),
                    id_column,
                },
                t,
            )
        }
    }
}

/// Variant of `transform` that does NOT reorder the immediate inner-join
/// chain (it honors `NO_REORDER`) but still recurses into subquery
/// sub-plans so they get reordered separately.
fn recurse_subqueries_only(
    plan: LogicalPlan,
    stats: &CatalogStats,
    config: &ReorderConfig,
) -> LogicalPlan {
    map_expr_subqueries_in_plan(plan, &mut |sub| transform(sub, stats, config))
}

/// Generic single-step recursion that maps `f` over each child plan and
/// reconstructs the parent. Also recurses into subquery sub-plans inside
/// `PlanExpr::ScalarSubquery` predicates by name.
fn recurse_children<F>(plan: LogicalPlan, f: &mut F) -> LogicalPlan
where
    F: FnMut(LogicalPlan) -> LogicalPlan,
{
    use LogicalPlan as L;
    match plan {
        L::TableScan { .. }
        | L::CreateTable { .. }
        | L::DropTable { .. }
        | L::DeleteFrom { .. }
        | L::DropView { .. }
        | L::ExchangeNode { .. }
        | L::OneRow => plan,

        L::Projection {
            input,
            exprs,
            schema,
        } => L::Projection {
            input: Box::new(f(*input)),
            exprs,
            schema,
        },
        L::Filter { input, predicate } => L::Filter {
            input: Box::new(f(*input)),
            predicate,
        },
        L::Sort { input, order_by } => L::Sort {
            input: Box::new(f(*input)),
            order_by,
        },
        L::Limit {
            input,
            limit,
            offset,
        } => L::Limit {
            input: Box::new(f(*input)),
            limit,
            offset,
        },
        L::Explain { input, analyze } => L::Explain {
            input: Box::new(f(*input)),
            analyze,
        },
        L::Distinct { input } => L::Distinct {
            input: Box::new(f(*input)),
        },
        L::Window { input, functions } => L::Window {
            input: Box::new(f(*input)),
            functions,
        },
        L::Aggregate {
            input,
            group_by,
            aggr_exprs,
            schema,
        } => L::Aggregate {
            input: Box::new(f(*input)),
            group_by,
            aggr_exprs,
            schema,
        },
        L::PartialAggregate {
            input,
            group_by,
            aggr_exprs,
            schema,
        } => L::PartialAggregate {
            input: Box::new(f(*input)),
            group_by,
            aggr_exprs,
            schema,
        },
        L::FinalAggregate {
            input,
            group_by,
            aggr_exprs,
            schema,
        } => L::FinalAggregate {
            input: Box::new(f(*input)),
            group_by,
            aggr_exprs,
            schema,
        },
        L::Join {
            left,
            right,
            join_type,
            condition,
            ..
        } => L::Join {
            left: Box::new(f(*left)),
            right: Box::new(f(*right)),
            join_type,
            condition,
            dynamic_filter_ids: Vec::new(),
        },
        L::SemiJoin {
            left,
            right,
            left_key,
            right_key,
            residual,
            ..
        } => L::SemiJoin {
            left: Box::new(f(*left)),
            right: Box::new(f(*right)),
            left_key,
            right_key,
            residual,
            dynamic_filter_ids: Vec::new(),
        },
        L::AntiJoin {
            left,
            right,
            left_key,
            right_key,
            residual,
        } => L::AntiJoin {
            left: Box::new(f(*left)),
            right: Box::new(f(*right)),
            left_key,
            right_key,
            residual,
        },
        L::ScalarSubquery { subplan } => L::ScalarSubquery {
            subplan: Box::new(f(*subplan)),
        },
        L::UnionAll { inputs } => L::UnionAll {
            inputs: inputs.into_iter().map(f).collect(),
        },
        L::Intersect { left, right } => L::Intersect {
            left: Box::new(f(*left)),
            right: Box::new(f(*right)),
        },
        L::Except { left, right } => L::Except {
            left: Box::new(f(*left)),
            right: Box::new(f(*right)),
        },
        L::CreateTableAsSelect { name, source } => L::CreateTableAsSelect {
            name,
            source: Box::new(f(*source)),
        },
        L::InsertInto { table, source } => L::InsertInto {
            table,
            source: Box::new(f(*source)),
        },
        L::CreateView { name, sql, plan } => L::CreateView {
            name,
            sql,
            plan: Box::new(f(*plan)),
        },
        L::AssignUniqueId { input, id_column } => L::AssignUniqueId {
            input: Box::new(f(*input)),
            id_column,
        },
    }
}

/// Walks every `PlanExpr::ScalarSubquery { subplan }` inside the
/// expressions attached to `plan` and applies `f` to each `subplan`.
/// This is the entry-point for honoring `NO_REORDER` while still
/// reordering subqueries (which are independent statements semantically).
fn map_expr_subqueries_in_plan<F>(plan: LogicalPlan, f: &mut F) -> LogicalPlan
where
    F: FnMut(LogicalPlan) -> LogicalPlan,
{
    // For v1 we keep this simple: only the subquery field of
    // `LogicalPlan::ScalarSubquery` is descended; expression-level
    // subqueries inside Filter/Projection predicates flow through the
    // normal `recurse_children` path under `NO_REORDER` because their
    // outer container does not reorder anyway.
    match plan {
        LogicalPlan::ScalarSubquery { subplan } => LogicalPlan::ScalarSubquery {
            subplan: Box::new(f(*subplan)),
        },
        other => recurse_children(other, &mut |c| map_expr_subqueries_in_plan(c, f)),
    }
}

// ---------------------------------------------------------------------------
// Chain detection and rewrite
// ---------------------------------------------------------------------------

/// Returns the reordered plan when `plan` is the root of a contiguous
/// inner-join chain we know how to rewrite, or `None` otherwise.
fn try_reorder_chain(
    plan: &LogicalPlan,
    stats: &CatalogStats,
    config: &ReorderConfig,
) -> Option<(LogicalPlan, RemapTable)> {
    // The root of a reorderable chain is a Join whose join_type is Inner.
    if !matches!(
        plan,
        LogicalPlan::Join {
            join_type: JoinType::Inner,
            ..
        }
    ) {
        return None;
    }

    let mut leaves: Vec<LogicalPlan> = Vec::new();
    let mut edges: Vec<PlanExpr> = Vec::new();
    if !flatten_inner_chain(plan, &mut leaves, &mut edges) {
        return None;
    }

    if leaves.len() < 2 {
        return None;
    }

    // Duplicate column names across leaves (a self-join, e.g. TPC-H Q07/Q08
    // `nation n1`/`nation n2`) used to bail here because the old name-based
    // re-resolution couldn't disambiguate them. We now resolve every column
    // by LEAF-ORIGIN (original combined index → `(leaf, pos)` → new index),
    // which is unambiguous regardless of names — so self-joins reorder, and
    // the permutation is threaded to the consuming operator via the returned
    // `RemapTable` (see `transform_remap`).

    // Reorder each leaf in case it contains its own nested inner-join
    // chain that survived the boundary detection (e.g., inner inside
    // outer-join's right side). This is conservative — calling
    // `transform` again on each leaf is a no-op when there's no chain.
    let leaves: Vec<LogicalPlan> = leaves
        .into_iter()
        .map(|leaf| transform(leaf, stats, config))
        .collect();

    let original_cost = estimated_cardinality(plan, stats);

    // Selinger DP for small chains; greedy fallback for big ones.
    let mut order = if leaves.len() <= config.dp_max_tables {
        selinger_order(&leaves, &edges, stats)
            .unwrap_or_else(|| greedy_order(&leaves, &edges, stats))
    } else {
        greedy_order(&leaves, &edges, stats)
    };
    if selective_dim_first_enabled() {
        if let Some(selective_order) = selective_dim_chain_order(&leaves, &edges, stats) {
            let selective_prefact_max =
                left_deep_prefact_max_intermediate(&selective_order, &leaves, &edges, stats);
            if selective_prefact_max < SELECTIVE_DIM_PREFACT_MAX_ROWS as f64 {
                order = selective_order;
            }
        }
    }

    // Diagnostic (2026-06-09, q08 join-order): dump the per-leaf
    // post-filter cardinality the cost model sees and the chosen order,
    // so we can tell whether a selective dimension (e.g. q08 `part`
    // p_type=1/150) is being scored as selective or not. Gated by the
    // same env the coordinator's FRAGTRACE uses.
    if std::env::var("ARNEB_TRACE_FRAGMENTS")
        .map(|v| v == "1")
        .unwrap_or(false)
    {
        eprintln!(
            "[REORDERTRACE] chain leaves={} order={order:?}",
            leaves.len()
        );
        for (i, leaf) in leaves.iter().enumerate() {
            eprintln!(
                "[REORDERTRACE]   leaf[{i}] {} card={:.0} unfiltered={:.0}",
                leaf_table_label(leaf),
                estimated_cardinality(leaf, stats),
                estimated_cardinality(&strip_filters(leaf), stats),
            );
        }
        // Compare the DP's chosen order against the variant that swaps the
        // 2nd/3rd leaves (for q08: lineitem,orders,part -> lineitem,part,
        // orders). Prints each order's selinger_cost and per-join output
        // card, to expose whether the NDV fallback inflates the
        // part-first intermediate.
        if let Some(chosen_plan) = emit_left_deep(&leaves, &edges, &order) {
            eprintln!(
                "[REORDERTRACE]   CHOSEN order={order:?} selinger_cost={:.0}",
                selinger_cost(&chosen_plan, stats)
            );
            dump_join_outputs(&chosen_plan, stats, "CHOSEN");
            if order.len() >= 3 {
                let mut alt = order.clone();
                alt.swap(1, 2);
                if let Some(alt_plan) = emit_left_deep(&leaves, &edges, &alt) {
                    eprintln!(
                        "[REORDERTRACE]   ALT    order={alt:?} selinger_cost={:.0}",
                        selinger_cost(&alt_plan, stats)
                    );
                    dump_join_outputs(&alt_plan, stats, "ALT");
                }
            }
        }
    }

    // Re-emit a left-deep tree.
    let rewritten = emit_left_deep(&leaves, &edges, &order)?;

    let new_cost = estimated_cardinality(&rewritten, stats);
    if new_cost > original_cost + 0.5 {
        // The chosen plan has worse OUTPUT cardinality than the
        // textual order — for inner joins this can only happen when
        // missing stats made the estimate flaky; bail out
        // conservatively.
        return None;
    }

    // Leaf-origin permutation over the chain's full combined width: original
    // combined index `oi` → `new_offsets[leaf] + pos`. The consuming operator
    // remaps its refs through this (`transform_remap`). Identity when the
    // chosen order equals the original.
    let widths = leaf_widths(&leaves);
    let orig_off = cumulative_offsets(&widths);
    let new_off = new_offsets(&order, &widths);
    let total: usize = widths.iter().sum();
    let table: RemapTable = (0..total)
        .map(|oi| {
            let (leaf, pos) =
                leaf_of_orig_index(oi, &orig_off, &widths).expect("orig index within chain width");
            new_off[leaf] + pos
        })
        .collect();

    Some((rewritten, table))
}

/// Diagnostic helper (q08 join-order): short label naming the table(s)
/// under a reorder leaf, for the `[REORDERTRACE]` dump.
fn leaf_table_label(plan: &LogicalPlan) -> String {
    use LogicalPlan as L;
    match plan {
        L::TableScan { table, .. } => table.to_string(),
        L::Filter { input, .. }
        | L::Projection { input, .. }
        | L::Sort { input, .. }
        | L::Limit { input, .. }
        | L::Aggregate { input, .. }
        | L::PartialAggregate { input, .. }
        | L::FinalAggregate { input, .. }
        | L::Distinct { input }
        | L::Window { input, .. }
        | L::AssignUniqueId { input, .. } => leaf_table_label(input),
        L::Join { left, right, .. } => {
            format!(
                "({} join {})",
                leaf_table_label(left),
                leaf_table_label(right)
            )
        }
        _ => "?".to_string(),
    }
}

/// Diagnostic helper (q08 join-order): print each join level's estimated
/// output cardinality (deepest first) plus the build-side label, so the
/// running-intermediate blow-up from a mis-estimated NDV is visible.
fn dump_join_outputs(plan: &LogicalPlan, stats: &CatalogStats, tag: &str) {
    if let LogicalPlan::Join { left, right, .. } = plan {
        dump_join_outputs(left, stats, tag);
        eprintln!(
            "[REORDERTRACE]     {tag} join build={} out={:.0}",
            leaf_table_label(right),
            estimated_cardinality(plan, stats),
        );
    }
}

fn left_deep_prefact_max_intermediate(
    order: &[usize],
    leaves: &[LogicalPlan],
    edges: &[PlanExpr],
    stats: &CatalogStats,
) -> f64 {
    let Some((fact_idx, _)) = leaves.iter().enumerate().max_by(|(_, a), (_, b)| {
        estimated_cardinality(a, stats)
            .partial_cmp(&estimated_cardinality(b, stats))
            .unwrap_or(std::cmp::Ordering::Equal)
    }) else {
        return f64::INFINITY;
    };

    let mut max_intermediate = 0.0_f64;
    let mut prefix = Vec::new();
    for &leaf_idx in order {
        if leaf_idx == fact_idx {
            return max_intermediate;
        }
        prefix.push(leaf_idx);
        if prefix.len() >= 2 {
            let Some(plan) = emit_left_deep(leaves, edges, &prefix) else {
                return f64::INFINITY;
            };
            max_intermediate = max_intermediate.max(estimated_cardinality(&plan, stats));
        }
    }
    f64::INFINITY
}

/// Diagnostic helper (q08 join-order): drop `Filter` wrappers (which are
/// schema-preserving, so column indices stay valid) so
/// `estimated_cardinality` reports the leaf's UNFILTERED scan size. The
/// gap between filtered and unfiltered card reveals whether the reorder
/// cost model is actually applying each leaf's filter selectivity.
fn strip_filters(plan: &LogicalPlan) -> LogicalPlan {
    use LogicalPlan as L;
    match plan {
        L::Filter { input, .. } => strip_filters(input),
        L::Projection {
            input,
            exprs,
            schema,
        } => L::Projection {
            input: Box::new(strip_filters(input)),
            exprs: exprs.clone(),
            schema: schema.clone(),
        },
        other => other.clone(),
    }
}

#[derive(Debug, Clone)]
struct SelectiveDimChain {
    prefix: Vec<usize>,
    output_card: f64,
}

/// Detects a multi-hop selective dimension chain and anchors it before the
/// large fact joins. This is intentionally narrower than the general Selinger
/// scorer: start at a leaf filtered to <=10 rows, walk only join-connected
/// prefixes whose intermediate output stays dimension-sized, then accept the
/// first large output as the propagated selective dimension subtree.
fn selective_dim_chain_order(
    leaves: &[LogicalPlan],
    edges: &[PlanExpr],
    stats: &CatalogStats,
) -> Option<Vec<usize>> {
    let n = leaves.len();
    if !(3..=12).contains(&n) {
        return None;
    }
    let widths = leaf_widths(leaves);
    let orig_off = cumulative_offsets(&widths);
    let edge_refs: Vec<Vec<usize>> = edges
        .iter()
        .map(|edge| leaves_referenced_by_index(edge, &orig_off, &widths))
        .collect();

    let leaf_cards: Vec<f64> = leaves
        .iter()
        .map(|leaf| estimated_cardinality(leaf, stats))
        .collect();
    let mut chains = Vec::new();
    for root in 0..n {
        let filtered = leaf_cards[root];
        if filtered > SELECTIVE_DIM_TINY_FILTER_ROWS {
            continue;
        }
        let unfiltered = estimated_cardinality(&strip_filters(&leaves[root]), stats);
        if unfiltered <= filtered {
            continue;
        }
        find_selective_dim_chains(
            leaves,
            edges,
            stats,
            &edge_refs,
            &leaf_cards,
            vec![root],
            &mut chains,
        );
    }

    chains
        .into_iter()
        .filter_map(|chain| {
            complete_selective_dim_order(leaves, edges, stats, chain.prefix)
                .map(|order| (order, chain.output_card))
        })
        .min_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
        .map(|(order, _)| order)
}

fn find_selective_dim_chains(
    leaves: &[LogicalPlan],
    edges: &[PlanExpr],
    stats: &CatalogStats,
    edge_refs: &[Vec<usize>],
    leaf_cards: &[f64],
    path: Vec<usize>,
    out: &mut Vec<SelectiveDimChain>,
) {
    const MAX_CHAIN_LEAVES: usize = 5; // 2-4 joins.
    for candidate in 0..leaves.len() {
        if path.contains(&candidate) || !connected_to_prefix(edge_refs, &path, candidate) {
            continue;
        }
        let mut prefix = path.clone();
        prefix.push(candidate);
        let Some(plan) = emit_left_deep(leaves, edges, &prefix) else {
            continue;
        };
        let output = estimated_cardinality(&plan, stats);
        if prefix.len() >= 3
            && output > SELECTIVE_DIM_CHAIN_SMALL_ROWS
            && leaf_cards[candidate] > SELECTIVE_DIM_CHAIN_SMALL_ROWS
        {
            out.push(SelectiveDimChain {
                prefix,
                output_card: output,
            });
            continue;
        }
        if output <= SELECTIVE_DIM_CHAIN_SMALL_ROWS && prefix.len() < MAX_CHAIN_LEAVES {
            find_selective_dim_chains(leaves, edges, stats, edge_refs, leaf_cards, prefix, out);
        }
    }
}

fn complete_selective_dim_order(
    leaves: &[LogicalPlan],
    edges: &[PlanExpr],
    stats: &CatalogStats,
    mut order: Vec<usize>,
) -> Option<Vec<usize>> {
    let widths = leaf_widths(leaves);
    let orig_off = cumulative_offsets(&widths);
    let edge_refs: Vec<Vec<usize>> = edges
        .iter()
        .map(|edge| leaves_referenced_by_index(edge, &orig_off, &widths))
        .collect();
    while order.len() < leaves.len() {
        let next = (0..leaves.len())
            .filter(|candidate| !order.contains(candidate))
            .filter(|candidate| connected_to_prefix(&edge_refs, &order, *candidate))
            .min_by(|&a, &b| {
                let cost_a = candidate_cost(leaves, edges, stats, &order, a);
                let cost_b = candidate_cost(leaves, edges, stats, &order, b);
                cost_a
                    .partial_cmp(&cost_b)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })?;
        order.push(next);
    }
    emit_left_deep(leaves, edges, &order).map(|_| order)
}

fn connected_to_prefix(edge_refs: &[Vec<usize>], prefix: &[usize], candidate: usize) -> bool {
    edge_refs
        .iter()
        .any(|refs| refs.contains(&candidate) && refs.iter().any(|leaf| prefix.contains(leaf)))
}

/// Flattens a contiguous inner-join chain rooted at `plan` into `leaves`
/// (one per non-Join leaf) and `edges` (one per AND-atom in each Join's
/// `On(expr)` condition). Returns `false` if a non-inner Join or a
/// non-equi join condition (`JoinCondition::None`) is encountered,
/// signalling to caller that no rewrite should be attempted.
fn flatten_inner_chain(
    plan: &LogicalPlan,
    leaves: &mut Vec<LogicalPlan>,
    edges: &mut Vec<PlanExpr>,
) -> bool {
    match plan {
        LogicalPlan::Join {
            left,
            right,
            join_type: JoinType::Inner,
            condition,
            ..
        } => {
            match condition {
                JoinCondition::On(expr) => split_and_atoms(expr, edges),
                JoinCondition::None => return false,
            }
            if !flatten_inner_chain(left, leaves, edges) {
                return false;
            }
            if !flatten_inner_chain(right, leaves, edges) {
                return false;
            }
            true
        }
        _ => {
            leaves.push(plan.clone());
            true
        }
    }
}

fn split_and_atoms(expr: &PlanExpr, sink: &mut Vec<PlanExpr>) {
    match expr {
        PlanExpr::BinaryOp {
            left,
            op: BinaryOp::And,
            right,
            ..
        } => {
            split_and_atoms(left, sink);
            split_and_atoms(right, sink);
        }
        _ => sink.push(expr.clone()),
    }
}

// ---------------------------------------------------------------------------
// Selinger DP ordering
// ---------------------------------------------------------------------------

/// Selinger-style left-deep DP. Enumerates every connected left-deep
/// ordering of `leaves` and picks the one minimising the
/// **partition-aware** Selinger cost (see [`selinger_cost`] in
/// `cost.rs`): probe/output costs divided by the outermost leaf's
/// estimated partition count, build costs sequential.
///
/// Returns `None` if no left-deep ordering is build-able — caller
/// should fall back to the greedy heuristic. The bitmask
/// representation limits this to `leaves.len() <= 12` in practice
/// (caller gates via `ReorderConfig::dp_max_tables`).
fn selinger_order(
    leaves: &[LogicalPlan],
    edges: &[PlanExpr],
    stats: &CatalogStats,
) -> Option<Vec<usize>> {
    let n = leaves.len();
    if n == 0 || n > 12 {
        return None;
    }

    // dp[bitmask] = Some((order, total_selinger_cost_for_left_deep_prefix)).
    // We store the COST OF THE FULL PLAN built from the prefix
    // (re-computed via `selinger_cost` on every extension) rather than
    // an incrementally-accumulated cost, because Selinger cost depends
    // on the OUTERMOST leaf's partition count — different prefixes
    // expose different outermost leaves, so per-step accumulation
    // wouldn't compose.
    let mut dp: Vec<Option<(Vec<usize>, f64)>> = vec![None; 1usize << n];

    // Singletons: cost = leaf scan / its own partitions.
    for i in 0..n {
        let cost = selinger_cost(&leaves[i], stats);
        dp[1usize << i] = Some((vec![i], cost));
    }

    // Increasing subset size.
    for bitmask in 1usize..(1usize << n) {
        let size = (bitmask as u32).count_ones() as usize;
        if size < 2 {
            continue;
        }
        let mut best: Option<(Vec<usize>, f64)> = None;
        for j in 0..n {
            if bitmask & (1usize << j) == 0 {
                continue;
            }
            let prefix_mask = bitmask ^ (1usize << j);
            let Some((prefix_order, _)) = dp[prefix_mask].clone() else {
                continue;
            };
            let mut candidate_order = prefix_order;
            candidate_order.push(j);
            let Some(plan) = emit_left_deep(leaves, edges, &candidate_order) else {
                // No edge connects {j} to the prefix → Cartesian split.
                continue;
            };
            let total = selinger_cost(&plan, stats);
            match &best {
                None => best = Some((candidate_order, total)),
                Some((_, c)) if total < *c => best = Some((candidate_order, total)),
                _ => {}
            }
        }
        dp[bitmask] = best;
    }

    dp[(1usize << n) - 1]
        .as_ref()
        .map(|(order, _)| order.clone())
}

// ---------------------------------------------------------------------------
// Greedy ordering
// ---------------------------------------------------------------------------

/// Greedy left-deep ordering — fallback used when DP can't run
/// (`leaves.len() > dp_max_tables`, or `selinger_order` returns `None`
/// because no left-deep ordering connects all leaves).
///
/// Pre-2026-05-26 this minimised intermediate result size (classic
/// Selinger heuristic): pick the smallest leaf, then at each step
/// extend by the next smallest. That treats LEFT and RIGHT
/// symmetrically — and for hash joins it gets the asymmetry wrong:
/// a "smallest first" left-deep tree puts the largest leaf on the
/// outermost RIGHT, where it becomes the build side. For Q09-shape
/// queries (one fact + many dims) that's a 6M-row hash build, the
/// exact pattern Trino's broadcast-eligibility check would flag.
///
/// New behaviour: at each step, score every (prefix + candidate)
/// extension with [`selinger_cost`] — the same partition-aware
/// build/probe-asymmetric cost the DP path uses — and pick the
/// minimum. This makes greedy a faithful 1-step lookahead of the DP,
/// instead of a different scoring philosophy. The DP itself is
/// unchanged.
///
/// Cost: O(n²) `emit_left_deep` + `selinger_cost` calls (n is the
/// leaf count; greedy fires only for n > 8 in practice). Each call
/// walks the partial tree once, so total is O(n³) leaf visits.
fn greedy_order(leaves: &[LogicalPlan], edges: &[PlanExpr], stats: &CatalogStats) -> Vec<usize> {
    let n = leaves.len();
    let mut order: Vec<usize> = Vec::with_capacity(n);
    let mut chosen = vec![false; n];

    // Anchor: pick the leaf with the LARGEST cardinality. In a
    // left-deep tree the leftmost leaf is the deepest probe — it
    // never appears on a `right` (build) child. For hash join, where
    // the right side is materialised in a hash table, the biggest
    // leaf is the one we MOST want to keep off the build side. The
    // pre-2026-05-26 heuristic anchored with the SMALLEST leaf
    // (classic Selinger "minimise intermediate result size"); that
    // optimises for the row-store join cost model, not hash join,
    // and exposed Q09-shape queries (one fact + many dims) to a
    // 6M-row build at the outermost level.
    //
    // For Selinger DP (`selinger_order`) this asymmetry is captured
    // by scoring every full ordering with `selinger_cost`; greedy
    // can only see one step at a time, so the anchor heuristic
    // carries the asymmetry that 1-step lookahead misses.
    let card: Vec<f64> = leaves
        .iter()
        .map(|l| estimated_cardinality(l, stats))
        .collect();
    let first = (0..n)
        .max_by(|&a, &b| {
            card[a]
                .partial_cmp(&card[b])
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .expect("at least one leaf");
    order.push(first);
    chosen[first] = true;

    for _ in 1..n {
        let next = (0..n)
            .filter(|i| !chosen[*i])
            .min_by(|&a, &b| {
                let cost_a = candidate_cost(leaves, edges, stats, &order, a);
                let cost_b = candidate_cost(leaves, edges, stats, &order, b);
                cost_a
                    .partial_cmp(&cost_b)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .expect("at least one unchosen leaf");
        order.push(next);
        chosen[next] = true;
    }

    order
}

/// Score `(prefix + [candidate])` as a left-deep plan with
/// [`selinger_cost`]. Candidates that don't share an equi-join edge
/// with the prefix get `f64::INFINITY`, so [`greedy_order`] always
/// prefers a connected extension when one exists.
fn candidate_cost(
    leaves: &[LogicalPlan],
    edges: &[PlanExpr],
    stats: &CatalogStats,
    prefix: &[usize],
    candidate: usize,
) -> f64 {
    let mut probe_order = prefix.to_vec();
    probe_order.push(candidate);
    match emit_left_deep(leaves, edges, &probe_order) {
        Some(plan) => selinger_cost(&plan, stats),
        None => f64::INFINITY,
    }
}

// ---------------------------------------------------------------------------
// Re-emit left-deep with re-resolved column indices
// ---------------------------------------------------------------------------

/// Re-emits the chain as a left-deep tree of inner joins in `order`,
/// re-attaching edges to the deepest join where both sides of each atom
/// are in the accumulated leaves so far. Returns `None` if some edge
/// cannot be placed (signalling the caller to give up the rewrite).
fn emit_left_deep(
    leaves: &[LogicalPlan],
    edges: &[PlanExpr],
    order: &[usize],
) -> Option<LogicalPlan> {
    // Leaf-origin layout: resolve column indices by (leaf, pos) so a chain
    // containing a self-join (duplicate column names) reorders correctly,
    // instead of bailing / misrouting on name collisions.
    let widths = leaf_widths(leaves);
    let orig_off = cumulative_offsets(&widths);
    let new_off = new_offsets(order, &widths);

    // Track per-edge which leaves it references (by leaf-origin).
    let edge_leaves: Vec<Vec<usize>> = edges
        .iter()
        .map(|e| leaves_referenced_by_index(e, &orig_off, &widths))
        .collect();

    // For each edge: at what point in the new order are all its leaves
    // accumulated? That's where the edge must attach.
    //
    //   attach_at[e] = max(order.position(leaf) for leaf in edge_leaves[e])
    //
    // If `edge_leaves[e]` is empty (no recognizable column refs), we
    // cannot place the edge confidently — give up.
    //
    // Edges that reference leaves not in `order` are SKIPPED rather
    // than failing the build. This lets the Selinger DP call this
    // function with PARTIAL prefix orderings: an edge that crosses
    // the prefix boundary just won't be placed at this prefix's cost
    // computation; the future DP step that adds the missing leaf
    // will place it. The final emit_left_deep call (with the full
    // order) catches dropped edges via `edges_left.is_empty()` at
    // the bottom of this function.
    let mut attach_at: Vec<Option<usize>> = Vec::with_capacity(edges.len());
    for refs in &edge_leaves {
        if refs.is_empty() {
            return None;
        }
        let mut pos_max = 0usize;
        let mut any_outside = false;
        for &leaf_idx in refs {
            match order.iter().position(|&i| i == leaf_idx) {
                Some(pos) => pos_max = pos_max.max(pos),
                None => {
                    any_outside = true;
                    break;
                }
            }
        }
        if any_outside {
            attach_at.push(None);
            continue;
        }
        // Edges that only touch the first leaf cannot be a join atom —
        // they should have been a Filter, not a join condition. Skip
        // gracefully.
        if pos_max == 0 {
            // Anchor it at the next join (pos 1) so the predicate
            // survives evaluation.
            pos_max = 1;
        }
        attach_at.push(Some(pos_max));
    }

    // Build the tree.
    let mut current = leaves[order[0]].clone();
    let mut current_schema = current.schema();
    // Track which edges still need to be attached. Edges that
    // reference leaves outside `order` (i.e., partial-prefix case
    // for Selinger DP) start out with `attach_at == None` and are
    // never placed by this call — they're considered "deferred"
    // and the caller (DP) will handle them in a future emit_left_deep
    // call that includes those leaves.
    let mut edges_left: Vec<usize> = (0..edges.len())
        .filter(|&e| attach_at[e].is_some())
        .collect();

    for (step, &leaf_idx) in order.iter().enumerate().skip(1) {
        let right = leaves[leaf_idx].clone();
        let right_schema = right.schema();

        // Gather edges that attach at this step.
        let mut atoms = Vec::new();
        let mut still_left = Vec::new();
        for e in edges_left.iter().copied() {
            if attach_at[e] == Some(step) {
                atoms.push(rebuild_column_indices(
                    &edges[e], &orig_off, &widths, &new_off,
                )?);
            } else {
                still_left.push(e);
            }
        }
        edges_left = still_left;

        let condition = if atoms.is_empty() {
            // No atoms attach here. For partial-prefix Selinger DP
            // calls this is acceptable IFF an upstream future call
            // (with more leaves) will pick this edge up. We emit a
            // Cartesian-style Join here so cost estimation still
            // proceeds — the final full-order emit_left_deep call
            // catches genuinely-dropped edges via `edges_left`
            // remaining non-empty at the end.
            //
            // To avoid silent Cartesian explosion in the FINAL plan,
            // we still fail when `order.len() == leaves.len()` and a
            // step lacks atoms.
            if order.len() == leaves.len() {
                return None;
            }
            JoinCondition::None
        } else {
            JoinCondition::On(combine_with_and(atoms))
        };

        let joined_schema = {
            let mut s = current_schema.clone();
            s.extend(right_schema.iter().cloned());
            s
        };
        current = LogicalPlan::Join {
            left: Box::new(current),
            right: Box::new(right),
            join_type: JoinType::Inner,
            condition,
            dynamic_filter_ids: Vec::new(),
        };
        current_schema = joined_schema;
    }

    // For the FINAL emit (full ordering), every edge must have been
    // consumed; otherwise some predicate would be silently dropped.
    // For partial prefixes (DP intermediate), only the in-prefix
    // edges were required and `edges_left` here tracks edges whose
    // `attach_at` was `Some(step)` for steps beyond `order.len()` —
    // but since we build only up to `order.len()`, those should
    // already be drained by the loop above. A non-empty residual
    // means a real bug.
    if !edges_left.is_empty() {
        return None;
    }

    Some(current)
}

fn combine_with_and(mut atoms: Vec<PlanExpr>) -> PlanExpr {
    if atoms.len() == 1 {
        return atoms.pop().unwrap();
    }
    let mut iter = atoms.into_iter();
    let first = iter.next().unwrap();
    iter.fold(first, |acc, e| PlanExpr::BinaryOp {
        left: Box::new(acc),
        op: BinaryOp::And,
        right: Box::new(e),
        span: None,
    })
}

// ---------------------------------------------------------------------------
// Leaf-origin column layout (self-join-robust, name-independent)
//
// Every join-condition / predicate `Column { index, .. }` carries the
// ORIGINAL combined-schema position. Because the original tree is left-deep
// and indices count from the left, a single `orig_offsets` map (leaves in
// collected order) resolves any original index to `(leaf, pos_in_leaf)` —
// unambiguously, even when two leaves share column names (self-join). After
// reordering, `new_offsets` gives each leaf's start in the new combined
// schema, so `new_index = new_offsets[leaf] + pos`. This replaces the old
// NAME-based resolution that bailed (or misrouted) on duplicate names.
// ---------------------------------------------------------------------------

/// Column count of each leaf, in original (collected) order.
fn leaf_widths(leaves: &[LogicalPlan]) -> Vec<usize> {
    leaves.iter().map(|l| l.schema().len()).collect()
}

/// Cumulative start offset of each leaf in the original combined schema.
fn cumulative_offsets(widths: &[usize]) -> Vec<usize> {
    let mut offs = Vec::with_capacity(widths.len());
    let mut acc = 0usize;
    for &w in widths {
        offs.push(acc);
        acc += w;
    }
    offs
}

/// Map an original combined-schema index to `(leaf, pos_in_leaf)`.
fn leaf_of_orig_index(
    orig_index: usize,
    orig_offsets: &[usize],
    widths: &[usize],
) -> Option<(usize, usize)> {
    for i in 0..widths.len() {
        if orig_index >= orig_offsets[i] && orig_index < orig_offsets[i] + widths[i] {
            return Some((i, orig_index - orig_offsets[i]));
        }
    }
    None
}

/// Start offset of each leaf in the NEW combined schema produced by `order`.
/// `new_offsets[leaf]` = sum of widths of leaves placed before it in `order`.
/// Leaves absent from a partial `order` keep offset 0 (their edges are
/// deferred by `emit_left_deep`'s `attach_at` logic, so this is never read).
fn new_offsets(order: &[usize], widths: &[usize]) -> Vec<usize> {
    let mut offs = vec![0usize; widths.len()];
    let mut acc = 0usize;
    for &leaf in order {
        offs[leaf] = acc;
        acc += widths[leaf];
    }
    offs
}

/// The leaves an edge references, by leaf-origin of its column indices
/// (name-independent). Replaces the name-based `leaves_referenced`.
fn leaves_referenced_by_index(
    edge: &PlanExpr,
    orig_offsets: &[usize],
    widths: &[usize],
) -> Vec<usize> {
    let mut set = std::collections::BTreeSet::new();
    visit_column_indices(edge, &mut |oi| {
        if let Some((leaf, _)) = leaf_of_orig_index(oi, orig_offsets, widths) {
            set.insert(leaf);
        }
    });
    set.into_iter().collect()
}

/// Visit every `PlanExpr::Column`'s original index in `expr`.
fn visit_column_indices<F: FnMut(usize)>(expr: &PlanExpr, f: &mut F) {
    let _ = rewrite_expr(expr, &mut |_name: &str, idx: usize| {
        f(idx);
        Some(idx)
    });
}

/// Re-resolves every `PlanExpr::Column { index, .. }` in `expr` from its
/// original combined index to its new combined index by leaf-origin.
/// A reference outside the reorderable leaves' range keeps its original
/// index (parity with the old name-miss fallback — not a chain column).
fn rebuild_column_indices(
    expr: &PlanExpr,
    orig_offsets: &[usize],
    widths: &[usize],
    new_offs: &[usize],
) -> Option<PlanExpr> {
    let mut remap = |_name: &str, old_idx: usize| -> Option<usize> {
        match leaf_of_orig_index(old_idx, orig_offsets, widths) {
            Some((leaf, pos)) => Some(new_offs[leaf] + pos),
            None => Some(old_idx),
        }
    };
    rewrite_expr(expr, &mut remap)
}

/// Structural walk that re-resolves every `PlanExpr::Column { index, name }`
/// via `remap(name, old_index) -> Option<new_index>`. Everything else is
/// cloned/recursed unchanged. `remap` returning `None` aborts the rewrite.
/// Both the name-based and the leaf-origin (index-based) resolvers share
/// this walk — only the `remap` closure differs.
fn rewrite_expr<F>(expr: &PlanExpr, remap: &mut F) -> Option<PlanExpr>
where
    F: FnMut(&str, usize) -> Option<usize>,
{
    use PlanExpr as E;
    Some(match expr {
        E::Column { name, span, index } => {
            let new_idx = remap(name, *index)?;
            E::Column {
                index: new_idx,
                name: name.clone(),
                span: *span,
            }
        }
        E::Literal { value, span } => E::Literal {
            value: value.clone(),
            span: *span,
        },
        E::BinaryOp {
            left,
            op,
            right,
            span,
        } => E::BinaryOp {
            left: Box::new(rewrite_expr(left, remap)?),
            op: *op,
            right: Box::new(rewrite_expr(right, remap)?),
            span: *span,
        },
        E::UnaryOp { op, expr, span } => E::UnaryOp {
            op: *op,
            expr: Box::new(rewrite_expr(expr, remap)?),
            span: *span,
        },
        E::IsNull { expr, span } => E::IsNull {
            expr: Box::new(rewrite_expr(expr, remap)?),
            span: *span,
        },
        E::IsNotNull { expr, span } => E::IsNotNull {
            expr: Box::new(rewrite_expr(expr, remap)?),
            span: *span,
        },
        E::Between {
            expr,
            negated,
            low,
            high,
            span,
        } => E::Between {
            expr: Box::new(rewrite_expr(expr, remap)?),
            negated: *negated,
            low: Box::new(rewrite_expr(low, remap)?),
            high: Box::new(rewrite_expr(high, remap)?),
            span: *span,
        },
        E::InList {
            expr,
            list,
            negated,
            span,
        } => E::InList {
            expr: Box::new(rewrite_expr(expr, remap)?),
            list: list
                .iter()
                .map(|e| rewrite_expr(e, remap))
                .collect::<Option<Vec<_>>>()?,
            negated: *negated,
            span: *span,
        },
        E::Cast {
            expr,
            data_type,
            span,
        } => E::Cast {
            expr: Box::new(rewrite_expr(expr, remap)?),
            data_type: data_type.clone(),
            span: *span,
        },
        E::Function {
            name,
            args,
            distinct,
            span,
        } => E::Function {
            name: name.clone(),
            args: args
                .iter()
                .map(|a| rewrite_expr(a, remap))
                .collect::<Option<Vec<_>>>()?,
            distinct: *distinct,
            span: *span,
        },
        E::CaseExpr {
            operand,
            when_clauses,
            else_result,
            span,
        } => E::CaseExpr {
            operand: match operand {
                Some(op) => Some(Box::new(rewrite_expr(op, remap)?)),
                None => None,
            },
            when_clauses: when_clauses
                .iter()
                .map(|(w, t)| Some((rewrite_expr(w, remap)?, rewrite_expr(t, remap)?)))
                .collect::<Option<Vec<_>>>()?,
            else_result: match else_result {
                Some(e) => Some(Box::new(rewrite_expr(e, remap)?)),
                None => None,
            },
            span: *span,
        },
        E::ScalarSubquery { subplan, span } => E::ScalarSubquery {
            subplan: subplan.clone(),
            span: *span,
        },
        E::Parameter {
            index,
            type_hint,
            span,
        } => E::Parameter {
            index: *index,
            type_hint: type_hint.clone(),
            span: *span,
        },
        E::Wildcard => E::Wildcard,
    })
}

// ---------------------------------------------------------------------------
// Post-reorder consistency validation (safety net)
// ---------------------------------------------------------------------------

/// Returns `true` when every `PlanExpr::Column { index, name }` in `plan`
/// resolves to a same-named column at `index` in its enclosing operator's
/// input schema (for joins, the combined left ++ right schema).
///
/// This is the leaf-origin-safe counterpart to the old name-based rebuild: it
/// never *resolves* by name (which cannot disambiguate a self-join's duplicate
/// columns — the F-Perf-RN misroute), it only *checks* that the indices the
/// reorder produced still point at the correctly-named column. A `false`
/// result means `transform_remap` left a stale reference, and
/// `JoinReorder::analyze` falls back to the pristine input plan rather than
/// shipping an inconsistent tree.
fn indices_consistent(plan: &LogicalPlan) -> bool {
    use LogicalPlan as L;

    fn refs_ok(expr: &PlanExpr, schema: &[ColumnInfo]) -> bool {
        let mut ok = true;
        let _ = rewrite_expr(expr, &mut |name, idx| {
            if idx >= schema.len()
                || (schema[idx].name != name && schema.iter().any(|c| c.name == name))
            {
                ok = false;
            }
            Some(idx)
        });
        ok
    }
    fn all_ok(exprs: &[PlanExpr], schema: &[ColumnInfo]) -> bool {
        exprs.iter().all(|e| refs_ok(e, schema))
    }

    match plan {
        L::TableScan { .. }
        | L::CreateTable { .. }
        | L::DropTable { .. }
        | L::DeleteFrom { .. }
        | L::DropView { .. }
        | L::ExchangeNode { .. }
        | L::OneRow => true,

        L::Projection { input, exprs, .. } => {
            all_ok(exprs, &input.schema()) && indices_consistent(input)
        }
        L::Filter { input, predicate } => {
            refs_ok(predicate, &input.schema()) && indices_consistent(input)
        }
        L::Sort { input, order_by } => {
            let s = input.schema();
            order_by.iter().all(|o| refs_ok(&o.expr, &s)) && indices_consistent(input)
        }
        L::Limit { input, .. }
        | L::Distinct { input }
        | L::Explain { input, .. }
        | L::AssignUniqueId { input, .. } => indices_consistent(input),

        L::Aggregate {
            input,
            group_by,
            aggr_exprs,
            ..
        }
        | L::PartialAggregate {
            input,
            group_by,
            aggr_exprs,
            ..
        }
        | L::FinalAggregate {
            input,
            group_by,
            aggr_exprs,
            ..
        } => {
            let s = input.schema();
            all_ok(group_by, &s) && all_ok(aggr_exprs, &s) && indices_consistent(input)
        }
        L::Window { input, functions } => {
            let s = input.schema();
            functions.iter().all(|f| {
                all_ok(&f.args, &s)
                    && all_ok(&f.partition_by, &s)
                    && f.order_by.iter().all(|o| refs_ok(&o.expr, &s))
            }) && indices_consistent(input)
        }
        L::Join {
            left,
            right,
            condition,
            ..
        } => {
            let mut s = left.schema();
            s.extend(right.schema());
            let cond_ok = match condition {
                JoinCondition::On(e) => refs_ok(e, &s),
                JoinCondition::None => true,
            };
            cond_ok && indices_consistent(left) && indices_consistent(right)
        }
        L::SemiJoin {
            left,
            right,
            left_key,
            right_key,
            residual,
            ..
        }
        | L::AntiJoin {
            left,
            right,
            left_key,
            right_key,
            residual,
        } => {
            let ls = left.schema();
            let rs = right.schema();
            let mut combined = ls.clone();
            combined.extend(rs.iter().cloned());
            let res_ok = residual
                .as_ref()
                .map(|r| refs_ok(r, &combined))
                .unwrap_or(true);
            refs_ok(left_key, &ls)
                && refs_ok(right_key, &rs)
                && res_ok
                && indices_consistent(left)
                && indices_consistent(right)
        }
        L::ScalarSubquery { subplan } => indices_consistent(subplan),
        L::UnionAll { inputs } => inputs.iter().all(indices_consistent),
        L::Intersect { left, right } | L::Except { left, right } => {
            indices_consistent(left) && indices_consistent(right)
        }
        L::CreateTableAsSelect { source, .. } | L::InsertInto { source, .. } => {
            indices_consistent(source)
        }
        L::CreateView { plan, .. } => indices_consistent(plan),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arneb_catalog::TableStatistics;
    use arneb_common::types::{ColumnInfo, DataType, ScalarValue};
    use std::collections::HashMap;

    fn col_info(name: &str) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn scan(table: &str, cols: &[&str]) -> LogicalPlan {
        LogicalPlan::TableScan {
            table: TableReference::table(table),
            schema: cols.iter().map(|c| col_info(c)).collect(),
            alias: None,
            properties: Default::default(),
            dynamic_filters_consumed: Vec::new(),
        }
    }

    fn col_expr(idx: usize, name: &str) -> PlanExpr {
        PlanExpr::Column {
            index: idx,
            name: name.to_string(),
            span: None,
        }
    }

    fn eq(left: PlanExpr, right: PlanExpr) -> PlanExpr {
        PlanExpr::BinaryOp {
            left: Box::new(left),
            op: BinaryOp::Eq,
            right: Box::new(right),
            span: None,
        }
    }

    fn and(left: PlanExpr, right: PlanExpr) -> PlanExpr {
        PlanExpr::BinaryOp {
            left: Box::new(left),
            op: BinaryOp::And,
            right: Box::new(right),
            span: None,
        }
    }

    fn lit(v: i64) -> PlanExpr {
        PlanExpr::Literal {
            value: ScalarValue::Int64(v),
            span: None,
        }
    }

    fn stats_for(table: &str, row_count: u64, column_ndvs: &[(&str, u64)]) -> TableStatistics {
        let mut columns = HashMap::new();
        for (col, ndv) in column_ndvs {
            columns.insert(
                col.to_string(),
                arneb_catalog::ColumnStatistics {
                    ndv: Some(*ndv),
                    ..arneb_catalog::ColumnStatistics::default()
                },
            );
        }
        let _ = table;
        TableStatistics {
            row_count: Some(row_count),
            size_bytes: None,
            columns,
        }
    }

    fn collect_join_leaves_left_deep(plan: &LogicalPlan) -> Vec<String> {
        let mut leaves = Vec::new();
        fn walk(plan: &LogicalPlan, sink: &mut Vec<String>) {
            match plan {
                LogicalPlan::Join {
                    left,
                    right,
                    join_type: JoinType::Inner,
                    ..
                } => {
                    walk(left, sink);
                    walk(right, sink);
                }
                LogicalPlan::TableScan { table, .. } => sink.push(table.table.clone()),
                other => sink.push(format!("{}", other)),
            }
        }
        walk(plan, &mut leaves);
        leaves
    }

    // -- No-op cases ---------------------------------------------------

    #[test]
    fn noop_for_single_table_scan() {
        let plan = scan("t", &["id"]);
        let before = plan.to_string();
        let mut ctx = AnalyzerContext::new();
        let after = JoinReorder::new().analyze(plan, &mut ctx).unwrap();
        assert_eq!(after.to_string(), before);
    }

    #[test]
    fn noop_for_only_outer_joins() {
        let plan = LogicalPlan::Join {
            left: Box::new(scan("a", &["k"])),
            right: Box::new(scan("b", &["k2"])),
            join_type: JoinType::Left,
            condition: JoinCondition::On(eq(col_expr(0, "k"), col_expr(0, "k2"))),
            dynamic_filter_ids: Vec::new(),
        };
        let before = plan.to_string();
        let mut ctx = AnalyzerContext::new();
        let after = JoinReorder::new().analyze(plan, &mut ctx).unwrap();
        assert_eq!(after.to_string(), before);
    }

    #[test]
    fn noop_when_no_reorder_hint_present() {
        // big ⋈ small with stats — if reordered, smallest would move to
        // outer. With the hint, original SQL order is preserved.
        let mut catalog_stats = CatalogStats::new();
        catalog_stats.insert(
            TableReference::table("big"),
            stats_for("big", 6_000_000, &[("k", 1_500_000)]),
        );
        catalog_stats.insert(
            TableReference::table("small"),
            stats_for("small", 25, &[("k", 25)]),
        );

        let plan = LogicalPlan::Join {
            left: Box::new(scan("big", &["k"])),
            right: Box::new(scan("small", &["k"])),
            join_type: JoinType::Inner,
            condition: JoinCondition::On(eq(col_expr(0, "k"), col_expr(1, "k"))),
            dynamic_filter_ids: Vec::new(),
        };
        let before = plan.to_string();
        let mut ctx = AnalyzerContext::with_stats(std::sync::Arc::new(catalog_stats));
        ctx.hints.insert(Hint::NoReorder);
        let after = JoinReorder::new().analyze(plan, &mut ctx).unwrap();
        assert_eq!(after.to_string(), before);
    }

    // -- Two-way reorder ---------------------------------------------

    #[test]
    fn reorders_two_way_to_put_smaller_on_build() {
        // HashJoinExec uses RIGHT as the build side, so the smaller
        // table should land on the RIGHT (build) to keep the hash
        // table small and let the larger probe side parallelise.
        // Original SQL order: big ⋈ small (big on left). That's
        // already optimal — JoinReorder should keep it (or reach the
        // same shape).
        let mut catalog_stats = CatalogStats::new();
        catalog_stats.insert(
            TableReference::table("big"),
            stats_for("big", 6_000_000, &[("kb", 1_500_000)]),
        );
        catalog_stats.insert(
            TableReference::table("small"),
            stats_for("small", 25, &[("ks", 25)]),
        );

        let plan = LogicalPlan::Join {
            left: Box::new(scan("big", &["kb"])),
            right: Box::new(scan("small", &["ks"])),
            join_type: JoinType::Inner,
            condition: JoinCondition::On(eq(col_expr(0, "kb"), col_expr(1, "ks"))),
            dynamic_filter_ids: Vec::new(),
        };
        let mut ctx = AnalyzerContext::with_stats(std::sync::Arc::new(catalog_stats));
        let after = JoinReorder::new().analyze(plan, &mut ctx).unwrap();

        let leaves = collect_join_leaves_left_deep(&after);
        assert_eq!(leaves, vec!["big".to_string(), "small".to_string()]);
    }

    // -- Three-way reorder (TPC-H style) ----------------------------

    #[test]
    fn reorders_three_way_puts_smaller_on_build() {
        // big ⋈ medium ⋈ small with sizes 6M / 200K / 25.
        // Build is RIGHT — smaller tables should land on RIGHT (build)
        // and the biggest on LEFT (probe-parallel). Selinger DP picks
        // [big, medium, small] (read left-to-right: big LEFT, then
        // medium RIGHT/build at inner level, then small RIGHT/build
        // at outer level).
        let mut catalog_stats = CatalogStats::new();
        catalog_stats.insert(
            TableReference::table("big"),
            stats_for("big", 6_000_000, &[("k", 1_500_000)]),
        );
        catalog_stats.insert(
            TableReference::table("medium"),
            stats_for("medium", 200_000, &[("k", 200_000)]),
        );
        catalog_stats.insert(
            TableReference::table("small"),
            stats_for("small", 25, &[("k", 25)]),
        );

        // Original: ((big ⋈ medium) ⋈ small) with conditions:
        //   big.k = medium.k AND medium.k = small.k
        let big_med = LogicalPlan::Join {
            left: Box::new(scan("big", &["bigk"])),
            right: Box::new(scan("medium", &["medk"])),
            join_type: JoinType::Inner,
            condition: JoinCondition::On(eq(col_expr(0, "bigk"), col_expr(1, "medk"))),
            dynamic_filter_ids: Vec::new(),
        };
        let plan = LogicalPlan::Join {
            left: Box::new(big_med),
            right: Box::new(scan("small", &["smallk"])),
            join_type: JoinType::Inner,
            condition: JoinCondition::On(eq(col_expr(1, "medk"), col_expr(2, "smallk"))),
            dynamic_filter_ids: Vec::new(),
        };

        let mut ctx = AnalyzerContext::with_stats(std::sync::Arc::new(catalog_stats));
        let after = JoinReorder::new().analyze(plan, &mut ctx).unwrap();

        let leaves = collect_join_leaves_left_deep(&after);
        assert!(
            leaves.first().map(|s| s == "big").unwrap_or(false),
            "expected 'big' to be the leftmost leaf (probe-parallel), got {:?}",
            leaves
        );
    }

    // -- Self-join chain (q08-shape) — build-side robustness --------
    //
    // planner-build-side-selection task 1.1. Before §2, q08 built the 90M
    // lineitem side and probed 20K part because JoinReorder bailed on the
    // `nation n1 / nation n2` self-join's duplicate leaf column names and
    // kept the SQL order. Leaf-origin tracking now reorders self-joins, so
    // the big fact MUST land on the LEFT (probe) spine, not be built.

    #[test]
    fn reorders_self_join_chain_puts_fact_on_probe_spine() {
        let mut catalog_stats = CatalogStats::new();
        catalog_stats.insert(
            TableReference::table("lineitem"),
            stats_for("lineitem", 6_000_000, &[("l_nk", 1_000)]),
        );
        catalog_stats.insert(
            TableReference::table("nation"),
            stats_for("nation", 25, &[("n_nk", 25), ("n_rk", 5)]),
        );

        // Original SQL order: ((nation_n1 ⋈ nation_n2) ⋈ lineitem) with
        //   n1.n_rk = n2.n_rk  AND  n2.n_nk = lineitem.l_nk
        // lineitem is the OUTER RIGHT child = BUILT (the 90M-class bug).
        // The two `nation` scans share column names; leaf-origin tracking
        // disambiguates them and reorders so lineitem lands on the probe spine.
        let n1_n2 = LogicalPlan::Join {
            left: Box::new(scan("nation", &["n_rk", "n_nk"])),
            right: Box::new(scan("nation", &["n_rk", "n_nk"])),
            join_type: JoinType::Inner,
            condition: JoinCondition::On(eq(col_expr(0, "n_rk"), col_expr(2, "n_rk"))),
            dynamic_filter_ids: Vec::new(),
        };
        let plan = LogicalPlan::Join {
            left: Box::new(n1_n2),
            right: Box::new(scan("lineitem", &["l_nk"])),
            join_type: JoinType::Inner,
            condition: JoinCondition::On(eq(col_expr(3, "n_nk"), col_expr(4, "l_nk"))),
            dynamic_filter_ids: Vec::new(),
        };

        let mut ctx = AnalyzerContext::with_stats(std::sync::Arc::new(catalog_stats));
        let after = JoinReorder::new().analyze(plan, &mut ctx).unwrap();

        let leaves = collect_join_leaves_left_deep(&after);
        assert_eq!(
            leaves.first().map(String::as_str),
            Some("lineitem"),
            "fact (lineitem, 6M) must be on the probe spine, not built; got {:?}",
            leaves
        );
    }

    // planner-build-side-selection task 1.2 — q18 RowConverter guard.
    // The F-Perf-RN revert was caused by reordered self-join column
    // indices pointing at the WRONG leaf's column (a RowConverter index
    // mismatch at runtime). This guard asserts, at unit-test time, that
    // every join-condition `Column { index, name }` in a reordered
    // self-join plan resolves to a column with the SAME name in that
    // join's combined left++right schema. Passes trivially today (the
    // chain bails, indices unchanged); MUST stay green once leaf-origin
    // tracking makes the reorder actually rebuild self-join indices.

    fn assert_join_column_indices_consistent(plan: &LogicalPlan) {
        fn visit_cols<'a>(expr: &'a PlanExpr, out: &mut Vec<(usize, &'a str)>) {
            match expr {
                PlanExpr::Column { index, name, .. } => out.push((*index, name.as_str())),
                PlanExpr::BinaryOp { left, right, .. } => {
                    visit_cols(left, out);
                    visit_cols(right, out);
                }
                PlanExpr::UnaryOp { expr, .. }
                | PlanExpr::IsNull { expr, .. }
                | PlanExpr::IsNotNull { expr, .. }
                | PlanExpr::Cast { expr, .. } => visit_cols(expr, out),
                _ => {}
            }
        }
        if let LogicalPlan::Join {
            left,
            right,
            condition: JoinCondition::On(expr),
            ..
        } = plan
        {
            let mut combined = left.schema();
            combined.extend(right.schema());
            let mut cols = Vec::new();
            visit_cols(expr, &mut cols);
            for (idx, name) in cols {
                assert!(
                    idx < combined.len(),
                    "join column index {idx} out of bounds (schema len {})",
                    combined.len()
                );
                assert_eq!(
                    combined[idx].name, name,
                    "join column index {idx} resolves to {:?}, expected {:?} (RowConverter mismatch class)",
                    combined[idx].name, name
                );
            }
            assert_join_column_indices_consistent(left);
            assert_join_column_indices_consistent(right);
        }
    }

    #[test]
    fn self_join_reorder_keeps_column_indices_consistent() {
        let mut catalog_stats = CatalogStats::new();
        catalog_stats.insert(
            TableReference::table("lineitem"),
            stats_for("lineitem", 6_000_000, &[("l_nk", 1_000)]),
        );
        catalog_stats.insert(
            TableReference::table("nation"),
            stats_for("nation", 25, &[("n_nk", 25), ("n_rk", 5)]),
        );
        let n1_n2 = LogicalPlan::Join {
            left: Box::new(scan("nation", &["n_rk", "n_nk"])),
            right: Box::new(scan("nation", &["n_rk", "n_nk"])),
            join_type: JoinType::Inner,
            condition: JoinCondition::On(eq(col_expr(0, "n_rk"), col_expr(2, "n_rk"))),
            dynamic_filter_ids: Vec::new(),
        };
        let plan = LogicalPlan::Join {
            left: Box::new(n1_n2),
            right: Box::new(scan("lineitem", &["l_nk"])),
            join_type: JoinType::Inner,
            condition: JoinCondition::On(eq(col_expr(3, "n_nk"), col_expr(4, "l_nk"))),
            dynamic_filter_ids: Vec::new(),
        };
        let mut ctx = AnalyzerContext::with_stats(std::sync::Arc::new(catalog_stats));
        let after = JoinReorder::new().analyze(plan, &mut ctx).unwrap();
        // Whether or not the chain was reordered, every join-condition
        // column index must resolve to the correctly-named column.
        assert_join_column_indices_consistent(&after);
    }

    // -- Greedy fallback ---------------------------------------------
    //
    // greedy_order fires when leaves.len() > dp_max_tables (default 8)
    // OR when selinger_order returns None (no connected ordering). The
    // critical correctness property: for one big fact + many small
    // dims, the fact must end up on the LEFT (probe-parallel), because
    // hash join builds the RIGHT side and a 6M-row build is the
    // textbook wrong-side bug.

    #[test]
    fn greedy_puts_fact_on_left_for_fact_plus_many_dims() {
        // 9 leaves: 1 fact (6M) + 8 dims (each ≤ 1k). All dims join to
        // fact via a shared fact column `fact.fk{i}`. With >8 leaves
        // greedy fires.
        let mut catalog_stats = CatalogStats::new();
        catalog_stats.insert(
            TableReference::table("fact"),
            stats_for(
                "fact",
                6_000_000,
                &[
                    ("fk0", 1_000),
                    ("fk1", 1_000),
                    ("fk2", 1_000),
                    ("fk3", 1_000),
                    ("fk4", 1_000),
                    ("fk5", 1_000),
                    ("fk6", 1_000),
                    ("fk7", 1_000),
                ],
            ),
        );
        let dim_sizes = [25u64, 50, 100, 200, 400, 600, 800, 1000];
        let mut leaves: Vec<LogicalPlan> = Vec::new();
        leaves.push(scan(
            "fact",
            &["fk0", "fk1", "fk2", "fk3", "fk4", "fk5", "fk6", "fk7"],
        ));
        for (i, sz) in dim_sizes.iter().enumerate() {
            let name = format!("d{i}");
            let col = format!("d{i}k");
            catalog_stats.insert(
                TableReference::table(&name),
                stats_for(&name, *sz, &[(&col, *sz)]),
            );
            leaves.push(scan(&name, &[&col]));
        }

        // Edges connecting fact.fk_i = d_i.d_i_k. The leaf-owner
        // resolver inside emit_left_deep matches by column NAME
        // (e.g. "fk3", "d3k") which are globally unique here.
        let edges: Vec<PlanExpr> = (0..8)
            .map(|i| {
                eq(
                    col_expr(0, &format!("fk{i}")),
                    col_expr(i + 1, &format!("d{i}k")),
                )
            })
            .collect();

        let stats = std::sync::Arc::new(catalog_stats);
        let order = greedy_order(&leaves, &edges, &stats);

        // 'fact' is leaves[0]. The pre-fix greedy_order picked the
        // smallest leaf first (d0, 25 rows) and put 'fact' last — i.e.
        // on the outermost RIGHT (build side, 6M row hash table). The
        // selinger_cost-driven greedy must instead push 'fact' to the
        // LEFTMOST position so it ends up probing parallel-wise.
        assert_eq!(
            order[0], 0,
            "greedy_order must anchor with 'fact' (idx=0) on the LEFT; \
             got order = {order:?} (leaf names: fact, d0..d7)"
        );
    }

    // -- Robustness ---------------------------------------------------

    #[test]
    fn noop_when_no_stats_available() {
        // With no statistics, every leaf has the same default size and
        // the greedy algorithm could pick any permutation. Verify that
        // the rewrite either preserves the plan or produces an
        // equivalent plan that the cost model rates no worse.
        let plan = LogicalPlan::Join {
            left: Box::new(scan("a", &["k"])),
            right: Box::new(scan("b", &["k2"])),
            join_type: JoinType::Inner,
            condition: JoinCondition::On(eq(col_expr(0, "k"), col_expr(0, "k2"))),
            dynamic_filter_ids: Vec::new(),
        };
        let mut ctx = AnalyzerContext::new();
        // Should not panic, and should remain a valid plan.
        let after = JoinReorder::new().analyze(plan.clone(), &mut ctx).unwrap();
        assert!(matches!(after, LogicalPlan::Join { .. }));
    }

    #[test]
    fn pass_recurses_into_scalar_subquery() {
        // Outer plan is unreorderable (single scan), but the subplan
        // inside a ScalarSubquery is a reorderable inner-join chain. Verify
        // it gets reordered.
        let mut catalog_stats = CatalogStats::new();
        catalog_stats.insert(
            TableReference::table("big"),
            stats_for("big", 6_000_000, &[("kb", 1_500_000)]),
        );
        catalog_stats.insert(
            TableReference::table("small"),
            stats_for("small", 25, &[("ks", 25)]),
        );
        let subplan = LogicalPlan::Join {
            left: Box::new(scan("big", &["kb"])),
            right: Box::new(scan("small", &["ks"])),
            join_type: JoinType::Inner,
            condition: JoinCondition::On(eq(col_expr(0, "kb"), col_expr(1, "ks"))),
            dynamic_filter_ids: Vec::new(),
        };
        let plan = LogicalPlan::ScalarSubquery {
            subplan: Box::new(subplan),
        };
        let mut ctx = AnalyzerContext::with_stats(std::sync::Arc::new(catalog_stats));
        let after = JoinReorder::new().analyze(plan, &mut ctx).unwrap();
        if let LogicalPlan::ScalarSubquery { subplan } = after {
            let leaves = collect_join_leaves_left_deep(&subplan);
            // Bigger on LEFT (probe), smaller on RIGHT (build).
            assert_eq!(leaves, vec!["big".to_string(), "small".to_string()]);
        } else {
            panic!("expected ScalarSubquery, got {:?}", after);
        }
    }

    #[test]
    fn handles_split_and_condition() {
        // Single Join with `a.k = b.k AND a.k > 0` — the AND-atom split
        // helper should still recognize the equi-key.
        let mut catalog_stats = CatalogStats::new();
        catalog_stats.insert(
            TableReference::table("a"),
            stats_for("a", 1000, &[("ak", 1000)]),
        );
        catalog_stats.insert(
            TableReference::table("b"),
            stats_for("b", 10, &[("bk", 10)]),
        );
        let plan = LogicalPlan::Join {
            left: Box::new(scan("a", &["ak"])),
            right: Box::new(scan("b", &["bk"])),
            join_type: JoinType::Inner,
            condition: JoinCondition::On(and(
                eq(col_expr(0, "ak"), col_expr(1, "bk")),
                PlanExpr::BinaryOp {
                    left: Box::new(col_expr(0, "ak")),
                    op: BinaryOp::Gt,
                    right: Box::new(lit(0)),
                    span: None,
                },
            )),
            dynamic_filter_ids: Vec::new(),
        };
        let mut ctx = AnalyzerContext::with_stats(std::sync::Arc::new(catalog_stats));
        let after = JoinReorder::new().analyze(plan, &mut ctx).unwrap();
        // Build is RIGHT; smaller (b) lands on the RIGHT, bigger (a)
        // on the LEFT (probe-parallel).
        let leaves = collect_join_leaves_left_deep(&after);
        assert_eq!(leaves, vec!["a".to_string(), "b".to_string()]);
    }

    /// Repros TPC-H Q16's pattern: Filter above a swapped Join.
    /// Verifies that the Filter's predicate columns are correctly
    /// re-indexed after the join is reordered.
    #[test]
    fn filter_above_swapped_join_has_correct_column_indices() {
        // Mimic: Filter [p_brand != 'X' AND p_type NOT LIKE 'Y']
        //          Join (partsupp INNER JOIN part) ON p_partkey = ps_partkey
        // partsupp: ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment (5 cols)
        // part: p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment (9 cols)
        let mut catalog_stats = CatalogStats::new();
        catalog_stats.insert(
            TableReference::table("partsupp"),
            stats_for(
                "partsupp",
                800_000,
                &[("ps_partkey", 200_000), ("ps_suppkey", 10_000)],
            ),
        );
        catalog_stats.insert(
            TableReference::table("part"),
            stats_for(
                "part",
                200_000,
                &[("p_partkey", 200_000), ("p_brand", 25), ("p_type", 150)],
            ),
        );

        let partsupp = LogicalPlan::TableScan {
            table: TableReference::table("partsupp"),
            schema: vec![
                col_info("ps_partkey"),
                col_info("ps_suppkey"),
                col_info("ps_availqty"),
                col_info("ps_supplycost"),
                col_info("ps_comment"),
            ],
            alias: None,
            properties: Default::default(),
            dynamic_filters_consumed: Vec::new(),
        };
        let part = LogicalPlan::TableScan {
            table: TableReference::table("part"),
            schema: vec![
                col_info("p_partkey"),
                col_info("p_name"),
                col_info("p_mfgr"),
                col_info("p_brand"),
                col_info("p_type"),
                col_info("p_size"),
                col_info("p_container"),
                col_info("p_retailprice"),
                col_info("p_comment"),
            ],
            alias: None,
            properties: Default::default(),
            dynamic_filters_consumed: Vec::new(),
        };

        // Original schema: partsupp ++ part = 5 + 9 = 14 cols
        //   p_partkey at idx 5, p_brand at idx 8, p_type at idx 9
        //   ps_partkey at idx 0
        let join = LogicalPlan::Join {
            left: Box::new(partsupp),
            right: Box::new(part),
            join_type: JoinType::Inner,
            condition: JoinCondition::On(eq(col_expr(5, "p_partkey"), col_expr(0, "ps_partkey"))),
            dynamic_filter_ids: Vec::new(),
        };

        // Filter has predicates referencing p_brand (idx 8) and p_type (idx 9)
        // in the ORIGINAL schema.
        let filter = LogicalPlan::Filter {
            input: Box::new(join),
            predicate: PlanExpr::BinaryOp {
                left: Box::new(PlanExpr::BinaryOp {
                    left: Box::new(col_expr(8, "p_brand")),
                    op: BinaryOp::NotEq,
                    right: Box::new(PlanExpr::Literal {
                        value: arneb_common::types::ScalarValue::Utf8("Brand#45".to_string()),
                        span: None,
                    }),
                    span: None,
                }),
                op: BinaryOp::And,
                right: Box::new(col_expr(9, "p_type")),
                span: None,
            },
        };

        let mut ctx = AnalyzerContext::with_stats(std::sync::Arc::new(catalog_stats));
        let after = JoinReorder::new().analyze(filter, &mut ctx).unwrap();

        // Build = RIGHT, so the smaller side (part, 200K) lands on the
        // RIGHT and the bigger side (partsupp, 800K) stays on the LEFT.
        // Schema stays the same as input: partsupp ++ part = 5 + 9 = 14 cols.
        //   ps_partkey at 0, p_partkey at 5, p_brand at 8, p_type at 9.
        match &after {
            LogicalPlan::Filter { input, predicate } => {
                // Confirm the join: partsupp on LEFT, part on RIGHT.
                if let LogicalPlan::Join { left, right, .. } = input.as_ref() {
                    if let LogicalPlan::TableScan { table, .. } = left.as_ref() {
                        assert_eq!(
                            table.table, "partsupp",
                            "expected partsupp on LEFT (build is RIGHT — smaller part goes right)"
                        );
                    } else {
                        panic!("expected TableScan on left, got {:?}", left);
                    }
                    if let LogicalPlan::TableScan { table, .. } = right.as_ref() {
                        assert_eq!(table.table, "part", "expected part on RIGHT");
                    } else {
                        panic!("expected TableScan on right, got {:?}", right);
                    }
                } else {
                    panic!("expected Join below Filter, got {:?}", input);
                }

                // Filter predicate's column indices unchanged because
                // the schema layout (partsupp ++ part) wasn't swapped.
                let mut found = std::collections::HashMap::new();
                fn walk(e: &PlanExpr, out: &mut std::collections::HashMap<String, usize>) {
                    match e {
                        PlanExpr::Column { name, index, .. } => {
                            out.insert(name.clone(), *index);
                        }
                        PlanExpr::BinaryOp { left, right, .. } => {
                            walk(left, out);
                            walk(right, out);
                        }
                        _ => {}
                    }
                }
                walk(predicate, &mut found);
                assert_eq!(
                    found.get("p_brand"),
                    Some(&8),
                    "p_brand should be at index 8 in partsupp ++ part schema, got {:?}",
                    found
                );
                assert_eq!(
                    found.get("p_type"),
                    Some(&9),
                    "p_type should be at index 9 in partsupp ++ part schema, got {:?}",
                    found
                );
            }
            _ => panic!("expected Filter, got {:?}", after),
        }
    }

    #[test]
    fn rebuild_indices_preserves_column_resolution() {
        // Build is RIGHT — Selinger keeps big on LEFT (probe-parallel)
        // and small on RIGHT (small build). Original was big.kb on
        // left (idx 0) and small.ks on right (idx 1). After reorder,
        // shape is unchanged → kb at 0, ks at 1.
        let mut catalog_stats = CatalogStats::new();
        catalog_stats.insert(
            TableReference::table("big"),
            stats_for("big", 6_000_000, &[("kb", 1_500_000)]),
        );
        catalog_stats.insert(
            TableReference::table("small"),
            stats_for("small", 25, &[("ks", 25)]),
        );
        let plan = LogicalPlan::Join {
            left: Box::new(scan("big", &["kb"])),
            right: Box::new(scan("small", &["ks"])),
            join_type: JoinType::Inner,
            condition: JoinCondition::On(eq(col_expr(0, "kb"), col_expr(1, "ks"))),
            dynamic_filter_ids: Vec::new(),
        };
        let mut ctx = AnalyzerContext::with_stats(std::sync::Arc::new(catalog_stats));
        let after = JoinReorder::new().analyze(plan, &mut ctx).unwrap();

        if let LogicalPlan::Join { condition, .. } = after {
            if let JoinCondition::On(expr) = condition {
                let mut indices: Vec<(String, usize)> = Vec::new();
                fn collect(e: &PlanExpr, sink: &mut Vec<(String, usize)>) {
                    match e {
                        PlanExpr::Column { name, index, .. } => sink.push((name.clone(), *index)),
                        PlanExpr::BinaryOp { left, right, .. } => {
                            collect(left, sink);
                            collect(right, sink);
                        }
                        _ => {}
                    }
                }
                collect(&expr, &mut indices);
                // kb is in left side (idx 0); ks is in right side (idx 1).
                let ks = indices.iter().find(|(n, _)| n == "ks").unwrap();
                let kb = indices.iter().find(|(n, _)| n == "kb").unwrap();
                assert_eq!(kb.1, 0);
                assert_eq!(ks.1, 1);
            } else {
                panic!("expected ON condition");
            }
        } else {
            panic!("expected Join");
        }
    }
}
