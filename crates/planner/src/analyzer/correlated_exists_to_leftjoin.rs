//! `CorrelatedExistsToLeftJoin` analyzer pass.
//!
//! Rewrites `SemiJoin / AntiJoin { residual: Some(r), .. }` —
//! correlated EXISTS / NOT EXISTS with mixed equi + non-equi
//! correlation, e.g. TPC-H Q21's
//! `EXISTS (SELECT 1 FROM lineitem l2 WHERE l2.l_orderkey =
//! l1.l_orderkey AND l2.l_suppkey <> l1.l_suppkey)` — into the
//! Trino-style LeftJoin + AssignUniqueId + Aggregate(BOOL_OR) +
//! Filter form:
//!
//! ```text
//! SemiJoin { left, right, left_key, right_key, residual: Some(r) }
//!     │
//!     ▼
//! Projection (keep first L original outer cols, drop __rowid + __exists)
//!   ← Filter(IS NOT NULL(__exists))                       -- SEMI
//!     ← Aggregate(group_by=[left_cols.., __rowid],
//!                 aggr=[__exists = BOOL_OR(__sub_true)])
//!       ← Join(Left, On(left_key = right_key AND residual_remapped))
//!         ├─ AssignUniqueId(left)        -- appends Int64 __rowid
//!         └─ Projection(right cols + literal TRUE __sub_true)
//! ```
//!
//! `AntiJoin` uses the same shape but with `IS NULL(__exists)` for the
//! Filter (a row passes iff zero inner rows matched).
//!
//! ### Why this is faster than `SemiJoinExec { residual }`
//!
//! `SemiJoinExec` with a residual must, for every outer key match,
//! iterate every inner row in the same hash bucket and evaluate the
//! residual until either one passes or all fail. For TPC-H Q21 (~6M
//! `l1.l_orderkey` outer rows, ~10–30 inner candidates per bucket),
//! this is O(outer × bucket_size) residual evals.
//!
//! The rewrite turns it into:
//!   - one HashJoin probe (equi check via vectorised hash + pre-built
//!     table)
//!   - one residual evaluation per joined pair (vectorised batch eval
//!     during probe)
//!   - one BOOL_OR aggregate per outer row (linear scan over its
//!     joined batches)
//!
//! Mirrors Trino's `TransformExistsApplyToCorrelatedJoin` →
//! `TransformCorrelatedJoinToJoin` chain.
//!
//! ### Pipeline placement
//!
//! Runs AFTER `JoinReorder` so any reorder of the outer plan settles
//! first. `SemiJoin` / `AntiJoin` themselves are not reorderable
//! (`JoinReorder::recurse_children` walks into their children but
//! never picks them as a chain root), so the SemiJoin's internal
//! key/residual indices stay stable across reorder.

use std::sync::atomic::{AtomicU64, Ordering};

use arneb_common::error::PlanError;
use arneb_common::types::{ColumnInfo, DataType, ScalarValue};
use arneb_sql_parser::ast;

use crate::analyzer::{AnalysisPass, AnalyzerContext};
use crate::plan::{JoinCondition, LogicalPlan, PlanExpr};

/// Pass instance. Stateless except for a process-wide counter that
/// keeps synthetic column names (`__semi_rowid_N`, `__semi_sub_true_N`,
/// `__semi_exists_N`) unique across rewrites — useful when the same
/// query contains multiple correlated EXISTS / NOT EXISTS (e.g.
/// TPC-H Q21 has both).
#[derive(Debug, Default)]
pub struct CorrelatedExistsToLeftJoin;

impl CorrelatedExistsToLeftJoin {
    pub fn new() -> Self {
        Self
    }
}

impl AnalysisPass for CorrelatedExistsToLeftJoin {
    fn name(&self) -> &'static str {
        "CorrelatedExistsToLeftJoin"
    }

    fn analyze(
        &self,
        plan: LogicalPlan,
        _ctx: &mut AnalyzerContext,
    ) -> Result<LogicalPlan, PlanError> {
        Ok(rewrite(plan))
    }
}

// Module-private counter shared across all rewrites for uniqueness.
// `Relaxed` is fine — we only need uniqueness, not happens-before.
static NAME_COUNTER: AtomicU64 = AtomicU64::new(0);

fn fresh_id() -> u64 {
    NAME_COUNTER.fetch_add(1, Ordering::Relaxed)
}

// ---------------------------------------------------------------------------
// Walker
// ---------------------------------------------------------------------------

fn rewrite(plan: LogicalPlan) -> LogicalPlan {
    use LogicalPlan as L;
    // Bottom-up: recurse into children first so nested SemiJoins get
    // rewritten before their parent.
    let plan = recurse_children(plan, &mut rewrite);
    match plan {
        L::SemiJoin {
            left,
            right,
            left_key,
            right_key,
            residual: Some(r),
            ..
        } => rewrite_one(*left, *right, left_key, right_key, r, /* anti */ false),
        L::AntiJoin {
            left,
            right,
            left_key,
            right_key,
            residual: Some(r),
        } => rewrite_one(*left, *right, left_key, right_key, r, /* anti */ true),
        other => other,
    }
}

/// One-step recursion: re-build `plan` with `f` applied to each child.
/// Mirrors the shape in `analyzer::join_reorder::recurse_children` but
/// kept private here so this pass is self-contained.
fn recurse_children<F>(plan: LogicalPlan, f: &mut F) -> LogicalPlan
where
    F: FnMut(LogicalPlan) -> LogicalPlan,
{
    use LogicalPlan as L;
    match plan {
        L::TableScan { .. }
        | L::ExchangeNode { .. }
        | L::CreateTable { .. }
        | L::DropTable { .. }
        | L::DeleteFrom { .. }
        | L::DropView { .. }
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
        L::AssignUniqueId { input, id_column } => L::AssignUniqueId {
            input: Box::new(f(*input)),
            id_column,
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
    }
}

// ---------------------------------------------------------------------------
// The rewrite itself
// ---------------------------------------------------------------------------

fn rewrite_one(
    left: LogicalPlan,
    right: LogicalPlan,
    left_key: PlanExpr,
    right_key: PlanExpr,
    residual: PlanExpr,
    anti: bool,
) -> LogicalPlan {
    let left_schema = left.schema();
    let original_right_schema = right.schema();
    let l = left_schema.len();
    let id = fresh_id();

    let rowid_name = format!("__semi_rowid_{id}");
    let sub_true_name = format!("__semi_sub_true_{id}");
    let exists_name = format!("__semi_exists_{id}");

    // ---- Right-side column pruning ----
    // Original SemiJoinExec runs F-Perf3 ("residual column pruning")
    // to keep only the right columns referenced by `right_key` and
    // `residual`. The rewrite must preserve that — otherwise the
    // LeftJoin builds a hash table over all right columns (e.g.
    // lineitem's 16 cols, 6M rows ≈ 7.4 GB raw before column
    // pruning vs 820 MB pruned). `prune_for_columns` in the physical
    // planner bails out on non-INNER joins for safety, so we have
    // to do this here.
    //
    // We collect right-side indices from `right_key` (which lives in
    // the right schema, 0..R) and the right portion of `residual`
    // (which lives in the combined [left|right] layout, so right
    // indices appear as l..l+R).
    let mut right_keep_indices: Vec<usize> = Vec::new();
    let mut seen: std::collections::HashSet<usize> = std::collections::HashSet::new();
    let mut push_right = |i: usize| {
        if i < original_right_schema.len() && seen.insert(i) {
            right_keep_indices.push(i);
        }
    };
    collect_column_indices(&right_key, &mut |i| push_right(i));
    collect_column_indices(&residual, &mut |i| {
        if i >= l {
            push_right(i - l);
        }
    });
    right_keep_indices.sort();

    // Old-right-index -> new-right-index (post-prune position).
    let right_remap: std::collections::HashMap<usize, usize> = right_keep_indices
        .iter()
        .enumerate()
        .map(|(new, &old)| (old, new))
        .collect();
    let pruned_right_schema: Vec<ColumnInfo> = right_keep_indices
        .iter()
        .map(|&i| original_right_schema[i].clone())
        .collect();
    let pr = pruned_right_schema.len();

    let right_pruned = if pr == original_right_schema.len() {
        // Nothing to prune; skip the extra Projection node.
        right
    } else {
        let proj_exprs: Vec<PlanExpr> = right_keep_indices
            .iter()
            .map(|&i| PlanExpr::Column {
                index: i,
                name: original_right_schema[i].name.clone(),
                span: None,
            })
            .collect();
        LogicalPlan::Projection {
            input: Box::new(right),
            exprs: proj_exprs,
            schema: pruned_right_schema.clone(),
        }
    };

    // ---- AssignUniqueId(left): schema = [left_cols.. | __rowid] ----
    let assign_unique = LogicalPlan::AssignUniqueId {
        input: Box::new(left),
        id_column: rowid_name.clone(),
    };

    // ---- Projection(pruned_right + TRUE as __sub_true) ----
    // schema = [pruned_right_cols.. | __sub_true]
    let mut proj_exprs: Vec<PlanExpr> = (0..pr)
        .map(|i| PlanExpr::Column {
            index: i,
            name: pruned_right_schema[i].name.clone(),
            span: None,
        })
        .collect();
    proj_exprs.push(PlanExpr::Literal {
        value: ScalarValue::Boolean(true),
        span: None,
    });
    let mut proj_schema: Vec<ColumnInfo> = pruned_right_schema.clone();
    proj_schema.push(ColumnInfo {
        name: sub_true_name.clone(),
        data_type: DataType::Boolean,
        nullable: false,
    });
    let right_with_marker = LogicalPlan::Projection {
        input: Box::new(right_pruned),
        exprs: proj_exprs,
        schema: proj_schema,
    };

    let r = pr; // post-prune right width drives all later index math

    // ---- Remap right_key / residual against the pruned right ----
    // Both reference original right indices; map them through
    // `right_remap` so they index into pruned_right_schema.
    let right_key_pruned = remap_columns(right_key, &right_remap);
    // For residual: only the right-side indices (>= l) get remapped.
    let residual_right_pruned = walk_columns(residual, &mut |idx, name, span| {
        if idx >= l {
            let old_r = idx - l;
            let new_r = *right_remap.get(&old_r).unwrap_or(&old_r);
            PlanExpr::Column {
                index: l + new_r,
                name,
                span,
            }
        } else {
            PlanExpr::Column {
                index: idx,
                name,
                span,
            }
        }
    });

    // ---- Build the equi+residual ON condition ----
    // Join layout = [left | __rowid | pruned_right | __sub_true]
    //               widths: L,  1,    R,            1
    let shifted_right_key = shift_right(right_key_pruned, l + 1);
    let equi = PlanExpr::BinaryOp {
        left: Box::new(left_key),
        op: ast::BinaryOp::Eq,
        right: Box::new(shifted_right_key),
        span: None,
    };
    let residual_remapped = shift_residual(residual_right_pruned, l);
    let on_expr = and_expr(equi, residual_remapped);

    // ---- LEFT Join ----
    let join_node = LogicalPlan::Join {
        left: Box::new(assign_unique),
        right: Box::new(right_with_marker),
        join_type: ast::JoinType::Left,
        condition: JoinCondition::On(on_expr),
        dynamic_filter_ids: Vec::new(),
    };

    // ---- Aggregate(group_by=[left_cols, __rowid],
    //                aggr=[__exists = BOOL_OR(__sub_true)]) ----
    let join_schema = join_node.schema(); // [left | __rowid | right | __sub_true]

    let mut group_by: Vec<PlanExpr> = (0..l)
        .map(|i| PlanExpr::Column {
            index: i,
            name: left_schema[i].name.clone(),
            span: None,
        })
        .collect();
    group_by.push(PlanExpr::Column {
        index: l,
        name: rowid_name.clone(),
        span: None,
    });

    let sub_true_idx = l + 1 + r;
    let bool_or_arg = PlanExpr::Column {
        index: sub_true_idx,
        name: sub_true_name.clone(),
        span: None,
    };
    let agg_exprs = vec![PlanExpr::Function {
        name: "BOOL_OR".to_string(),
        args: vec![bool_or_arg],
        distinct: false,
        span: None,
    }];

    // Aggregate output schema = [group_by_cols.. , aggr_results..]
    //                        = [left_cols.. | __rowid | __exists]
    let mut agg_schema: Vec<ColumnInfo> = left_schema.clone();
    agg_schema.push(ColumnInfo {
        name: rowid_name.clone(),
        data_type: DataType::Int64,
        nullable: false,
    });
    agg_schema.push(ColumnInfo {
        name: exists_name.clone(),
        data_type: DataType::Boolean,
        nullable: true,
    });

    // Sanity-check our index arithmetic against what `Join::schema()`
    // produced. The widths are mechanical — a mismatch is a bug in
    // this pass, not a runtime condition — so assert and let it
    // surface in tests.
    debug_assert_eq!(join_schema.len(), l + 1 + r + 1);

    let aggregate_node = LogicalPlan::Aggregate {
        input: Box::new(join_node),
        group_by,
        aggr_exprs: agg_exprs,
        schema: agg_schema,
    };

    // ---- Filter(IS NOT NULL(__exists))     for SEMI
    //      Filter(IS NULL(__exists))         for ANTI ----
    let exists_col = PlanExpr::Column {
        index: l + 1, // [left_cols.. | __rowid | __exists] -> index = l+1
        name: exists_name.clone(),
        span: None,
    };
    let filter_pred = if anti {
        PlanExpr::IsNull {
            expr: Box::new(exists_col),
            span: None,
        }
    } else {
        PlanExpr::IsNotNull {
            expr: Box::new(exists_col),
            span: None,
        }
    };
    let filter_node = LogicalPlan::Filter {
        input: Box::new(aggregate_node),
        predicate: filter_pred,
    };

    // ---- Final Projection: keep only the original left cols. ----
    // Strip the trailing __rowid + __exists so callers see the
    // original SemiJoin output shape (same names, same indices,
    // same data types).
    let final_exprs: Vec<PlanExpr> = (0..l)
        .map(|i| PlanExpr::Column {
            index: i,
            name: left_schema[i].name.clone(),
            span: None,
        })
        .collect();

    LogicalPlan::Projection {
        input: Box::new(filter_node),
        exprs: final_exprs,
        schema: left_schema,
    }
}

// ---------------------------------------------------------------------------
// Index-shift helpers
// ---------------------------------------------------------------------------

/// Add `delta` to every `Column { index }` in `expr`. Used to lift the
/// SemiJoin's `right_key` (indexed into the right schema, 0..R) into
/// the combined Join layout (right starts at L+1).
fn shift_right(expr: PlanExpr, delta: usize) -> PlanExpr {
    walk_columns(expr, &mut |idx, name, span| PlanExpr::Column {
        index: idx + delta,
        name,
        span,
    })
}

/// Shift residual right-side indices by +1 (the `__rowid` inserted at
/// position `l` pushes every right-side column one slot to the right).
/// Left-side indices (< l) stay where they are.
fn shift_residual(expr: PlanExpr, l: usize) -> PlanExpr {
    walk_columns(expr, &mut |idx, name, span| {
        if idx < l {
            PlanExpr::Column {
                index: idx,
                name,
                span,
            }
        } else {
            PlanExpr::Column {
                index: idx + 1,
                name,
                span,
            }
        }
    })
}

fn walk_columns<F>(expr: PlanExpr, f: &mut F) -> PlanExpr
where
    F: FnMut(usize, String, Option<arneb_sql_parser::Span>) -> PlanExpr,
{
    use PlanExpr as E;
    match expr {
        E::Column { index, name, span } => f(index, name, span),
        E::Literal { .. } | E::Parameter { .. } | E::Wildcard => expr,
        E::BinaryOp {
            left,
            op,
            right,
            span,
        } => E::BinaryOp {
            left: Box::new(walk_columns(*left, f)),
            op,
            right: Box::new(walk_columns(*right, f)),
            span,
        },
        E::UnaryOp { op, expr, span } => E::UnaryOp {
            op,
            expr: Box::new(walk_columns(*expr, f)),
            span,
        },
        E::Function {
            name,
            args,
            distinct,
            span,
        } => E::Function {
            name,
            args: args.into_iter().map(|a| walk_columns(a, f)).collect(),
            distinct,
            span,
        },
        E::IsNull { expr, span } => E::IsNull {
            expr: Box::new(walk_columns(*expr, f)),
            span,
        },
        E::IsNotNull { expr, span } => E::IsNotNull {
            expr: Box::new(walk_columns(*expr, f)),
            span,
        },
        E::Between {
            expr,
            negated,
            low,
            high,
            span,
        } => E::Between {
            expr: Box::new(walk_columns(*expr, f)),
            negated,
            low: Box::new(walk_columns(*low, f)),
            high: Box::new(walk_columns(*high, f)),
            span,
        },
        E::InList {
            expr,
            list,
            negated,
            span,
        } => E::InList {
            expr: Box::new(walk_columns(*expr, f)),
            list: list.into_iter().map(|e| walk_columns(e, f)).collect(),
            negated,
            span,
        },
        E::Cast {
            expr,
            data_type,
            span,
        } => E::Cast {
            expr: Box::new(walk_columns(*expr, f)),
            data_type,
            span,
        },
        E::CaseExpr {
            operand,
            when_clauses,
            else_result,
            span,
        } => E::CaseExpr {
            operand: operand.map(|o| Box::new(walk_columns(*o, f))),
            when_clauses: when_clauses
                .into_iter()
                .map(|(w, t)| (walk_columns(w, f), walk_columns(t, f)))
                .collect(),
            else_result: else_result.map(|e| Box::new(walk_columns(*e, f))),
            span,
        },
        E::ScalarSubquery { .. } => expr,
    }
}

/// Visit every `Column` leaf in `expr` and invoke `cb` with its
/// index.
fn collect_column_indices<F: FnMut(usize)>(expr: &PlanExpr, cb: &mut F) {
    use PlanExpr as E;
    match expr {
        E::Column { index, .. } => cb(*index),
        E::Literal { .. } | E::Parameter { .. } | E::Wildcard => {}
        E::BinaryOp { left, right, .. } => {
            collect_column_indices(left, cb);
            collect_column_indices(right, cb);
        }
        E::UnaryOp { expr, .. } => collect_column_indices(expr, cb),
        E::Function { args, .. } => {
            for a in args {
                collect_column_indices(a, cb);
            }
        }
        E::IsNull { expr, .. } | E::IsNotNull { expr, .. } => collect_column_indices(expr, cb),
        E::Between {
            expr, low, high, ..
        } => {
            collect_column_indices(expr, cb);
            collect_column_indices(low, cb);
            collect_column_indices(high, cb);
        }
        E::InList { expr, list, .. } => {
            collect_column_indices(expr, cb);
            for e in list {
                collect_column_indices(e, cb);
            }
        }
        E::Cast { expr, .. } => collect_column_indices(expr, cb),
        E::CaseExpr {
            operand,
            when_clauses,
            else_result,
            ..
        } => {
            if let Some(o) = operand {
                collect_column_indices(o, cb);
            }
            for (w, t) in when_clauses {
                collect_column_indices(w, cb);
                collect_column_indices(t, cb);
            }
            if let Some(e) = else_result {
                collect_column_indices(e, cb);
            }
        }
        E::ScalarSubquery { .. } => {}
    }
}

/// Rewrite every `Column { index }` whose index appears in `remap`,
/// keeping the original column otherwise.
fn remap_columns(expr: PlanExpr, remap: &std::collections::HashMap<usize, usize>) -> PlanExpr {
    walk_columns(expr, &mut |idx, name, span| {
        let new_idx = remap.get(&idx).copied().unwrap_or(idx);
        PlanExpr::Column {
            index: new_idx,
            name,
            span,
        }
    })
}

/// `a AND b` constructor.
fn and_expr(a: PlanExpr, b: PlanExpr) -> PlanExpr {
    PlanExpr::BinaryOp {
        left: Box::new(a),
        op: ast::BinaryOp::And,
        right: Box::new(b),
        span: None,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arneb_common::types::{ColumnInfo, DataType, TableReference};
    use std::collections::HashMap;

    fn col(name: &str, dt: DataType) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            data_type: dt,
            nullable: false,
        }
    }

    fn scan(table: &str, cols: Vec<ColumnInfo>) -> LogicalPlan {
        LogicalPlan::TableScan {
            table: TableReference::table(table),
            schema: cols,
            alias: None,
            properties: HashMap::new(),
            dynamic_filters_consumed: Vec::new(),
        }
    }

    fn col_ref(idx: usize, name: &str) -> PlanExpr {
        PlanExpr::Column {
            index: idx,
            name: name.to_string(),
            span: None,
        }
    }

    fn semi_with_residual() -> LogicalPlan {
        // left: l(a:Int64, b:Int64), right: r(x:Int64, y:Int64)
        // SemiJoin on l.a = r.x with residual l.b <> r.y
        let left = scan(
            "l",
            vec![col("a", DataType::Int64), col("b", DataType::Int64)],
        );
        let right = scan(
            "r",
            vec![col("x", DataType::Int64), col("y", DataType::Int64)],
        );
        // Residual references combined schema [l.a, l.b, r.x, r.y] indexes 0,1,2,3
        let residual = PlanExpr::BinaryOp {
            left: Box::new(col_ref(1, "b")), // left.b at index 1
            op: ast::BinaryOp::NotEq,
            right: Box::new(col_ref(3, "y")), // right.y at index 1+2 = 3
            span: None,
        };
        LogicalPlan::SemiJoin {
            left: Box::new(left),
            right: Box::new(right),
            left_key: col_ref(0, "a"),  // left.a (left-relative index)
            right_key: col_ref(0, "x"), // right.x (right-relative index)
            residual: Some(residual),
            dynamic_filter_ids: Vec::new(),
        }
    }

    #[test]
    fn rewrites_semi_with_residual_to_left_join_aggregate_filter_projection() {
        let plan = semi_with_residual();
        let rewritten = rewrite(plan);

        // Expect: Projection ← Filter ← Aggregate ← Join ← (AssignUniqueId, Projection)
        let proj = match &rewritten {
            LogicalPlan::Projection { input, schema, .. } => {
                // Output schema must equal the original SemiJoin output (left schema).
                assert_eq!(schema.len(), 2);
                assert_eq!(schema[0].name, "a");
                assert_eq!(schema[1].name, "b");
                input
            }
            _ => panic!("expected top-level Projection, got {rewritten}"),
        };
        let filter = match proj.as_ref() {
            LogicalPlan::Filter { input, predicate } => {
                // SEMI -> IS NOT NULL(__exists)
                assert!(matches!(predicate, PlanExpr::IsNotNull { .. }));
                input
            }
            _ => panic!("expected Filter under Projection, got {proj}"),
        };
        let agg = match filter.as_ref() {
            LogicalPlan::Aggregate {
                input,
                group_by,
                aggr_exprs,
                schema,
            } => {
                // group_by = [left.a, left.b, __rowid] (3 cols)
                assert_eq!(group_by.len(), 3);
                // aggr = [BOOL_OR(__sub_true)]
                assert_eq!(aggr_exprs.len(), 1);
                match &aggr_exprs[0] {
                    PlanExpr::Function { name, .. } => assert_eq!(name, "BOOL_OR"),
                    other => panic!("expected BOOL_OR function, got {other}"),
                }
                // schema = [a, b, __rowid, __exists]
                assert_eq!(schema.len(), 4);
                assert!(schema[2].name.starts_with("__semi_rowid_"));
                assert!(schema[3].name.starts_with("__semi_exists_"));
                input
            }
            _ => panic!("expected Aggregate under Filter, got {filter}"),
        };
        let (jt, _cond, jleft, jright) = match agg.as_ref() {
            LogicalPlan::Join {
                join_type,
                condition,
                left,
                right,
                ..
            } => (*join_type, condition.clone(), left, right),
            _ => panic!("expected Join under Aggregate, got {agg}"),
        };
        assert_eq!(jt, ast::JoinType::Left);
        // Left child of the Join must be AssignUniqueId
        assert!(matches!(jleft.as_ref(), LogicalPlan::AssignUniqueId { .. }));
        // Right child of the Join must be Projection adding __sub_true
        match jright.as_ref() {
            LogicalPlan::Projection { schema, .. } => {
                assert_eq!(schema.len(), 3); // x, y, __sub_true
                assert!(schema[2].name.starts_with("__semi_sub_true_"));
                assert_eq!(schema[2].data_type, DataType::Boolean);
            }
            other => panic!("expected right-side Projection, got {other}"),
        }
    }

    #[test]
    fn anti_residual_uses_is_null_filter() {
        // Same shape but AntiJoin instead of SemiJoin.
        let left = scan(
            "l",
            vec![col("a", DataType::Int64), col("b", DataType::Int64)],
        );
        let right = scan(
            "r",
            vec![col("x", DataType::Int64), col("y", DataType::Int64)],
        );
        let residual = PlanExpr::BinaryOp {
            left: Box::new(col_ref(1, "b")),
            op: ast::BinaryOp::NotEq,
            right: Box::new(col_ref(3, "y")),
            span: None,
        };
        let plan = LogicalPlan::AntiJoin {
            left: Box::new(left),
            right: Box::new(right),
            left_key: col_ref(0, "a"),
            right_key: col_ref(0, "x"),
            residual: Some(residual),
        };
        let rewritten = rewrite(plan);
        let proj = match &rewritten {
            LogicalPlan::Projection { input, .. } => input,
            _ => panic!("expected Projection"),
        };
        match proj.as_ref() {
            LogicalPlan::Filter { predicate, .. } => {
                assert!(matches!(predicate, PlanExpr::IsNull { .. }));
            }
            _ => panic!("expected Filter under Projection"),
        }
    }

    #[test]
    fn plain_semi_without_residual_is_untouched() {
        let left = scan("l", vec![col("a", DataType::Int64)]);
        let right = scan("r", vec![col("x", DataType::Int64)]);
        let plan = LogicalPlan::SemiJoin {
            left: Box::new(left),
            right: Box::new(right),
            left_key: col_ref(0, "a"),
            right_key: col_ref(0, "x"),
            residual: None,
            dynamic_filter_ids: Vec::new(),
        };
        let rewritten = rewrite(plan);
        assert!(
            matches!(rewritten, LogicalPlan::SemiJoin { residual: None, .. }),
            "plain SemiJoin must not be rewritten, got {rewritten}"
        );
    }

    #[test]
    fn residual_right_indices_are_shifted_by_one() {
        // Verify that the rewritten ON condition references the right
        // residual column at index l+1+r_idx (i.e. shifted by +1 to
        // skip the __rowid inserted between left and right).
        let plan = semi_with_residual();
        let rewritten = rewrite(plan);
        // Drill down to the Join's ON expression.
        let on = drill_to_join_on(&rewritten);
        // ON = (left.a = shifted_right.x) AND (left.b <> shifted_right.y)
        // The full ON is: AND(Eq(col 0, col 3), NotEq(col 1, col 4))
        //   - L=2, so __rowid at 2, right.x at 3, right.y at 4
        match &on {
            PlanExpr::BinaryOp {
                left,
                op: ast::BinaryOp::And,
                right,
                ..
            } => {
                // First conjunct: equi
                match left.as_ref() {
                    PlanExpr::BinaryOp {
                        left: lk,
                        op: ast::BinaryOp::Eq,
                        right: rk,
                        ..
                    } => {
                        assert_eq!(idx(lk), 0); // left.a stays at 0
                        assert_eq!(idx(rk), 3); // right.x shifted to L+1+0=3
                    }
                    other => panic!("expected equi (Eq), got {other}"),
                }
                // Second conjunct: residual
                match right.as_ref() {
                    PlanExpr::BinaryOp {
                        left: lk,
                        op: ast::BinaryOp::NotEq,
                        right: rk,
                        ..
                    } => {
                        assert_eq!(idx(lk), 1); // left.b stays at 1
                        assert_eq!(idx(rk), 4); // right.y was 3, shifted to 4
                    }
                    other => panic!("expected residual (NotEq), got {other}"),
                }
            }
            other => panic!("expected AND, got {other}"),
        }
    }

    fn idx(expr: &PlanExpr) -> usize {
        match expr {
            PlanExpr::Column { index, .. } => *index,
            other => panic!("expected Column, got {other}"),
        }
    }

    fn drill_to_join_on(plan: &LogicalPlan) -> PlanExpr {
        // Walk Projection -> Filter -> Aggregate -> Join, return its
        // ON expression.
        let proj_input = match plan {
            LogicalPlan::Projection { input, .. } => input,
            _ => panic!("expected Projection at top"),
        };
        let filter_input = match proj_input.as_ref() {
            LogicalPlan::Filter { input, .. } => input,
            _ => panic!("expected Filter under Projection"),
        };
        let agg_input = match filter_input.as_ref() {
            LogicalPlan::Aggregate { input, .. } => input,
            _ => panic!("expected Aggregate under Filter"),
        };
        match agg_input.as_ref() {
            LogicalPlan::Join {
                condition: JoinCondition::On(e),
                ..
            } => e.clone(),
            _ => panic!("expected Join with On condition"),
        }
    }
}
