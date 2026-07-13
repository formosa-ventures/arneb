//! `SemiJoinDedupBuild` — collapse SemiJoin/AntiJoin build sides to
//! the distinct combinations of columns the join key + residual
//! actually read.
//!
//! Trino-inspired (per `TransformFilteringSemiJoinToInnerJoin` +
//! decorrelation pipeline research): for a non-trivial residual the
//! build side is often a high-row-count table whose useful
//! contribution is a much smaller set of distinct (key, residual-col)
//! tuples. Q21's `EXISTS (SELECT * FROM lineitem l2 WHERE l2.l_orderkey
//! = l1.l_orderkey AND l2.l_suppkey <> l1.l_suppkey)` reads only
//! `(l_orderkey, l_suppkey)` of 6M lineitem rows → ~30K distinct pairs.
//!
//! This pass wraps the right child in an empty-aggregate (GROUP BY the
//! referenced cols, no agg expressions) so the SemiJoinExec hash build
//! probes a much smaller, deduped set. Indices in `right_key` and the
//! residual are rewritten to point at the projected layout.
//!
//! Only fires when `residual.is_some()` — for the bare hash-set semi
//! path the build is already a cheap u64-set probe; adding an
//! aggregation would re-hash without payoff.

use std::collections::{HashMap, HashSet};

use arneb_common::error::PlanError;
use arneb_common::types::ColumnInfo;

use crate::analyzer::{AnalysisPass, AnalyzerContext};
use crate::plan::{LogicalPlan, PlanExpr};

pub struct SemiJoinDedupBuild;

impl SemiJoinDedupBuild {
    pub fn new() -> Self {
        Self
    }
}

impl Default for SemiJoinDedupBuild {
    fn default() -> Self {
        Self::new()
    }
}

impl AnalysisPass for SemiJoinDedupBuild {
    fn name(&self) -> &'static str {
        "SemiJoinDedupBuild"
    }

    fn analyze(
        &self,
        plan: LogicalPlan,
        _ctx: &mut AnalyzerContext,
    ) -> Result<LogicalPlan, PlanError> {
        Ok(rewrite(plan))
    }
}

fn rewrite(plan: LogicalPlan) -> LogicalPlan {
    use LogicalPlan as L;

    match plan {
        L::SemiJoin {
            left,
            right,
            left_key,
            right_key,
            residual,
            ..
        } => {
            let left = Box::new(rewrite(*left));
            let right = Box::new(rewrite(*right));
            match residual {
                None => L::SemiJoin {
                    left,
                    right,
                    left_key,
                    right_key,
                    residual: None,
                    dynamic_filter_ids: Vec::new(),
                },
                Some(r) => {
                    let (new_right, new_right_key, new_residual) =
                        dedup_build_side(*right, &left_key, right_key, r, left.schema().len());
                    L::SemiJoin {
                        left,
                        right: Box::new(new_right),
                        left_key,
                        right_key: new_right_key,
                        residual: Some(new_residual),
                        dynamic_filter_ids: Vec::new(),
                    }
                }
            }
        }
        L::AntiJoin {
            left,
            right,
            left_key,
            right_key,
            residual,
        } => {
            let left = Box::new(rewrite(*left));
            let right = Box::new(rewrite(*right));
            match residual {
                None => L::AntiJoin {
                    left,
                    right,
                    left_key,
                    right_key,
                    residual: None,
                },
                Some(r) => {
                    let (new_right, new_right_key, new_residual) =
                        dedup_build_side(*right, &left_key, right_key, r, left.schema().len());
                    L::AntiJoin {
                        left,
                        right: Box::new(new_right),
                        left_key,
                        right_key: new_right_key,
                        residual: Some(new_residual),
                    }
                }
            }
        }
        L::Filter { input, predicate } => L::Filter {
            input: Box::new(rewrite(*input)),
            predicate,
        },
        L::Projection {
            input,
            exprs,
            schema,
        } => L::Projection {
            input: Box::new(rewrite(*input)),
            exprs,
            schema,
        },
        L::Aggregate {
            input,
            group_by,
            aggr_exprs,
            schema,
        } => L::Aggregate {
            input: Box::new(rewrite(*input)),
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
            input: Box::new(rewrite(*input)),
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
            input: Box::new(rewrite(*input)),
            group_by,
            aggr_exprs,
            schema,
        },
        L::Sort { input, order_by } => L::Sort {
            input: Box::new(rewrite(*input)),
            order_by,
        },
        L::Limit {
            input,
            limit,
            offset,
        } => L::Limit {
            input: Box::new(rewrite(*input)),
            limit,
            offset,
        },
        L::Distinct { input } => L::Distinct {
            input: Box::new(rewrite(*input)),
        },
        L::Join {
            left,
            right,
            join_type,
            condition,
            ..
        } => L::Join {
            left: Box::new(rewrite(*left)),
            right: Box::new(rewrite(*right)),
            join_type,
            condition,
            dynamic_filter_ids: Vec::new(),
        },
        L::UnionAll { inputs } => L::UnionAll {
            inputs: inputs.into_iter().map(rewrite).collect(),
        },
        L::Intersect { left, right } => L::Intersect {
            left: Box::new(rewrite(*left)),
            right: Box::new(rewrite(*right)),
        },
        L::Except { left, right } => L::Except {
            left: Box::new(rewrite(*left)),
            right: Box::new(rewrite(*right)),
        },
        L::ScalarSubquery { subplan } => L::ScalarSubquery {
            subplan: Box::new(rewrite(*subplan)),
        },
        L::Window { input, functions } => L::Window {
            input: Box::new(rewrite(*input)),
            functions,
        },
        L::Explain { input, analyze } => L::Explain {
            input: Box::new(rewrite(*input)),
            analyze,
        },
        // Leaves and non-recursive variants.
        other => other,
    }
}

/// Build the deduplicated right side and the rewritten right_key /
/// residual. The output schema of the new right is
/// `[right_key_col, ...other residual-referenced right cols]` —
/// right_key_col always first so the rewritten right_key is
/// `Column { index: 0 }`.
fn dedup_build_side(
    right: LogicalPlan,
    _left_key: &PlanExpr,
    right_key: PlanExpr,
    residual: PlanExpr,
    left_width: usize,
) -> (LogicalPlan, PlanExpr, PlanExpr) {
    let right_schema = right.schema();
    let right_width = right_schema.len();

    // Indices in `right` that the right_key references.
    let mut needed_right: Vec<usize> = Vec::new();
    let mut seen: HashSet<usize> = HashSet::new();
    let mut push = |idx: usize| {
        if seen.insert(idx) {
            needed_right.push(idx);
        }
    };
    collect_indices(&right_key, &mut |idx| push(idx));

    // Indices the residual references (in the joined layout). Keep
    // only the right-side portion; left-side indices are unchanged.
    collect_indices(&residual, &mut |idx| {
        if idx >= left_width {
            let r = idx - left_width;
            if r < right_width {
                push(r);
            }
        }
    });

    needed_right.sort();
    // If the right_key is a multi-column expression that hits the
    // same column twice, dedup. Already covered by `seen`.

    // If we'd keep ALL columns there's nothing to gain — bail out
    // (avoids inserting a no-op aggregate).
    if needed_right.len() == right_width {
        return (right, right_key, residual);
    }

    // Build the project list — one Column per needed_right index,
    // in needed_right order.
    let mut proj_exprs: Vec<PlanExpr> = Vec::with_capacity(needed_right.len());
    let mut proj_schema: Vec<ColumnInfo> = Vec::with_capacity(needed_right.len());
    let mut old_to_new: HashMap<usize, usize> = HashMap::new();
    for (new_pos, &old_idx) in needed_right.iter().enumerate() {
        proj_exprs.push(PlanExpr::Column {
            index: old_idx,
            name: right_schema[old_idx].name.clone(),
            span: None,
        });
        proj_schema.push(right_schema[old_idx].clone());
        old_to_new.insert(old_idx, new_pos);
    }

    // Wrap right in a Projection so the dedup aggregate has only the
    // needed columns. Then GROUP BY all of them (empty agg list ⇒
    // DISTINCT semantics).
    let projected = LogicalPlan::Projection {
        input: Box::new(right),
        exprs: proj_exprs,
        schema: proj_schema.clone(),
    };

    // GROUP BY references the projected layout (post-Projection
    // indices are 0..n in needed_right order).
    let group_by: Vec<PlanExpr> = (0..needed_right.len())
        .map(|i| PlanExpr::Column {
            index: i,
            name: proj_schema[i].name.clone(),
            span: None,
        })
        .collect();
    let aggregated = LogicalPlan::Aggregate {
        input: Box::new(projected),
        group_by: group_by.clone(),
        aggr_exprs: Vec::new(),
        schema: proj_schema,
    };

    // Rewrite right_key indices: in the new aggregated schema,
    // column N appears at position `old_to_new[old_idx]`.
    let new_right_key = remap_right_indices(right_key, &old_to_new);

    // Rewrite residual: split into left part (indices < left_width,
    // unchanged) and right part (indices >= left_width, remapped to
    // left_width + new_pos).
    let new_residual = remap_residual(residual, left_width, &old_to_new);

    (aggregated, new_right_key, new_residual)
}

/// Walk a [`PlanExpr`] and invoke `cb` once per `Column` leaf with
/// its `index`.
fn collect_indices<F: FnMut(usize)>(expr: &PlanExpr, cb: &mut F) {
    match expr {
        PlanExpr::Column { index, .. } => cb(*index),
        PlanExpr::Literal { .. } | PlanExpr::Parameter { .. } | PlanExpr::Wildcard => {}
        PlanExpr::BinaryOp { left, right, .. } => {
            collect_indices(left, cb);
            collect_indices(right, cb);
        }
        PlanExpr::UnaryOp { expr, .. } => collect_indices(expr, cb),
        PlanExpr::Function { args, .. } => {
            for a in args {
                collect_indices(a, cb);
            }
        }
        PlanExpr::IsNull { expr, .. } | PlanExpr::IsNotNull { expr, .. } => {
            collect_indices(expr, cb);
        }
        PlanExpr::InList { expr, list, .. } => {
            collect_indices(expr, cb);
            for e in list {
                collect_indices(e, cb);
            }
        }
        PlanExpr::Between {
            expr, low, high, ..
        } => {
            collect_indices(expr, cb);
            collect_indices(low, cb);
            collect_indices(high, cb);
        }
        PlanExpr::CaseExpr {
            operand,
            when_clauses,
            else_result,
            ..
        } => {
            if let Some(op) = operand {
                collect_indices(op, cb);
            }
            for (w, t) in when_clauses {
                collect_indices(w, cb);
                collect_indices(t, cb);
            }
            if let Some(e) = else_result {
                collect_indices(e, cb);
            }
        }
        PlanExpr::Cast { expr, .. } => collect_indices(expr, cb),
        PlanExpr::ScalarSubquery { .. } => {}
    }
}

/// Rewrite every `Column { index }` reference whose index appears in
/// `remap` to its mapped position. Used for the right_key — indices
/// reference the right schema directly (not the joined layout).
fn remap_right_indices(expr: PlanExpr, remap: &HashMap<usize, usize>) -> PlanExpr {
    walk_remap(expr, &|i| remap.get(&i).copied())
}

/// Rewrite a residual planned against `[left ++ right]` so its
/// right-side references land at `[left ++ projected_right]`. Left
/// indices (< left_width) are unchanged.
fn remap_residual(
    expr: PlanExpr,
    left_width: usize,
    right_old_to_new: &HashMap<usize, usize>,
) -> PlanExpr {
    walk_remap(expr, &|i| {
        if i < left_width {
            None
        } else {
            let r = i - left_width;
            right_old_to_new.get(&r).map(|&new_r| left_width + new_r)
        }
    })
}

fn walk_remap(expr: PlanExpr, f: &dyn Fn(usize) -> Option<usize>) -> PlanExpr {
    match expr {
        PlanExpr::Column { index, name, span } => PlanExpr::Column {
            index: f(index).unwrap_or(index),
            name,
            span,
        },
        PlanExpr::Literal { .. } | PlanExpr::Parameter { .. } | PlanExpr::Wildcard => expr,
        PlanExpr::BinaryOp {
            left,
            op,
            right,
            span,
        } => PlanExpr::BinaryOp {
            left: Box::new(walk_remap(*left, f)),
            op,
            right: Box::new(walk_remap(*right, f)),
            span,
        },
        PlanExpr::UnaryOp { op, expr, span } => PlanExpr::UnaryOp {
            op,
            expr: Box::new(walk_remap(*expr, f)),
            span,
        },
        PlanExpr::Function {
            name,
            args,
            distinct,
            span,
        } => PlanExpr::Function {
            name,
            args: args.into_iter().map(|a| walk_remap(a, f)).collect(),
            distinct,
            span,
        },
        PlanExpr::IsNull { expr, span } => PlanExpr::IsNull {
            expr: Box::new(walk_remap(*expr, f)),
            span,
        },
        PlanExpr::IsNotNull { expr, span } => PlanExpr::IsNotNull {
            expr: Box::new(walk_remap(*expr, f)),
            span,
        },
        PlanExpr::InList {
            expr,
            list,
            negated,
            span,
        } => PlanExpr::InList {
            expr: Box::new(walk_remap(*expr, f)),
            list: list.into_iter().map(|e| walk_remap(e, f)).collect(),
            negated,
            span,
        },
        PlanExpr::Between {
            expr,
            low,
            high,
            negated,
            span,
        } => PlanExpr::Between {
            expr: Box::new(walk_remap(*expr, f)),
            low: Box::new(walk_remap(*low, f)),
            high: Box::new(walk_remap(*high, f)),
            negated,
            span,
        },
        PlanExpr::CaseExpr {
            operand,
            when_clauses,
            else_result,
            span,
        } => PlanExpr::CaseExpr {
            operand: operand.map(|o| Box::new(walk_remap(*o, f))),
            when_clauses: when_clauses
                .into_iter()
                .map(|(w, t)| (walk_remap(w, f), walk_remap(t, f)))
                .collect(),
            else_result: else_result.map(|e| Box::new(walk_remap(*e, f))),
            span,
        },
        PlanExpr::Cast {
            expr,
            data_type,
            span,
        } => PlanExpr::Cast {
            expr: Box::new(walk_remap(*expr, f)),
            data_type,
            span,
        },
        PlanExpr::ScalarSubquery { .. } => expr,
    }
}
