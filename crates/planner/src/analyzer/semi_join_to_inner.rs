//! `SemiJoinToInnerJoin` analyzer pass.
//!
//! When a `SemiJoin`'s right side is **provably unique** on its
//! `right_key` (typically because the subquery is a `GROUP BY
//! right_key`-style aggregate), the SemiJoin's set-semantics
//! collapse to plain equi-join semantics: each left row matches at
//! most one right row, and the SemiJoin would emit the left row
//! once. An InnerJoin emits the same left rows in the same order
//! (modulo what JoinReorder later decides) but goes through arneb's
//! more heavily optimized `HashJoinExec` path — F-Perf2 parallel
//! probe, F-Perf6 row-encoded multi-col keys, F-Perf-IH inline
//! first-match, Step DF dynamic filters, Step VP vectorized probe.
//!
//! Mirrors Trino's
//! `io.trino.sql.planner.iterative.rule.TransformFilteringSemiJoinToInnerJoin`.
//!
//! ## Pattern
//!
//! ```text
//! SemiJoin { left, right, left_key, right_key: Column(i), residual: None }
//! where `right` matches:
//!   Projection / Filter / Sort / Limit
//!     ← Aggregate { group_by: [..., col@i', ...], ... }
//! and `right_key`'s index `i` traces through the Projection's
//! `Column`-only `exprs` back to a group_by column at index `i'`.
//! ```
//!
//! ## Rewrite
//!
//! ```text
//! Projection(keep first L left cols)
//!   ← Join { Inner, On(left_key = right_key + L), left, right }
//! ```
//!
//! The Projection at the top drops the right-side columns, restoring
//! the SemiJoin's original output schema (left only).
//!
//! ## Pipeline placement
//!
//! Runs BEFORE `JoinReorder` so the new Inner join can participate
//! in cost-based reorder with the rest of the inner-join chain.
//! Runs AFTER `TypeCoercion` so coercions on keys are already
//! applied. Runs BEFORE (any future re-enabling of)
//! `CorrelatedExistsToLeftJoin` so AntiJoin / SemiJoin-with-residual
//! get the residual rewrite while plain SemiJoins get this faster
//! rewrite.

use arneb_common::error::PlanError;
#[cfg(test)]
use arneb_common::types::ColumnInfo;
use arneb_sql_parser::ast;

use crate::analyzer::{AnalysisPass, AnalyzerContext};
use crate::plan::{JoinCondition, LogicalPlan, PlanExpr};

#[derive(Debug, Default)]
pub struct SemiJoinToInnerJoin;

impl SemiJoinToInnerJoin {
    pub fn new() -> Self {
        Self
    }
}

impl AnalysisPass for SemiJoinToInnerJoin {
    fn name(&self) -> &'static str {
        "SemiJoinToInnerJoin"
    }

    fn analyze(
        &self,
        plan: LogicalPlan,
        _ctx: &mut AnalyzerContext,
    ) -> Result<LogicalPlan, PlanError> {
        Ok(rewrite(plan))
    }
}

// ---------------------------------------------------------------------------
// Walker
// ---------------------------------------------------------------------------

fn rewrite(plan: LogicalPlan) -> LogicalPlan {
    let plan = recurse_children(plan, &mut rewrite);
    if let LogicalPlan::SemiJoin {
        left,
        right,
        left_key,
        right_key,
        residual: None,
        ..
    } = plan
    {
        if let Some(right_idx) = column_index(&right_key) {
            if column_traces_to_aggregate_group_by(&right, right_idx) {
                return rewrite_one(*left, *right, left_key, right_key);
            }
        }
        return LogicalPlan::SemiJoin {
            left,
            right,
            left_key,
            right_key,
            residual: None,
            dynamic_filter_ids: Vec::new(),
        };
    }
    plan
}

fn recurse_children<F: FnMut(LogicalPlan) -> LogicalPlan>(
    plan: LogicalPlan,
    f: &mut F,
) -> LogicalPlan {
    use LogicalPlan as L;
    match plan {
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
        L::TableScan { .. }
        | L::ExchangeNode { .. }
        | L::CreateTable { .. }
        | L::DropTable { .. }
        | L::DeleteFrom { .. }
        | L::DropView { .. }
        | L::OneRow => plan,
    }
}

// ---------------------------------------------------------------------------
// Uniqueness inference
// ---------------------------------------------------------------------------

fn column_index(expr: &PlanExpr) -> Option<usize> {
    if let PlanExpr::Column { index, .. } = expr {
        Some(*index)
    } else {
        None
    }
}

/// Return `true` if the column at `idx` in `plan`'s output schema
/// can be traced — through column-only Projections and row-preserving
/// nodes — back to a `group_by` column of an `Aggregate` underneath.
/// That column is unique by definition (GROUP BY produces one row per
/// distinct key tuple), so any downstream node that filters/sorts/
/// limits/projects-by-column preserves uniqueness on it.
fn column_traces_to_aggregate_group_by(plan: &LogicalPlan, idx: usize) -> bool {
    match plan {
        LogicalPlan::Aggregate { group_by, .. } => {
            // Aggregate's output schema is [group_by_cols.., aggr_cols..],
            // so any index in [0..group_by.len()) is a unique key.
            idx < group_by.len()
        }
        LogicalPlan::Projection { exprs, input, .. } => match exprs.get(idx) {
            Some(PlanExpr::Column { index: j, .. }) => {
                column_traces_to_aggregate_group_by(input, *j)
            }
            _ => false,
        },
        // Filter / Sort / Limit / Distinct preserve uniqueness of any
        // input column. (Distinct on a key superset preserves the
        // key's uniqueness.)
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Distinct { input } => column_traces_to_aggregate_group_by(input, idx),
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// Rewrite
// ---------------------------------------------------------------------------

fn rewrite_one(
    left: LogicalPlan,
    right: LogicalPlan,
    left_key: PlanExpr,
    right_key: PlanExpr,
) -> LogicalPlan {
    let left_schema = left.schema();
    let l = left_schema.len();

    let shifted_right_key = shift_columns(right_key, l);

    let on_expr = PlanExpr::BinaryOp {
        left: Box::new(left_key),
        op: ast::BinaryOp::Eq,
        right: Box::new(shifted_right_key),
        span: None,
    };

    let join_node = LogicalPlan::Join {
        left: Box::new(left),
        right: Box::new(right),
        join_type: ast::JoinType::Inner,
        condition: JoinCondition::On(on_expr),
        dynamic_filter_ids: Vec::new(),
    };

    // Wrap in a Projection that selects only the left cols so the
    // overall output schema matches the original SemiJoin's.
    let proj_exprs: Vec<PlanExpr> = (0..l)
        .map(|i| PlanExpr::Column {
            index: i,
            name: left_schema[i].name.clone(),
            span: None,
        })
        .collect();

    // Drop the `right_key` arg — replaced by the renamed version above.
    let _ = right_key;

    LogicalPlan::Projection {
        input: Box::new(join_node),
        exprs: proj_exprs,
        schema: left_schema,
    }
}

/// Shift every `Column { index }` in `expr` by `+delta`.
fn shift_columns(expr: PlanExpr, delta: usize) -> PlanExpr {
    use PlanExpr as E;
    match expr {
        E::Column { index, name, span } => E::Column {
            index: index + delta,
            name,
            span,
        },
        E::Literal { .. } | E::Parameter { .. } | E::Wildcard => expr,
        E::BinaryOp {
            left,
            op,
            right,
            span,
        } => E::BinaryOp {
            left: Box::new(shift_columns(*left, delta)),
            op,
            right: Box::new(shift_columns(*right, delta)),
            span,
        },
        E::UnaryOp { op, expr, span } => E::UnaryOp {
            op,
            expr: Box::new(shift_columns(*expr, delta)),
            span,
        },
        E::Function {
            name,
            args,
            distinct,
            span,
        } => E::Function {
            name,
            args: args.into_iter().map(|a| shift_columns(a, delta)).collect(),
            distinct,
            span,
        },
        E::IsNull { expr, span } => E::IsNull {
            expr: Box::new(shift_columns(*expr, delta)),
            span,
        },
        E::IsNotNull { expr, span } => E::IsNotNull {
            expr: Box::new(shift_columns(*expr, delta)),
            span,
        },
        E::Between {
            expr,
            negated,
            low,
            high,
            span,
        } => E::Between {
            expr: Box::new(shift_columns(*expr, delta)),
            negated,
            low: Box::new(shift_columns(*low, delta)),
            high: Box::new(shift_columns(*high, delta)),
            span,
        },
        E::InList {
            expr,
            list,
            negated,
            span,
        } => E::InList {
            expr: Box::new(shift_columns(*expr, delta)),
            list: list.into_iter().map(|e| shift_columns(e, delta)).collect(),
            negated,
            span,
        },
        E::Cast {
            expr,
            data_type,
            span,
        } => E::Cast {
            expr: Box::new(shift_columns(*expr, delta)),
            data_type,
            span,
        },
        E::CaseExpr {
            operand,
            when_clauses,
            else_result,
            span,
        } => E::CaseExpr {
            operand: operand.map(|o| Box::new(shift_columns(*o, delta))),
            when_clauses: when_clauses
                .into_iter()
                .map(|(w, t)| (shift_columns(w, delta), shift_columns(t, delta)))
                .collect(),
            else_result: else_result.map(|e| Box::new(shift_columns(*e, delta))),
            span,
        },
        E::ScalarSubquery { .. } => expr,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arneb_common::types::{DataType, TableReference};
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

    /// SemiJoin where right = `Projection(l_orderkey) ← Filter(SUM > 300)
    ///                         ← Aggregate(group_by=[l_orderkey],
    ///                                     aggr=[SUM(l_quantity)])`.
    /// Mirrors Q18's IN subquery shape.
    fn semi_q18_shape() -> LogicalPlan {
        let orders = scan(
            "orders",
            vec![
                col("o_orderkey", DataType::Int64),
                col("o_custkey", DataType::Int64),
            ],
        );
        let lineitem = scan(
            "lineitem",
            vec![
                col("l_orderkey", DataType::Int64),
                col("l_quantity", DataType::Float64),
            ],
        );
        let agg = LogicalPlan::Aggregate {
            input: Box::new(lineitem),
            group_by: vec![col_ref(0, "l_orderkey")],
            aggr_exprs: vec![PlanExpr::Function {
                name: "SUM".to_string(),
                args: vec![col_ref(1, "l_quantity")],
                distinct: false,
                span: None,
            }],
            schema: vec![
                col("l_orderkey", DataType::Int64),
                col("SUM(l_quantity)", DataType::Float64),
            ],
        };
        let filt = LogicalPlan::Filter {
            input: Box::new(agg),
            predicate: PlanExpr::BinaryOp {
                left: Box::new(col_ref(1, "SUM(l_quantity)")),
                op: ast::BinaryOp::Gt,
                right: Box::new(PlanExpr::Literal {
                    value: arneb_common::types::ScalarValue::Float64(300.0),
                    span: None,
                }),
                span: None,
            },
        };
        let proj = LogicalPlan::Projection {
            input: Box::new(filt),
            exprs: vec![col_ref(0, "l_orderkey")],
            schema: vec![col("l_orderkey", DataType::Int64)],
        };
        LogicalPlan::SemiJoin {
            left: Box::new(orders),
            right: Box::new(proj),
            left_key: col_ref(0, "o_orderkey"),
            right_key: col_ref(0, "l_orderkey"),
            residual: None,
            dynamic_filter_ids: Vec::new(),
        }
    }

    #[test]
    fn rewrites_q18_semi_to_projection_over_inner_join() {
        let plan = semi_q18_shape();
        let after = rewrite(plan);
        // Top-level must be Projection that keeps left cols (orders' 2 cols).
        match &after {
            LogicalPlan::Projection { schema, input, .. } => {
                assert_eq!(schema.len(), 2);
                assert_eq!(schema[0].name, "o_orderkey");
                assert_eq!(schema[1].name, "o_custkey");
                // Below: must be Inner Join (was SemiJoin).
                match input.as_ref() {
                    LogicalPlan::Join {
                        join_type,
                        condition,
                        ..
                    } => {
                        assert_eq!(*join_type, ast::JoinType::Inner);
                        // ON o_orderkey (idx 0) = l_orderkey (idx shifted to 2).
                        match condition {
                            JoinCondition::On(PlanExpr::BinaryOp { left, right, .. }) => {
                                assert!(matches!(left.as_ref(), PlanExpr::Column { index: 0, .. }));
                                assert!(matches!(
                                    right.as_ref(),
                                    PlanExpr::Column { index: 2, .. }
                                ));
                            }
                            other => panic!("expected ON BinaryOp, got {other:?}"),
                        }
                    }
                    other => panic!("expected Inner Join under Projection, got {other}"),
                }
            }
            other => panic!("expected top-level Projection, got {other}"),
        }
    }

    #[test]
    fn leaves_semi_with_non_unique_right_unchanged() {
        // Right is a plain TableScan — no Aggregate underneath, so
        // right_key isn't provably unique. Must stay as SemiJoin.
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
        let after = rewrite(plan);
        assert!(
            matches!(after, LogicalPlan::SemiJoin { residual: None, .. }),
            "non-unique right should stay as SemiJoin, got {after}"
        );
    }

    #[test]
    fn leaves_semi_with_residual_unchanged() {
        // Even when right is uniqueness-provable, presence of residual
        // means we still need SemiJoin's residual evaluation semantics.
        // The rewrite is sound only when residual is None.
        let left = scan("l", vec![col("a", DataType::Int64)]);
        let lineitem = scan("lineitem", vec![col("l_orderkey", DataType::Int64)]);
        let right = LogicalPlan::Aggregate {
            input: Box::new(lineitem),
            group_by: vec![col_ref(0, "l_orderkey")],
            aggr_exprs: vec![],
            schema: vec![col("l_orderkey", DataType::Int64)],
        };
        let plan = LogicalPlan::SemiJoin {
            left: Box::new(left),
            right: Box::new(right),
            left_key: col_ref(0, "a"),
            right_key: col_ref(0, "l_orderkey"),
            residual: Some(PlanExpr::Literal {
                value: arneb_common::types::ScalarValue::Boolean(true),
                span: None,
            }),
            dynamic_filter_ids: Vec::new(),
        };
        let after = rewrite(plan);
        assert!(
            matches!(
                after,
                LogicalPlan::SemiJoin {
                    residual: Some(_),
                    ..
                }
            ),
            "SemiJoin with residual should stay unchanged, got {after}"
        );
    }

    #[test]
    fn rewrites_aggregate_directly_under_semi() {
        // Right = Aggregate(group_by=[x]) directly; no Projection.
        let left = scan("l", vec![col("a", DataType::Int64)]);
        let lineitem = scan("lineitem", vec![col("l_orderkey", DataType::Int64)]);
        let right = LogicalPlan::Aggregate {
            input: Box::new(lineitem),
            group_by: vec![col_ref(0, "l_orderkey")],
            aggr_exprs: vec![],
            schema: vec![col("l_orderkey", DataType::Int64)],
        };
        let plan = LogicalPlan::SemiJoin {
            left: Box::new(left),
            right: Box::new(right),
            left_key: col_ref(0, "a"),
            right_key: col_ref(0, "l_orderkey"),
            residual: None,
            dynamic_filter_ids: Vec::new(),
        };
        let after = rewrite(plan);
        assert!(
            matches!(after, LogicalPlan::Projection { .. }),
            "Aggregate-direct-under-Semi should rewrite to Projection(Join), got {after}"
        );
    }

    #[test]
    fn rewrites_recurse_into_nested_semi() {
        // Verify the bottom-up walker rewrites a SemiJoin nested under
        // a Filter+Projection chain.
        let inner = semi_q18_shape();
        let wrapped = LogicalPlan::Filter {
            input: Box::new(inner),
            predicate: PlanExpr::Literal {
                value: arneb_common::types::ScalarValue::Boolean(true),
                span: None,
            },
        };
        let after = rewrite(wrapped);
        // Expect Filter ← Projection ← Join
        match after {
            LogicalPlan::Filter { input, .. } => match *input {
                LogicalPlan::Projection { input, .. } => {
                    assert!(matches!(*input, LogicalPlan::Join { .. }));
                }
                other => panic!("expected Projection under Filter, got {other}"),
            },
            other => panic!("expected Filter at top, got {other}"),
        }
    }
}
