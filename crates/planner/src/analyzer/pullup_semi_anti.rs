//! Gated SemiJoin / AntiJoin pull-up through row-preserving inner joins.
//!
//! Target shape after `JoinReorder` for TPC-H Q21:
//!
//! ```text
//! InnerJoin(InnerJoin(SemiOrAnti(L, SUB), R1), R2)
//! ```
//!
//! When each inner join is provably many-to-one from the accumulated left side
//! to the newly-added right side, applying the semi/anti predicate after the
//! inner joins is equivalent and can shrink the semi/anti probe side by orders
//! of magnitude:
//!
//! ```text
//! SemiOrAnti(InnerJoin(InnerJoin(L, R1), R2), SUB)
//! ```
//!
//! The pass is deliberately narrow. If it cannot prove row preservation or
//! remap every referenced column mechanically, it leaves the plan unchanged.

use std::sync::OnceLock;

use arneb_common::error::PlanError;
use arneb_common::types::TableReference;
use arneb_sql_parser::ast::{BinaryOp, JoinType};

use crate::analyzer::{AnalysisPass, AnalyzerContext};
use crate::cost::CatalogStats;
use crate::plan::{JoinCondition, LogicalPlan, PlanExpr};

#[derive(Debug, Default)]
pub struct PullupSemiAnti;

impl PullupSemiAnti {
    pub fn new() -> Self {
        Self
    }
}

impl AnalysisPass for PullupSemiAnti {
    fn name(&self) -> &'static str {
        "PullupSemiAnti"
    }

    fn analyze(
        &self,
        plan: LogicalPlan,
        ctx: &mut AnalyzerContext,
    ) -> Result<LogicalPlan, PlanError> {
        if !pullup_semi_anti_enabled() {
            return Ok(plan);
        }
        Ok(rewrite(plan, ctx.catalog_stats.as_ref()))
    }
}

fn pullup_semi_anti_enabled() -> bool {
    #[cfg(test)]
    if let Some(enabled) = pullup_semi_anti_test_override() {
        return enabled;
    }

    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        let enabled = std::env::var("ARNEB_PULLUP_SEMI_ANTI")
            .map(|v| v == "1")
            .unwrap_or(false);
        tracing::info!(
            target: "arneb::config",
            ARNEB_PULLUP_SEMI_ANTI = enabled,
            "ARNEB_PULLUP_SEMI_ANTI effective value (default off; =1 to enable semi/anti pull-up)"
        );
        enabled
    })
}

#[cfg(test)]
static PULLUP_TEST_OVERRIDE: OnceLock<std::sync::Mutex<Option<bool>>> = OnceLock::new();
#[cfg(test)]
static PULLUP_TEST_LOCK: OnceLock<std::sync::Mutex<()>> = OnceLock::new();

#[cfg(test)]
struct PullupOverride {
    _guard: std::sync::MutexGuard<'static, ()>,
}

#[cfg(test)]
impl Drop for PullupOverride {
    fn drop(&mut self) {
        *PULLUP_TEST_OVERRIDE
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("pullup semi/anti test override lock poisoned") = None;
    }
}

#[cfg(test)]
fn pullup_semi_anti_test_override() -> Option<bool> {
    *PULLUP_TEST_OVERRIDE
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("pullup semi/anti test override lock poisoned")
}

#[cfg(test)]
fn set_pullup_semi_anti_for_test(enabled: bool) -> PullupOverride {
    let guard = PULLUP_TEST_LOCK
        .get_or_init(|| std::sync::Mutex::new(()))
        .lock()
        .expect("pullup semi/anti test lock poisoned");
    *PULLUP_TEST_OVERRIDE
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("pullup semi/anti test override lock poisoned") = Some(enabled);
    PullupOverride { _guard: guard }
}

fn rewrite(plan: LogicalPlan, stats: &CatalogStats) -> LogicalPlan {
    if let Some(pulled) = try_pullup(plan.clone(), stats) {
        return pulled;
    }
    recurse_children(plan, &mut |child| rewrite(child, stats))
}

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

#[derive(Clone)]
enum SemiAnti {
    Semi {
        right: LogicalPlan,
        left_key: PlanExpr,
        right_key: PlanExpr,
        residual: Option<PlanExpr>,
    },
    Anti {
        right: LogicalPlan,
        left_key: PlanExpr,
        right_key: PlanExpr,
        residual: Option<PlanExpr>,
    },
}

fn try_pullup(plan: LogicalPlan, stats: &CatalogStats) -> Option<LogicalPlan> {
    let (new_left, semi_anti, old_left_width, added_width) = extract_left_deep(plan, stats)?;

    let right_width = semi_anti.right().schema().len();
    match semi_anti {
        SemiAnti::Semi {
            right,
            left_key,
            right_key,
            residual,
        } => {
            if !expr_refs_within(&left_key, old_left_width) {
                return None;
            }
            let residual = remap_residual(residual, old_left_width, right_width, added_width)?;
            Some(LogicalPlan::SemiJoin {
                left: Box::new(new_left),
                right: Box::new(right),
                left_key,
                right_key,
                residual,
                dynamic_filter_ids: Vec::new(),
            })
        }
        SemiAnti::Anti {
            right,
            left_key,
            right_key,
            residual,
        } => {
            if !expr_refs_within(&left_key, old_left_width) {
                return None;
            }
            let residual = remap_residual(residual, old_left_width, right_width, added_width)?;
            Some(LogicalPlan::AntiJoin {
                left: Box::new(new_left),
                right: Box::new(right),
                left_key,
                right_key,
                residual,
            })
        }
    }
}

impl SemiAnti {
    fn right(&self) -> &LogicalPlan {
        match self {
            Self::Semi { right, .. } | Self::Anti { right, .. } => right,
        }
    }
}

fn extract_left_deep(
    plan: LogicalPlan,
    stats: &CatalogStats,
) -> Option<(LogicalPlan, SemiAnti, usize, usize)> {
    match plan {
        LogicalPlan::SemiJoin {
            left,
            right,
            left_key,
            right_key,
            residual,
            ..
        } => {
            if matches!(left.as_ref(), LogicalPlan::Join { .. }) {
                return None;
            }
            let old_left_width = left.schema().len();
            Some((
                *left,
                SemiAnti::Semi {
                    right: *right,
                    left_key,
                    right_key,
                    residual,
                },
                old_left_width,
                0,
            ))
        }
        LogicalPlan::AntiJoin {
            left,
            right,
            left_key,
            right_key,
            residual,
        } => {
            if matches!(left.as_ref(), LogicalPlan::Join { .. }) {
                return None;
            }
            let old_left_width = left.schema().len();
            Some((
                *left,
                SemiAnti::Anti {
                    right: *right,
                    left_key,
                    right_key,
                    residual,
                },
                old_left_width,
                0,
            ))
        }
        LogicalPlan::Join {
            left,
            right,
            join_type: JoinType::Inner,
            condition,
            ..
        } => {
            let (new_left_child, semi_anti, old_left_width, added_width) =
                extract_left_deep(*left, stats)?;
            if !join_is_many_to_one(&new_left_child, &right, &condition, stats) {
                return None;
            }
            let right_width = right.schema().len();
            let joined = LogicalPlan::Join {
                left: Box::new(new_left_child),
                right,
                join_type: JoinType::Inner,
                condition,
                dynamic_filter_ids: Vec::new(),
            };
            Some((joined, semi_anti, old_left_width, added_width + right_width))
        }
        _ => None,
    }
}

fn join_is_many_to_one(
    left: &LogicalPlan,
    right: &LogicalPlan,
    condition: &JoinCondition,
    stats: &CatalogStats,
) -> bool {
    let JoinCondition::On(expr) = condition else {
        return false;
    };
    let left_width = left.schema().len();
    let mut ok = false;
    for atom in split_and_atoms(expr) {
        if let Some((l_idx, r_idx)) = equality_sides(&atom, left_width) {
            if l_idx < left_width && column_unique(right, r_idx, stats) {
                ok = true;
                break;
            }
        }
    }
    ok
}

fn split_and_atoms(expr: &PlanExpr) -> Vec<PlanExpr> {
    match expr {
        PlanExpr::BinaryOp {
            left,
            op: BinaryOp::And,
            right,
            ..
        } => {
            let mut out = split_and_atoms(left);
            out.extend(split_and_atoms(right));
            out
        }
        _ => vec![expr.clone()],
    }
}

fn equality_sides(expr: &PlanExpr, left_width: usize) -> Option<(usize, usize)> {
    let PlanExpr::BinaryOp {
        left,
        op: BinaryOp::Eq,
        right,
        ..
    } = expr
    else {
        return None;
    };
    let l = column_index(left)?;
    let r = column_index(right)?;
    match (l < left_width, r < left_width) {
        (true, false) => Some((l, r - left_width)),
        (false, true) => Some((r, l - left_width)),
        _ => None,
    }
}

fn column_index(expr: &PlanExpr) -> Option<usize> {
    if let PlanExpr::Column { index, .. } = expr {
        Some(*index)
    } else {
        None
    }
}

fn column_unique(plan: &LogicalPlan, idx: usize, stats: &CatalogStats) -> bool {
    match plan {
        LogicalPlan::Aggregate { group_by, .. }
        | LogicalPlan::PartialAggregate { group_by, .. }
        | LogicalPlan::FinalAggregate { group_by, .. } => idx < group_by.len(),
        LogicalPlan::Projection { input, exprs, .. } => match exprs.get(idx) {
            Some(PlanExpr::Column { index, .. }) => column_unique(input, *index, stats),
            _ => false,
        },
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Distinct { input }
        | LogicalPlan::Explain { input, .. } => column_unique(input, idx, stats),
        LogicalPlan::TableScan { table, schema, .. } => schema
            .get(idx)
            .and_then(|c| table_column_unique(table, &c.name, stats))
            .unwrap_or(false),
        _ => false,
    }
}

fn table_column_unique(table: &TableReference, column: &str, stats: &CatalogStats) -> Option<bool> {
    let table_stats = stats.get(table)?;
    let row_count = table_stats.row_count?;
    let ndv = table_stats.columns.get(column)?.ndv?;
    Some(ndv >= row_count)
}

fn remap_residual(
    residual: Option<PlanExpr>,
    old_left_width: usize,
    right_width: usize,
    added_width: usize,
) -> Option<Option<PlanExpr>> {
    match residual {
        Some(r) => rewrite_expr(r, &mut |idx, name, span| {
            let new_idx = if idx < old_left_width {
                idx
            } else if idx < old_left_width + right_width {
                idx + added_width
            } else {
                return None;
            };
            Some(PlanExpr::Column {
                index: new_idx,
                name,
                span,
            })
        })
        .map(Some),
        None => Some(None),
    }
}

fn expr_refs_within(expr: &PlanExpr, width: usize) -> bool {
    let mut ok = true;
    let _ = rewrite_expr(expr.clone(), &mut |idx, name, span| {
        if idx >= width {
            ok = false;
        }
        Some(PlanExpr::Column {
            index: idx,
            name,
            span,
        })
    });
    ok
}

fn rewrite_expr<F>(expr: PlanExpr, f: &mut F) -> Option<PlanExpr>
where
    F: FnMut(usize, String, Option<arneb_sql_parser::Span>) -> Option<PlanExpr>,
{
    use PlanExpr as E;
    Some(match expr {
        E::Column { index, name, span } => f(index, name, span)?,
        E::Literal { .. } | E::Parameter { .. } | E::Wildcard => expr,
        E::BinaryOp {
            left,
            op,
            right,
            span,
        } => E::BinaryOp {
            left: Box::new(rewrite_expr(*left, f)?),
            op,
            right: Box::new(rewrite_expr(*right, f)?),
            span,
        },
        E::UnaryOp { op, expr, span } => E::UnaryOp {
            op,
            expr: Box::new(rewrite_expr(*expr, f)?),
            span,
        },
        E::Function {
            name,
            args,
            distinct,
            span,
        } => E::Function {
            name,
            args: args
                .into_iter()
                .map(|a| rewrite_expr(a, f))
                .collect::<Option<Vec<_>>>()?,
            distinct,
            span,
        },
        E::IsNull { expr, span } => E::IsNull {
            expr: Box::new(rewrite_expr(*expr, f)?),
            span,
        },
        E::IsNotNull { expr, span } => E::IsNotNull {
            expr: Box::new(rewrite_expr(*expr, f)?),
            span,
        },
        E::Between {
            expr,
            negated,
            low,
            high,
            span,
        } => E::Between {
            expr: Box::new(rewrite_expr(*expr, f)?),
            negated,
            low: Box::new(rewrite_expr(*low, f)?),
            high: Box::new(rewrite_expr(*high, f)?),
            span,
        },
        E::InList {
            expr,
            list,
            negated,
            span,
        } => E::InList {
            expr: Box::new(rewrite_expr(*expr, f)?),
            list: list
                .into_iter()
                .map(|a| rewrite_expr(a, f))
                .collect::<Option<Vec<_>>>()?,
            negated,
            span,
        },
        E::Cast {
            expr,
            data_type,
            span,
        } => E::Cast {
            expr: Box::new(rewrite_expr(*expr, f)?),
            data_type,
            span,
        },
        E::CaseExpr {
            operand,
            when_clauses,
            else_result,
            span,
        } => E::CaseExpr {
            operand: match operand {
                Some(e) => Some(Box::new(rewrite_expr(*e, f)?)),
                None => None,
            },
            when_clauses: when_clauses
                .into_iter()
                .map(|(w, t)| Some((rewrite_expr(w, f)?, rewrite_expr(t, f)?)))
                .collect::<Option<Vec<_>>>()?,
            else_result: match else_result {
                Some(e) => Some(Box::new(rewrite_expr(*e, f)?)),
                None => None,
            },
            span,
        },
        E::ScalarSubquery { .. } => expr,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arneb_catalog::{ColumnStatistics, TableStatistics};
    use arneb_common::types::{ColumnInfo, DataType, ScalarValue};
    use std::collections::HashMap;
    use std::sync::Arc;

    fn col(name: &str) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn scan(table: &str, cols: &[&str]) -> LogicalPlan {
        LogicalPlan::TableScan {
            table: TableReference::table(table),
            schema: cols.iter().map(|c| col(c)).collect(),
            alias: None,
            properties: HashMap::new(),
            dynamic_filters_consumed: Vec::new(),
        }
    }

    fn col_ref(index: usize, name: &str) -> PlanExpr {
        PlanExpr::Column {
            index,
            name: name.to_string(),
            span: None,
        }
    }

    fn lit_bool(value: bool) -> PlanExpr {
        PlanExpr::Literal {
            value: ScalarValue::Boolean(value),
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

    fn ne(left: PlanExpr, right: PlanExpr) -> PlanExpr {
        PlanExpr::BinaryOp {
            left: Box::new(left),
            op: BinaryOp::NotEq,
            right: Box::new(right),
            span: None,
        }
    }

    fn inner(left: LogicalPlan, right: LogicalPlan, condition: PlanExpr) -> LogicalPlan {
        LogicalPlan::Join {
            left: Box::new(left),
            right: Box::new(right),
            join_type: JoinType::Inner,
            condition: JoinCondition::On(condition),
            dynamic_filter_ids: Vec::new(),
        }
    }

    fn unique_stats() -> Arc<CatalogStats> {
        let mut stats = CatalogStats::new();
        for (table, rows, unique_cols) in [
            ("l1", 100_u64, vec![]),
            ("sub", 100_u64, vec![]),
            ("orders", 100_u64, vec!["o_orderkey"]),
            ("nation", 25_u64, vec!["n_nationkey"]),
            ("many", 100_u64, vec![]),
        ] {
            let mut columns = HashMap::new();
            for c in unique_cols {
                columns.insert(
                    c.to_string(),
                    ColumnStatistics {
                        ndv: Some(rows),
                        ..ColumnStatistics::default()
                    },
                );
            }
            stats.insert(
                TableReference::table(table),
                TableStatistics {
                    row_count: Some(rows),
                    size_bytes: None,
                    columns,
                },
            );
        }
        Arc::new(stats)
    }

    fn anti_chain() -> LogicalPlan {
        let l1 = scan("l1", &["l_orderkey", "l_suppkey"]);
        let sub = scan("sub", &["l_orderkey", "l_suppkey"]);
        let anti = LogicalPlan::AntiJoin {
            left: Box::new(l1),
            right: Box::new(sub),
            left_key: col_ref(0, "l_orderkey"),
            right_key: col_ref(0, "l_orderkey"),
            residual: Some(ne(col_ref(1, "l_suppkey"), col_ref(3, "l_suppkey"))),
        };
        let orders = scan("orders", &["o_orderkey", "o_custkey"]);
        let nation = scan("nation", &["n_nationkey", "n_name"]);
        let with_orders = inner(
            anti,
            orders,
            eq(col_ref(0, "l_orderkey"), col_ref(2, "o_orderkey")),
        );
        inner(
            with_orders,
            nation,
            eq(col_ref(3, "o_custkey"), col_ref(4, "n_nationkey")),
        )
    }

    fn run_pass(plan: LogicalPlan, enabled: bool) -> LogicalPlan {
        let _override = set_pullup_semi_anti_for_test(enabled);
        let mut ctx = AnalyzerContext::with_stats(unique_stats());
        PullupSemiAnti::new().analyze(plan, &mut ctx).unwrap()
    }

    #[test]
    fn anti_join_pulls_above_inner_chain_and_remaps_residual() {
        let after = run_pass(anti_chain(), true);
        let LogicalPlan::AntiJoin {
            left,
            left_key,
            residual,
            ..
        } = after
        else {
            panic!("expected top AntiJoin");
        };
        assert!(matches!(left.as_ref(), LogicalPlan::Join { .. }));
        assert!(matches!(left_key, PlanExpr::Column { index: 0, .. }));

        let residual = residual.expect("residual must survive");
        let mut refs = Vec::new();
        let _ = rewrite_expr(residual, &mut |idx, name, span| {
            refs.push(idx);
            Some(PlanExpr::Column {
                index: idx,
                name,
                span,
            })
        });
        assert_eq!(refs, vec![1, 7]);
    }

    #[test]
    fn semi_join_pulls_above_inner_chain() {
        let l1 = scan("l1", &["l_orderkey", "l_suppkey"]);
        let sub = scan("sub", &["l_orderkey"]);
        let semi = LogicalPlan::SemiJoin {
            left: Box::new(l1),
            right: Box::new(sub),
            left_key: col_ref(0, "l_orderkey"),
            right_key: col_ref(0, "l_orderkey"),
            residual: None,
            dynamic_filter_ids: Vec::new(),
        };
        let orders = scan("orders", &["o_orderkey", "o_custkey"]);
        let plan = inner(
            semi,
            orders,
            eq(col_ref(0, "l_orderkey"), col_ref(2, "o_orderkey")),
        );

        let after = run_pass(plan, true);
        match after {
            LogicalPlan::SemiJoin { left, left_key, .. } => {
                assert!(matches!(left.as_ref(), LogicalPlan::Join { .. }));
                assert!(matches!(left_key, PlanExpr::Column { index: 0, .. }));
            }
            other => panic!("expected top SemiJoin, got {other}"),
        }
    }

    #[test]
    fn many_to_many_inner_join_bails() {
        let l1 = scan("l1", &["l_orderkey"]);
        let sub = scan("sub", &["l_orderkey"]);
        let semi = LogicalPlan::SemiJoin {
            left: Box::new(l1),
            right: Box::new(sub),
            left_key: col_ref(0, "l_orderkey"),
            right_key: col_ref(0, "l_orderkey"),
            residual: None,
            dynamic_filter_ids: Vec::new(),
        };
        let many = scan("many", &["m_orderkey"]);
        let plan = inner(
            semi,
            many,
            eq(col_ref(0, "l_orderkey"), col_ref(1, "m_orderkey")),
        );
        let after = run_pass(plan, true);
        assert!(matches!(after, LogicalPlan::Join { .. }));
    }

    #[test]
    fn residual_with_out_of_range_column_bails() {
        let l1 = scan("l1", &["l_orderkey"]);
        let sub = scan("sub", &["l_orderkey"]);
        let semi = LogicalPlan::SemiJoin {
            left: Box::new(l1),
            right: Box::new(sub),
            left_key: col_ref(0, "l_orderkey"),
            right_key: col_ref(0, "l_orderkey"),
            residual: Some(eq(col_ref(2, "not_in_left_or_sub"), lit_bool(true))),
            dynamic_filter_ids: Vec::new(),
        };
        let orders = scan("orders", &["o_orderkey"]);
        let plan = inner(
            semi,
            orders,
            eq(col_ref(0, "l_orderkey"), col_ref(1, "o_orderkey")),
        );
        let after = run_pass(plan, true);
        assert!(matches!(after, LogicalPlan::Join { .. }));
    }

    #[test]
    fn gate_off_leaves_plan_unchanged() {
        let after = run_pass(anti_chain(), false);
        assert!(matches!(after, LogicalPlan::Join { .. }));
    }
}
