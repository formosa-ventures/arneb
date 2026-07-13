//! Gated decorrelation for Q21-style correlated EXISTS / NOT EXISTS.
//!
//! This pass rewrites only the narrow shape produced for TPC-H Q21:
//! a `SemiJoin` / `AntiJoin` keyed by orderkey, with a residual
//! `inner.suppkey <> outer.suppkey`, whose right side is the same base
//! table scan (optionally with one inner-local filter for `AntiJoin`).
//! It replaces the full right-side semi/anti build with grouped supplier
//! min/max state per order.

use std::sync::OnceLock;

use arneb_common::error::PlanError;
use arneb_common::types::{ColumnInfo, TableReference};
use arneb_sql_parser::ast::{self, BinaryOp};

use crate::analyzer::{AnalysisPass, AnalyzerContext};
use crate::plan::{JoinCondition, LogicalPlan, PlanExpr};

#[derive(Debug, Default)]
pub struct DecorrelateExists;

impl DecorrelateExists {
    pub fn new() -> Self {
        Self
    }
}

impl AnalysisPass for DecorrelateExists {
    fn name(&self) -> &'static str {
        "DecorrelateExists"
    }

    fn analyze(
        &self,
        plan: LogicalPlan,
        _ctx: &mut AnalyzerContext,
    ) -> Result<LogicalPlan, PlanError> {
        if !decorr_exists_enabled() {
            return Ok(plan);
        }
        Ok(rewrite(plan))
    }
}

fn decorr_exists_enabled() -> bool {
    #[cfg(test)]
    if let Some(enabled) = decorr_exists_test_override() {
        return enabled;
    }

    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        let enabled = std::env::var("ARNEB_DECORR_EXISTS")
            .map(|v| v == "1")
            .unwrap_or(false);
        tracing::info!(
            target: "arneb::config",
            ARNEB_DECORR_EXISTS = enabled,
            "ARNEB_DECORR_EXISTS effective value (default off; =1 to enable grouped EXISTS decorrelation)"
        );
        enabled
    })
}

#[cfg(test)]
static DECORR_EXISTS_TEST_OVERRIDE: OnceLock<std::sync::Mutex<Option<bool>>> = OnceLock::new();
#[cfg(test)]
static DECORR_EXISTS_TEST_LOCK: OnceLock<std::sync::Mutex<()>> = OnceLock::new();

#[cfg(test)]
struct DecorrExistsOverride {
    _guard: std::sync::MutexGuard<'static, ()>,
}

#[cfg(test)]
impl Drop for DecorrExistsOverride {
    fn drop(&mut self) {
        *DECORR_EXISTS_TEST_OVERRIDE
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("decorrelate exists test override lock poisoned") = None;
    }
}

#[cfg(test)]
fn decorr_exists_test_override() -> Option<bool> {
    *DECORR_EXISTS_TEST_OVERRIDE
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("decorrelate exists test override lock poisoned")
}

#[cfg(test)]
fn set_decorr_exists_for_test(enabled: bool) -> DecorrExistsOverride {
    let guard = DECORR_EXISTS_TEST_LOCK
        .get_or_init(|| std::sync::Mutex::new(()))
        .lock()
        .expect("decorrelate exists test lock poisoned");
    *DECORR_EXISTS_TEST_OVERRIDE
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("decorrelate exists test override lock poisoned") = Some(enabled);
    DecorrExistsOverride { _guard: guard }
}

fn rewrite(plan: LogicalPlan) -> LogicalPlan {
    let plan = recurse_children(plan, &mut rewrite);
    match plan {
        LogicalPlan::SemiJoin {
            left,
            right,
            left_key,
            right_key,
            residual: Some(residual),
            ..
        } => match try_rewrite(&left, &right, &left_key, &right_key, &residual, false) {
            Some(rewritten) => rewritten,
            None => LogicalPlan::SemiJoin {
                left,
                right,
                left_key,
                right_key,
                residual: Some(residual),
                dynamic_filter_ids: Vec::new(),
            },
        },
        LogicalPlan::AntiJoin {
            left,
            right,
            left_key,
            right_key,
            residual: Some(residual),
        } => match try_rewrite(&left, &right, &left_key, &right_key, &residual, true) {
            Some(rewritten) => rewritten,
            None => LogicalPlan::AntiJoin {
                left,
                right,
                left_key,
                right_key,
                residual: Some(residual),
            },
        },
        other => other,
    }
}

fn try_rewrite(
    left: &LogicalPlan,
    right: &LogicalPlan,
    left_key: &PlanExpr,
    right_key: &PlanExpr,
    residual: &PlanExpr,
    anti: bool,
) -> Option<LogicalPlan> {
    let left_key_idx = column_index(left_key)?;
    let right_key_idx = column_index(right_key)?;

    let left_schema = left.schema();
    let left_width = left_schema.len();
    let (outer_supp_idx, inner_supp_idx) = match_residual_not_eq(residual, left_width)?;

    let right_scan = right_scan_shape(right, anti)?;
    if right_scan.table_schema.len() <= right_key_idx
        || right_scan.table_schema.len() <= inner_supp_idx
    {
        return None;
    }
    if left_schema.len() <= left_key_idx || left_schema.len() <= outer_supp_idx {
        return None;
    }
    if !contains_table(left, &right_scan.table) {
        return None;
    }
    if left_schema[left_key_idx].name != right_scan.table_schema[right_key_idx].name
        || left_schema[outer_supp_idx].name != right_scan.table_schema[inner_supp_idx].name
        || left_schema[left_key_idx].data_type != right_scan.table_schema[right_key_idx].data_type
        || left_schema[outer_supp_idx].data_type
            != right_scan.table_schema[inner_supp_idx].data_type
    {
        return None;
    }

    let agg = build_supplier_aggregate(right.clone(), right_key_idx, inner_supp_idx, anti);
    let agg_key = PlanExpr::Column {
        index: 0,
        name: agg.schema()[0].name.clone(),
        span: None,
    };
    let join = LogicalPlan::Join {
        left: Box::new(left.clone()),
        right: Box::new(agg),
        join_type: if anti {
            ast::JoinType::Left
        } else {
            ast::JoinType::Inner
        },
        condition: JoinCondition::On(eq(left_key.clone(), shift_columns(agg_key, left_width))),
        dynamic_filter_ids: Vec::new(),
    };

    let filter = LogicalPlan::Filter {
        input: Box::new(join),
        predicate: if anti {
            anti_residual(left_width, outer_supp_idx)
        } else {
            semi_residual(left_width, outer_supp_idx)
        },
    };

    let exprs = left_schema
        .iter()
        .enumerate()
        .map(|(index, c)| PlanExpr::Column {
            index,
            name: c.name.clone(),
            span: None,
        })
        .collect();

    Some(LogicalPlan::Projection {
        input: Box::new(filter),
        exprs,
        schema: left_schema,
    })
}

struct RightScanShape {
    table: TableReference,
    table_schema: Vec<ColumnInfo>,
}

fn right_scan_shape(plan: &LogicalPlan, anti: bool) -> Option<RightScanShape> {
    match plan {
        LogicalPlan::TableScan { table, schema, .. } if !anti => Some(RightScanShape {
            table: table.clone(),
            table_schema: schema.clone(),
        }),
        LogicalPlan::Filter { input, .. } if anti => match input.as_ref() {
            LogicalPlan::TableScan { table, schema, .. } => Some(RightScanShape {
                table: table.clone(),
                table_schema: schema.clone(),
            }),
            _ => None,
        },
        _ => None,
    }
}

fn build_supplier_aggregate(
    right: LogicalPlan,
    key_idx: usize,
    supp_idx: usize,
    anti: bool,
) -> LogicalPlan {
    let right_schema = right.schema();
    let key_col = right_schema[key_idx].clone();
    let supp_col = right_schema[supp_idx].clone();
    let supp_not_null = PlanExpr::IsNotNull {
        expr: Box::new(col(supp_idx, &supp_col.name)),
        span: None,
    };
    let filtered = match right {
        LogicalPlan::Filter { input, predicate } if anti => LogicalPlan::Filter {
            input,
            predicate: and(predicate, supp_not_null),
        },
        other => LogicalPlan::Filter {
            input: Box::new(other),
            predicate: supp_not_null,
        },
    };

    let min_name = if anti { "__min_late" } else { "__min_supp" };
    let max_name = if anti { "__max_late" } else { "__max_supp" };
    LogicalPlan::Aggregate {
        input: Box::new(filtered),
        group_by: vec![col(key_idx, &key_col.name)],
        aggr_exprs: vec![
            PlanExpr::Function {
                name: "MIN".to_string(),
                args: vec![col(supp_idx, &supp_col.name)],
                distinct: false,
                span: None,
            },
            PlanExpr::Function {
                name: "MAX".to_string(),
                args: vec![col(supp_idx, &supp_col.name)],
                distinct: false,
                span: None,
            },
        ],
        schema: vec![
            key_col,
            ColumnInfo {
                name: min_name.to_string(),
                data_type: supp_col.data_type.clone(),
                nullable: true,
            },
            ColumnInfo {
                name: max_name.to_string(),
                data_type: supp_col.data_type,
                nullable: true,
            },
        ],
    }
}

fn semi_residual(left_width: usize, outer_supp_idx: usize) -> PlanExpr {
    let min = col(left_width + 1, "__min_supp");
    let max = col(left_width + 2, "__max_supp");
    let outer = col(outer_supp_idx, "l_suppkey");
    // EXISTS: {non-null suppkeys on order} has an element != s iff
    // (>=2 distinct: min<max) OR (1 distinct == that one != s: min==max AND min!=s).
    and(
        PlanExpr::IsNotNull {
            expr: Box::new(outer.clone()),
            span: None,
        },
        or(
            lt(min.clone(), max.clone()),
            and(eq(min.clone(), max), ne(min, outer)),
        ),
    )
}

fn anti_residual(left_width: usize, outer_supp_idx: usize) -> PlanExpr {
    let min = col(left_width + 1, "__min_late");
    let max = col(left_width + 2, "__max_late");
    let outer = col(outer_supp_idx, "l_suppkey");
    // NOT-EXISTS: {late suppkeys} subset {s} iff (empty) OR (singleton == s).
    or(
        PlanExpr::IsNull {
            expr: Box::new(outer.clone()),
            span: None,
        },
        or(
            PlanExpr::IsNull {
                expr: Box::new(min.clone()),
                span: None,
            },
            and(eq(min.clone(), max), eq(min, outer)),
        ),
    )
}

fn match_residual_not_eq(expr: &PlanExpr, left_width: usize) -> Option<(usize, usize)> {
    let PlanExpr::BinaryOp {
        left,
        op: BinaryOp::NotEq,
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

fn contains_table(plan: &LogicalPlan, target: &TableReference) -> bool {
    match plan {
        LogicalPlan::TableScan { table, .. } => table == target,
        LogicalPlan::Projection { input, .. }
        | LogicalPlan::Filter { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::PartialAggregate { input, .. }
        | LogicalPlan::FinalAggregate { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Distinct { input }
        | LogicalPlan::Window { input, .. }
        | LogicalPlan::AssignUniqueId { input, .. }
        | LogicalPlan::Explain { input, .. } => contains_table(input, target),
        LogicalPlan::Join { left, right, .. }
        | LogicalPlan::Intersect { left, right }
        | LogicalPlan::Except { left, right }
        | LogicalPlan::SemiJoin { left, right, .. }
        | LogicalPlan::AntiJoin { left, right, .. } => {
            contains_table(left, target) || contains_table(right, target)
        }
        LogicalPlan::ScalarSubquery { subplan } => contains_table(subplan, target),
        LogicalPlan::UnionAll { inputs } => inputs.iter().any(|p| contains_table(p, target)),
        LogicalPlan::CreateTableAsSelect { source, .. }
        | LogicalPlan::InsertInto { source, .. } => contains_table(source, target),
        LogicalPlan::CreateView { plan, .. } => contains_table(plan, target),
        LogicalPlan::ExchangeNode { .. }
        | LogicalPlan::CreateTable { .. }
        | LogicalPlan::DropTable { .. }
        | LogicalPlan::DeleteFrom { .. }
        | LogicalPlan::DropView { .. }
        | LogicalPlan::OneRow => false,
    }
}

fn column_index(expr: &PlanExpr) -> Option<usize> {
    match expr {
        PlanExpr::Column { index, .. } => Some(*index),
        _ => None,
    }
}

fn col(index: usize, name: &str) -> PlanExpr {
    PlanExpr::Column {
        index,
        name: name.to_string(),
        span: None,
    }
}

fn eq(left: PlanExpr, right: PlanExpr) -> PlanExpr {
    bin(left, BinaryOp::Eq, right)
}

fn ne(left: PlanExpr, right: PlanExpr) -> PlanExpr {
    bin(left, BinaryOp::NotEq, right)
}

fn lt(left: PlanExpr, right: PlanExpr) -> PlanExpr {
    bin(left, BinaryOp::Lt, right)
}

fn and(left: PlanExpr, right: PlanExpr) -> PlanExpr {
    bin(left, BinaryOp::And, right)
}

fn or(left: PlanExpr, right: PlanExpr) -> PlanExpr {
    bin(left, BinaryOp::Or, right)
}

fn bin(left: PlanExpr, op: BinaryOp, right: PlanExpr) -> PlanExpr {
    PlanExpr::BinaryOp {
        left: Box::new(left),
        op,
        right: Box::new(right),
        span: None,
    }
}

fn shift_columns(expr: PlanExpr, delta: usize) -> PlanExpr {
    walk_columns(expr, &mut |idx, name, span| PlanExpr::Column {
        index: idx + delta,
        name,
        span,
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

#[cfg(test)]
mod tests {
    use super::*;
    use arneb_common::types::DataType;
    use std::collections::HashMap;

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    struct Line {
        orderkey: i64,
        suppkey: Option<i64>,
        late: bool,
    }

    fn ci(name: &str, nullable: bool) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable,
        }
    }

    fn scan_lineitem() -> LogicalPlan {
        LogicalPlan::TableScan {
            table: TableReference::table("lineitem"),
            schema: vec![
                ci("l_orderkey", false),
                ci("l_suppkey", true),
                ci("l_receiptdate", false),
                ci("l_commitdate", false),
            ],
            alias: None,
            properties: HashMap::new(),
            dynamic_filters_consumed: Vec::new(),
        }
    }

    fn q21_semijoin_shape() -> LogicalPlan {
        LogicalPlan::SemiJoin {
            left: Box::new(scan_lineitem()),
            right: Box::new(scan_lineitem()),
            left_key: col(0, "l_orderkey"),
            right_key: col(0, "l_orderkey"),
            residual: Some(ne(col(5, "l_suppkey"), col(1, "l_suppkey"))),
            dynamic_filter_ids: Vec::new(),
        }
    }

    fn q21_antijoin_shape() -> LogicalPlan {
        let late_filter = LogicalPlan::Filter {
            input: Box::new(scan_lineitem()),
            predicate: bin(
                col(2, "l_receiptdate"),
                BinaryOp::Gt,
                col(3, "l_commitdate"),
            ),
        };
        LogicalPlan::AntiJoin {
            left: Box::new(scan_lineitem()),
            right: Box::new(late_filter),
            left_key: col(0, "l_orderkey"),
            right_key: col(0, "l_orderkey"),
            residual: Some(ne(col(5, "l_suppkey"), col(1, "l_suppkey"))),
        }
    }

    #[test]
    fn gate_defaults_to_noop_in_pass() {
        let mut ctx = AnalyzerContext::new();
        let _override = set_decorr_exists_for_test(false);
        let before = q21_semijoin_shape();
        let after = DecorrelateExists::new()
            .analyze(before.clone(), &mut ctx)
            .unwrap();
        assert!(matches!(after, LogicalPlan::SemiJoin { .. }));
        assert_eq!(before.schema(), after.schema());
    }

    #[test]
    fn rewrites_q21_semi_shape_to_inner_join_aggregate_filter_projection() {
        let _override = set_decorr_exists_for_test(true);
        let mut ctx = AnalyzerContext::new();
        let after = DecorrelateExists::new()
            .analyze(q21_semijoin_shape(), &mut ctx)
            .unwrap();
        let LogicalPlan::Projection { input, schema, .. } = after else {
            panic!("expected projection");
        };
        assert_eq!(schema.len(), 4);
        let LogicalPlan::Filter { input, predicate } = input.as_ref() else {
            panic!("expected filter");
        };
        let pred = format!("{predicate}");
        assert!(pred.contains("__min_supp"));
        assert!(pred.contains("__max_supp"));
        let LogicalPlan::Join {
            join_type,
            right,
            condition,
            ..
        } = input.as_ref()
        else {
            panic!("expected join");
        };
        assert_eq!(*join_type, ast::JoinType::Inner);
        assert!(matches!(condition, JoinCondition::On(_)));
        let LogicalPlan::Aggregate {
            input,
            aggr_exprs,
            schema,
            ..
        } = right.as_ref()
        else {
            panic!("expected aggregate");
        };
        assert_eq!(aggr_exprs.len(), 2);
        assert_eq!(schema[1].name, "__min_supp");
        assert_eq!(schema[2].name, "__max_supp");
        assert!(!matches!(input.as_ref(), LogicalPlan::Aggregate { .. }));
    }

    #[test]
    fn rewrites_q21_anti_shape_to_left_join_aggregate_filter_projection() {
        let _override = set_decorr_exists_for_test(true);
        let mut ctx = AnalyzerContext::new();
        let after = DecorrelateExists::new()
            .analyze(q21_antijoin_shape(), &mut ctx)
            .unwrap();
        let LogicalPlan::Projection { input, .. } = after else {
            panic!("expected projection");
        };
        let LogicalPlan::Filter { input, predicate } = input.as_ref() else {
            panic!("expected filter");
        };
        let pred = format!("{predicate}");
        assert!(pred.contains("IS NULL"));
        assert!(pred.contains("__min_late"));
        assert!(pred.contains("__max_late"));
        let LogicalPlan::Join {
            join_type, right, ..
        } = input.as_ref()
        else {
            panic!("expected join");
        };
        assert_eq!(*join_type, ast::JoinType::Left);
        let LogicalPlan::Aggregate { input, schema, .. } = right.as_ref() else {
            panic!("expected aggregate");
        };
        assert_eq!(schema[1].name, "__min_late");
        assert_eq!(schema[2].name, "__max_late");
        assert!(!matches!(input.as_ref(), LogicalPlan::Aggregate { .. }));
        let LogicalPlan::Filter { predicate, .. } = input.as_ref() else {
            panic!("expected null/local filter");
        };
        let pred = format!("{predicate}");
        assert!(pred.contains("AND"));
        assert!(pred.contains("IS NOT NULL"));
    }

    #[test]
    fn q21_fixture_matches_original_semi_and_anti_semantics() {
        let rows = vec![
            Line {
                orderkey: 1,
                suppkey: Some(10),
                late: true,
            },
            Line {
                orderkey: 2,
                suppkey: Some(20),
                late: true,
            },
            Line {
                orderkey: 2,
                suppkey: Some(30),
                late: false,
            },
            Line {
                orderkey: 3,
                suppkey: Some(40),
                late: true,
            },
            Line {
                orderkey: 3,
                suppkey: Some(50),
                late: true,
            },
            Line {
                orderkey: 4,
                suppkey: Some(60),
                late: false,
            },
            Line {
                orderkey: 4,
                suppkey: Some(70),
                late: false,
            },
            Line {
                orderkey: 5,
                suppkey: None,
                late: true,
            },
            Line {
                orderkey: 5,
                suppkey: Some(80),
                late: false,
            },
            Line {
                orderkey: 6,
                suppkey: Some(90),
                late: true,
            },
            Line {
                orderkey: 6,
                suppkey: None,
                late: true,
            },
        ];
        for row in &rows {
            assert_eq!(
                original_exists(&rows, row),
                rewritten_exists(&rows, row),
                "EXISTS mismatch for {row:?}"
            );
            assert_eq!(
                original_not_exists_late(&rows, row),
                rewritten_not_exists_late(&rows, row),
                "NOT EXISTS mismatch for {row:?}"
            );
        }
    }

    fn original_exists(rows: &[Line], outer: &Line) -> bool {
        rows.iter().any(|inner| {
            inner.orderkey == outer.orderkey && sql_not_eq(inner.suppkey, outer.suppkey)
        })
    }

    fn rewritten_exists(rows: &[Line], outer: &Line) -> bool {
        let (min, max) = supp_min_max(rows.iter().filter(|r| r.orderkey == outer.orderkey));
        outer.suppkey.is_some()
            && (sql_lt(min, max) || (min == max && sql_not_eq(min, outer.suppkey)))
    }

    fn original_not_exists_late(rows: &[Line], outer: &Line) -> bool {
        !rows.iter().any(|inner| {
            inner.orderkey == outer.orderkey
                && inner.late
                && sql_not_eq(inner.suppkey, outer.suppkey)
        })
    }

    fn rewritten_not_exists_late(rows: &[Line], outer: &Line) -> bool {
        let (min, max) = supp_min_max(
            rows.iter()
                .filter(|r| r.orderkey == outer.orderkey && r.late),
        );
        outer.suppkey.is_none() || min.is_none() || (min == max && min == outer.suppkey)
    }

    fn supp_min_max<'a>(rows: impl Iterator<Item = &'a Line>) -> (Option<i64>, Option<i64>) {
        let mut min: Option<i64> = None;
        let mut max: Option<i64> = None;
        for row in rows {
            if let Some(suppkey) = row.suppkey {
                min = Some(min.map_or(suppkey, |current| current.min(suppkey)));
                max = Some(max.map_or(suppkey, |current| current.max(suppkey)));
            }
        }
        (min, max)
    }

    fn sql_lt(left: Option<i64>, right: Option<i64>) -> bool {
        matches!((left, right), (Some(l), Some(r)) if l < r)
    }

    fn sql_not_eq(left: Option<i64>, right: Option<i64>) -> bool {
        matches!((left, right), (Some(l), Some(r)) if l != r)
    }
}
