//! Env-gated Decimal128-to-Decimal64 narrowing for carried intermediate columns.

use std::collections::HashSet;
use std::sync::OnceLock;

use arneb_common::error::PlanError;
use arneb_common::types::{ColumnInfo, DataType, TableReference};
use arneb_sql_parser::ast;

use crate::analyzer::{AnalysisPass, AnalyzerContext};
use crate::plan::{JoinCondition, LogicalPlan, PlanExpr};

#[cfg(test)]
static NARROW_DECIMALS_TEST_OVERRIDE: OnceLock<std::sync::Mutex<Option<bool>>> = OnceLock::new();

/// Returns true when `ARNEB_NARROW_DECIMALS` enables decimal carry narrowing.
pub fn narrow_decimals_enabled() -> bool {
    #[cfg(test)]
    if let Some(override_value) = NARROW_DECIMALS_TEST_OVERRIDE
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("NARROW_DECIMALS_TEST_OVERRIDE mutex poisoned")
        .as_ref()
    {
        return *override_value;
    }

    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        let enabled = std::env::var("ARNEB_NARROW_DECIMALS").is_ok_and(|v| v == "1");
        if enabled {
            tracing::info!(
                target: "arneb::config",
                "ARNEB_NARROW_DECIMALS=on: narrowing carried Decimal128 columns to Decimal64"
            );
        }
        enabled
    })
}

fn narrow_decimals_allowlist() -> &'static HashSet<String> {
    static ALLOW: OnceLock<HashSet<String>> = OnceLock::new();
    ALLOW.get_or_init(|| {
        std::env::var("ARNEB_NARROW_DECIMALS_COLUMNS")
            .ok()
            .map(|v| {
                v.split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect()
            })
            .unwrap_or_default()
    })
}

#[derive(Debug, Default)]
pub struct NarrowDecimals;

impl NarrowDecimals {
    pub fn new() -> Self {
        Self
    }
}

impl AnalysisPass for NarrowDecimals {
    fn name(&self) -> &'static str {
        "NarrowDecimals"
    }

    fn analyze(
        &self,
        plan: LogicalPlan,
        _ctx: &mut AnalyzerContext,
    ) -> Result<LogicalPlan, PlanError> {
        Ok(narrow_plan(plan))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ColumnSource {
    table: TableReference,
    column: String,
    data_type: DataType,
}

#[derive(Debug, Clone)]
enum SourceTrace {
    Known(ColumnSource),
    Ambiguous(Vec<ColumnSource>),
    Unknown,
}

impl SourceTrace {
    fn blockable_sources(self) -> Vec<ColumnSource> {
        match self {
            SourceTrace::Known(source) => vec![source],
            SourceTrace::Ambiguous(sources) => sources,
            SourceTrace::Unknown => Vec::new(),
        }
    }
}

fn narrow_plan(plan: LogicalPlan) -> LogicalPlan {
    let mut carried = HashSet::new();
    let mut blocked = root_output_sources(&plan);
    collect_decimal_carry_and_blocks(&plan, false, &mut carried, &mut blocked);
    let candidates = carried
        .into_iter()
        .filter(|source| source_can_narrow(source) && !blocked.contains(source))
        .collect::<HashSet<_>>();
    rewrite_plan(plan, &candidates)
}

fn root_output_sources(plan: &LogicalPlan) -> HashSet<ColumnSource> {
    (0..plan.schema().len())
        .flat_map(|index| trace_output_column(plan, index).blockable_sources())
        .collect()
}

fn collect_decimal_carry_and_blocks(
    plan: &LogicalPlan,
    carried_by_parent: bool,
    carried: &mut HashSet<ColumnSource>,
    blocked: &mut HashSet<ColumnSource>,
) {
    match plan {
        LogicalPlan::Join {
            left,
            right,
            condition,
            ..
        } => {
            collect_decimal_carry_and_blocks(left, true, carried, blocked);
            collect_decimal_carry_and_blocks(right, true, carried, blocked);
            for (left_idx, right_idx) in equi_join_pairs(condition, left.schema().len()) {
                blocked.extend(trace_output_column(left, left_idx).blockable_sources());
                blocked.extend(trace_output_column(right, right_idx).blockable_sources());
            }
            if let JoinCondition::On(expr) = condition {
                block_expr_sources_joined(expr, left, right, blocked);
            }
        }
        LogicalPlan::Projection { input, exprs, .. } => {
            for expr in exprs {
                collect_expr_subquery_blocks(expr, blocked);
            }
            collect_decimal_carry_and_blocks(input, carried_by_parent, carried, blocked);
        }
        LogicalPlan::PartialAggregate {
            input,
            group_by,
            aggr_exprs,
            ..
        }
        | LogicalPlan::Aggregate {
            input,
            group_by,
            aggr_exprs,
            ..
        }
        | LogicalPlan::FinalAggregate {
            input,
            group_by,
            aggr_exprs,
            ..
        } => {
            for expr in group_by {
                block_expr_sources(expr, input, blocked);
            }
            for expr in aggr_exprs {
                block_direct_aggregate_decimal_sources(expr, input, blocked);
            }
            collect_decimal_carry_and_blocks(input, carried_by_parent, carried, blocked);
        }
        LogicalPlan::Filter { input, predicate } => {
            block_expr_sources(predicate, input, blocked);
            collect_decimal_carry_and_blocks(input, carried_by_parent, carried, blocked);
        }
        LogicalPlan::Sort { input, order_by } => {
            for sort in order_by {
                block_expr_sources(&sort.expr, input, blocked);
            }
            collect_decimal_carry_and_blocks(input, carried_by_parent, carried, blocked);
        }
        LogicalPlan::SemiJoin {
            left,
            right,
            left_key,
            right_key,
            residual,
            ..
        }
        | LogicalPlan::AntiJoin {
            left,
            right,
            left_key,
            right_key,
            residual,
            ..
        } => {
            block_expr_sources(left_key, left, blocked);
            block_expr_sources(right_key, right, blocked);
            if let Some(residual) = residual {
                block_expr_sources_joined(residual, left, right, blocked);
            }
            collect_decimal_carry_and_blocks(left, true, carried, blocked);
            collect_decimal_carry_and_blocks(right, true, carried, blocked);
        }
        LogicalPlan::TableScan { table, schema, .. } => {
            if carried_by_parent {
                for col in schema {
                    carried.insert(ColumnSource {
                        table: table.clone(),
                        column: col.name.clone(),
                        data_type: col.data_type.clone(),
                    });
                }
            }
        }
        LogicalPlan::Limit { input, .. }
        | LogicalPlan::Explain { input, .. }
        | LogicalPlan::Distinct { input }
        | LogicalPlan::Window { input, .. }
        | LogicalPlan::AssignUniqueId { input, .. } => {
            collect_decimal_carry_and_blocks(input, carried_by_parent, carried, blocked);
        }
        LogicalPlan::UnionAll { inputs } => {
            for input in inputs {
                collect_decimal_carry_and_blocks(input, carried_by_parent, carried, blocked);
            }
        }
        LogicalPlan::Intersect { left, right } | LogicalPlan::Except { left, right } => {
            collect_decimal_carry_and_blocks(left, carried_by_parent, carried, blocked);
            collect_decimal_carry_and_blocks(right, carried_by_parent, carried, blocked);
        }
        LogicalPlan::ScalarSubquery { subplan } => {
            block_plan_sources(subplan, blocked);
            collect_decimal_carry_and_blocks(subplan, carried_by_parent, carried, blocked);
        }
        LogicalPlan::CreateTableAsSelect { source, .. }
        | LogicalPlan::InsertInto { source, .. }
        | LogicalPlan::CreateView { plan: source, .. } => {
            collect_decimal_carry_and_blocks(source, carried_by_parent, carried, blocked);
        }
        LogicalPlan::ExchangeNode { .. }
        | LogicalPlan::CreateTable { .. }
        | LogicalPlan::DropTable { .. }
        | LogicalPlan::DeleteFrom { .. }
        | LogicalPlan::DropView { .. }
        | LogicalPlan::OneRow => {}
    }
}

fn source_can_narrow(source: &ColumnSource) -> bool {
    match source.data_type {
        DataType::Decimal128 { precision, .. } if precision <= 18 => {
            narrow_decimals_allowlist().is_empty()
                || narrow_decimals_allowlist().contains(&source.column)
        }
        _ => false,
    }
}

fn rewrite_plan(plan: LogicalPlan, candidates: &HashSet<ColumnSource>) -> LogicalPlan {
    match plan {
        LogicalPlan::TableScan { .. } => narrow_scan(plan, candidates),
        LogicalPlan::Projection {
            input,
            exprs,
            schema,
        } => {
            let input = rewrite_plan(*input, candidates);
            let input = castback_input_if_needed(input, exprs.iter());
            LogicalPlan::Projection {
                input: Box::new(input),
                exprs,
                schema,
            }
        }
        LogicalPlan::PartialAggregate {
            input,
            group_by,
            aggr_exprs,
            schema,
        } => {
            let input = rewrite_plan(*input, candidates);
            let input = castback_input_if_needed(input, group_by.iter().chain(aggr_exprs.iter()));
            LogicalPlan::PartialAggregate {
                input: Box::new(input),
                group_by,
                aggr_exprs,
                schema,
            }
        }
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggr_exprs,
            schema,
        } => {
            let input = rewrite_plan(*input, candidates);
            let input = castback_input_if_needed(input, group_by.iter().chain(aggr_exprs.iter()));
            LogicalPlan::Aggregate {
                input: Box::new(input),
                group_by,
                aggr_exprs,
                schema,
            }
        }
        LogicalPlan::FinalAggregate {
            input,
            group_by,
            aggr_exprs,
            schema,
        } => LogicalPlan::FinalAggregate {
            input: Box::new(rewrite_plan(*input, candidates)),
            group_by,
            aggr_exprs,
            schema,
        },
        LogicalPlan::Filter { input, predicate } => LogicalPlan::Filter {
            input: Box::new(rewrite_plan(*input, candidates)),
            predicate,
        },
        LogicalPlan::Join {
            left,
            right,
            join_type,
            condition,
            dynamic_filter_ids,
        } => LogicalPlan::Join {
            left: Box::new(rewrite_plan(*left, candidates)),
            right: Box::new(rewrite_plan(*right, candidates)),
            join_type,
            condition,
            dynamic_filter_ids,
        },
        LogicalPlan::Sort { input, order_by } => LogicalPlan::Sort {
            input: Box::new(rewrite_plan(*input, candidates)),
            order_by,
        },
        LogicalPlan::Limit {
            input,
            limit,
            offset,
        } => LogicalPlan::Limit {
            input: Box::new(rewrite_plan(*input, candidates)),
            limit,
            offset,
        },
        LogicalPlan::Explain { input, analyze } => LogicalPlan::Explain {
            input: Box::new(rewrite_plan(*input, candidates)),
            analyze,
        },
        LogicalPlan::SemiJoin {
            left,
            right,
            left_key,
            right_key,
            residual,
            dynamic_filter_ids,
        } => LogicalPlan::SemiJoin {
            left: Box::new(rewrite_plan(*left, candidates)),
            right: Box::new(rewrite_plan(*right, candidates)),
            left_key,
            right_key,
            residual,
            dynamic_filter_ids,
        },
        LogicalPlan::AntiJoin {
            left,
            right,
            left_key,
            right_key,
            residual,
        } => LogicalPlan::AntiJoin {
            left: Box::new(rewrite_plan(*left, candidates)),
            right: Box::new(rewrite_plan(*right, candidates)),
            left_key,
            right_key,
            residual,
        },
        LogicalPlan::UnionAll { inputs } => LogicalPlan::UnionAll {
            inputs: inputs
                .into_iter()
                .map(|input| rewrite_plan(input, candidates))
                .collect(),
        },
        LogicalPlan::Distinct { input } => LogicalPlan::Distinct {
            input: Box::new(rewrite_plan(*input, candidates)),
        },
        LogicalPlan::Intersect { left, right } => LogicalPlan::Intersect {
            left: Box::new(rewrite_plan(*left, candidates)),
            right: Box::new(rewrite_plan(*right, candidates)),
        },
        LogicalPlan::Except { left, right } => LogicalPlan::Except {
            left: Box::new(rewrite_plan(*left, candidates)),
            right: Box::new(rewrite_plan(*right, candidates)),
        },
        LogicalPlan::Window { input, functions } => LogicalPlan::Window {
            input: Box::new(rewrite_plan(*input, candidates)),
            functions,
        },
        LogicalPlan::AssignUniqueId { input, id_column } => LogicalPlan::AssignUniqueId {
            input: Box::new(rewrite_plan(*input, candidates)),
            id_column,
        },
        LogicalPlan::ScalarSubquery { subplan } => LogicalPlan::ScalarSubquery {
            subplan: Box::new(rewrite_plan(*subplan, candidates)),
        },
        other => other,
    }
}

fn narrow_scan(plan: LogicalPlan, candidates: &HashSet<ColumnSource>) -> LogicalPlan {
    let LogicalPlan::TableScan { table, schema, .. } = &plan else {
        return plan;
    };

    let mut changed = false;
    let mut exprs = Vec::with_capacity(schema.len());
    let mut output_schema = Vec::with_capacity(schema.len());
    for (index, col) in schema.iter().enumerate() {
        let base_col = PlanExpr::Column {
            index,
            name: col.name.clone(),
            span: None,
        };
        let source = ColumnSource {
            table: table.clone(),
            column: col.name.clone(),
            data_type: col.data_type.clone(),
        };
        if candidates.contains(&source) {
            let DataType::Decimal128 { precision, scale } = col.data_type else {
                exprs.push(base_col);
                output_schema.push(col.clone());
                continue;
            };
            changed = true;
            exprs.push(PlanExpr::Cast {
                expr: Box::new(base_col),
                data_type: DataType::Decimal64 { precision, scale },
                span: None,
            });
            output_schema.push(ColumnInfo {
                name: col.name.clone(),
                data_type: DataType::Decimal64 { precision, scale },
                nullable: col.nullable,
            });
        } else {
            exprs.push(base_col);
            output_schema.push(col.clone());
        }
    }

    if changed {
        LogicalPlan::Projection {
            input: Box::new(plan),
            exprs,
            schema: output_schema,
        }
    } else {
        plan
    }
}

fn castback_input_if_needed<'a>(
    input: LogicalPlan,
    exprs: impl Iterator<Item = &'a PlanExpr>,
) -> LogicalPlan {
    let schema = input.schema();
    let needed = decimal64_columns_used_by_exprs(exprs, &schema);
    if needed.is_empty() {
        return input;
    }
    castback_projection(input, &needed)
}

fn castback_projection(input: LogicalPlan, needed: &HashSet<usize>) -> LogicalPlan {
    let input_schema = input.schema();
    let mut exprs = Vec::with_capacity(input_schema.len());
    let mut schema = Vec::with_capacity(input_schema.len());
    for (index, col) in input_schema.iter().enumerate() {
        let col_expr = PlanExpr::Column {
            index,
            name: col.name.clone(),
            span: None,
        };
        match col.data_type {
            DataType::Decimal64 { precision, scale } if needed.contains(&index) => {
                exprs.push(PlanExpr::Cast {
                    expr: Box::new(col_expr),
                    data_type: DataType::Decimal128 { precision, scale },
                    span: None,
                });
                schema.push(ColumnInfo {
                    name: col.name.clone(),
                    data_type: DataType::Decimal128 { precision, scale },
                    nullable: col.nullable,
                });
            }
            _ => {
                exprs.push(col_expr);
                schema.push(col.clone());
            }
        }
    }
    LogicalPlan::Projection {
        input: Box::new(input),
        exprs,
        schema,
    }
}

fn decimal64_columns_used_by_exprs<'a>(
    exprs: impl Iterator<Item = &'a PlanExpr>,
    schema: &[ColumnInfo],
) -> HashSet<usize> {
    let mut used = HashSet::new();
    for expr in exprs {
        collect_decimal64_columns_needing_wide(expr, schema, &mut used);
    }
    used
}

fn collect_decimal64_columns_needing_wide(
    expr: &PlanExpr,
    schema: &[ColumnInfo],
    used: &mut HashSet<usize>,
) {
    match expr {
        PlanExpr::Column { .. } | PlanExpr::Literal { .. } | PlanExpr::Parameter { .. } => {}
        PlanExpr::BinaryOp { left, right, .. } => {
            collect_decimal64_column_refs(left, schema, used);
            collect_decimal64_column_refs(right, schema, used);
        }
        PlanExpr::Function { args, .. } => {
            for arg in args {
                collect_decimal64_column_refs(arg, schema, used);
            }
        }
        PlanExpr::UnaryOp { expr, .. }
        | PlanExpr::IsNull { expr, .. }
        | PlanExpr::IsNotNull { expr, .. }
        | PlanExpr::Cast { expr, .. } => {
            collect_decimal64_columns_needing_wide(expr, schema, used);
        }
        PlanExpr::Between {
            expr, low, high, ..
        } => {
            collect_decimal64_column_refs(expr, schema, used);
            collect_decimal64_column_refs(low, schema, used);
            collect_decimal64_column_refs(high, schema, used);
        }
        PlanExpr::InList { expr, list, .. } => {
            collect_decimal64_column_refs(expr, schema, used);
            for item in list {
                collect_decimal64_column_refs(item, schema, used);
            }
        }
        PlanExpr::CaseExpr {
            operand,
            when_clauses,
            else_result,
            ..
        } => {
            if let Some(operand) = operand {
                collect_decimal64_column_refs(operand, schema, used);
            }
            for (condition, result) in when_clauses {
                collect_decimal64_column_refs(condition, schema, used);
                collect_decimal64_column_refs(result, schema, used);
            }
            if let Some(else_result) = else_result {
                collect_decimal64_column_refs(else_result, schema, used);
            }
        }
        PlanExpr::ScalarSubquery { .. } | PlanExpr::Wildcard => {}
    }
}

fn collect_decimal64_column_refs(
    expr: &PlanExpr,
    schema: &[ColumnInfo],
    used: &mut HashSet<usize>,
) {
    match expr {
        PlanExpr::Column { index, .. } => {
            if schema
                .get(*index)
                .is_some_and(|c| matches!(c.data_type, DataType::Decimal64 { .. }))
            {
                used.insert(*index);
            }
        }
        PlanExpr::BinaryOp { left, right, .. } => {
            collect_decimal64_column_refs(left, schema, used);
            collect_decimal64_column_refs(right, schema, used);
        }
        PlanExpr::UnaryOp { expr, .. }
        | PlanExpr::IsNull { expr, .. }
        | PlanExpr::IsNotNull { expr, .. }
        | PlanExpr::Cast { expr, .. } => collect_decimal64_column_refs(expr, schema, used),
        PlanExpr::Function { args, .. } => {
            for arg in args {
                collect_decimal64_column_refs(arg, schema, used);
            }
        }
        PlanExpr::Between {
            expr, low, high, ..
        } => {
            collect_decimal64_column_refs(expr, schema, used);
            collect_decimal64_column_refs(low, schema, used);
            collect_decimal64_column_refs(high, schema, used);
        }
        PlanExpr::InList { expr, list, .. } => {
            collect_decimal64_column_refs(expr, schema, used);
            for item in list {
                collect_decimal64_column_refs(item, schema, used);
            }
        }
        PlanExpr::CaseExpr {
            operand,
            when_clauses,
            else_result,
            ..
        } => {
            if let Some(operand) = operand {
                collect_decimal64_column_refs(operand, schema, used);
            }
            for (condition, result) in when_clauses {
                collect_decimal64_column_refs(condition, schema, used);
                collect_decimal64_column_refs(result, schema, used);
            }
            if let Some(else_result) = else_result {
                collect_decimal64_column_refs(else_result, schema, used);
            }
        }
        PlanExpr::ScalarSubquery { .. }
        | PlanExpr::Literal { .. }
        | PlanExpr::Wildcard
        | PlanExpr::Parameter { .. } => {}
    }
}

fn block_direct_aggregate_decimal_sources(
    expr: &PlanExpr,
    input: &LogicalPlan,
    blocked: &mut HashSet<ColumnSource>,
) {
    if let PlanExpr::Function { name, args, .. } = expr {
        if matches!(name.to_uppercase().as_str(), "SUM" | "AVG") {
            for arg in args {
                if let PlanExpr::Column { index, .. } = arg {
                    blocked.extend(trace_output_column(input, *index).blockable_sources());
                }
            }
        }
    }
}

fn block_plan_sources(plan: &LogicalPlan, blocked: &mut HashSet<ColumnSource>) {
    for index in 0..plan.schema().len() {
        blocked.extend(trace_output_column(plan, index).blockable_sources());
    }
}

fn block_expr_sources(expr: &PlanExpr, input: &LogicalPlan, blocked: &mut HashSet<ColumnSource>) {
    match expr {
        PlanExpr::Column { index, .. } => {
            blocked.extend(trace_output_column(input, *index).blockable_sources());
        }
        PlanExpr::BinaryOp { left, right, .. } => {
            block_expr_sources(left, input, blocked);
            block_expr_sources(right, input, blocked);
        }
        PlanExpr::UnaryOp { expr, .. }
        | PlanExpr::IsNull { expr, .. }
        | PlanExpr::IsNotNull { expr, .. }
        | PlanExpr::Cast { expr, .. } => block_expr_sources(expr, input, blocked),
        PlanExpr::Function { args, .. } => {
            for arg in args {
                block_expr_sources(arg, input, blocked);
            }
        }
        PlanExpr::Between {
            expr, low, high, ..
        } => {
            block_expr_sources(expr, input, blocked);
            block_expr_sources(low, input, blocked);
            block_expr_sources(high, input, blocked);
        }
        PlanExpr::InList { expr, list, .. } => {
            block_expr_sources(expr, input, blocked);
            for item in list {
                block_expr_sources(item, input, blocked);
            }
        }
        PlanExpr::CaseExpr {
            operand,
            when_clauses,
            else_result,
            ..
        } => {
            if let Some(operand) = operand {
                block_expr_sources(operand, input, blocked);
            }
            for (condition, result) in when_clauses {
                block_expr_sources(condition, input, blocked);
                block_expr_sources(result, input, blocked);
            }
            if let Some(else_result) = else_result {
                block_expr_sources(else_result, input, blocked);
            }
        }
        PlanExpr::ScalarSubquery { subplan, .. } => block_plan_sources(subplan, blocked),
        PlanExpr::Literal { .. } | PlanExpr::Wildcard | PlanExpr::Parameter { .. } => {}
    }
}

fn collect_expr_subquery_blocks(expr: &PlanExpr, blocked: &mut HashSet<ColumnSource>) {
    match expr {
        PlanExpr::ScalarSubquery { subplan, .. } => block_plan_sources(subplan, blocked),
        PlanExpr::BinaryOp { left, right, .. } => {
            collect_expr_subquery_blocks(left, blocked);
            collect_expr_subquery_blocks(right, blocked);
        }
        PlanExpr::UnaryOp { expr, .. }
        | PlanExpr::IsNull { expr, .. }
        | PlanExpr::IsNotNull { expr, .. }
        | PlanExpr::Cast { expr, .. } => collect_expr_subquery_blocks(expr, blocked),
        PlanExpr::Function { args, .. } => {
            for arg in args {
                collect_expr_subquery_blocks(arg, blocked);
            }
        }
        PlanExpr::Between {
            expr, low, high, ..
        } => {
            collect_expr_subquery_blocks(expr, blocked);
            collect_expr_subquery_blocks(low, blocked);
            collect_expr_subquery_blocks(high, blocked);
        }
        PlanExpr::InList { expr, list, .. } => {
            collect_expr_subquery_blocks(expr, blocked);
            for item in list {
                collect_expr_subquery_blocks(item, blocked);
            }
        }
        PlanExpr::CaseExpr {
            operand,
            when_clauses,
            else_result,
            ..
        } => {
            if let Some(operand) = operand {
                collect_expr_subquery_blocks(operand, blocked);
            }
            for (condition, result) in when_clauses {
                collect_expr_subquery_blocks(condition, blocked);
                collect_expr_subquery_blocks(result, blocked);
            }
            if let Some(else_result) = else_result {
                collect_expr_subquery_blocks(else_result, blocked);
            }
        }
        PlanExpr::Column { .. }
        | PlanExpr::Literal { .. }
        | PlanExpr::Wildcard
        | PlanExpr::Parameter { .. } => {}
    }
}

fn block_expr_sources_joined(
    expr: &PlanExpr,
    left: &LogicalPlan,
    right: &LogicalPlan,
    blocked: &mut HashSet<ColumnSource>,
) {
    match expr {
        PlanExpr::Column { index, .. } => {
            let left_width = left.schema().len();
            if *index < left_width {
                blocked.extend(trace_output_column(left, *index).blockable_sources());
            } else {
                blocked.extend(trace_output_column(right, *index - left_width).blockable_sources());
            }
        }
        PlanExpr::BinaryOp {
            left: l, right: r, ..
        } => {
            block_expr_sources_joined(l, left, right, blocked);
            block_expr_sources_joined(r, left, right, blocked);
        }
        PlanExpr::UnaryOp { expr, .. }
        | PlanExpr::IsNull { expr, .. }
        | PlanExpr::IsNotNull { expr, .. }
        | PlanExpr::Cast { expr, .. } => block_expr_sources_joined(expr, left, right, blocked),
        PlanExpr::Function { args, .. } => {
            for arg in args {
                block_expr_sources_joined(arg, left, right, blocked);
            }
        }
        PlanExpr::Between {
            expr, low, high, ..
        } => {
            block_expr_sources_joined(expr, left, right, blocked);
            block_expr_sources_joined(low, left, right, blocked);
            block_expr_sources_joined(high, left, right, blocked);
        }
        PlanExpr::InList { expr, list, .. } => {
            block_expr_sources_joined(expr, left, right, blocked);
            for item in list {
                block_expr_sources_joined(item, left, right, blocked);
            }
        }
        PlanExpr::CaseExpr {
            operand,
            when_clauses,
            else_result,
            ..
        } => {
            if let Some(operand) = operand {
                block_expr_sources_joined(operand, left, right, blocked);
            }
            for (condition, result) in when_clauses {
                block_expr_sources_joined(condition, left, right, blocked);
                block_expr_sources_joined(result, left, right, blocked);
            }
            if let Some(else_result) = else_result {
                block_expr_sources_joined(else_result, left, right, blocked);
            }
        }
        PlanExpr::ScalarSubquery { subplan, .. } => block_plan_sources(subplan, blocked),
        PlanExpr::Literal { .. } | PlanExpr::Wildcard | PlanExpr::Parameter { .. } => {}
    }
}

fn trace_output_column(plan: &LogicalPlan, index: usize) -> SourceTrace {
    match plan {
        LogicalPlan::TableScan { table, schema, .. } => schema
            .get(index)
            .map(|c| {
                SourceTrace::Known(ColumnSource {
                    table: table.clone(),
                    column: c.name.clone(),
                    data_type: c.data_type.clone(),
                })
            })
            .unwrap_or(SourceTrace::Unknown),
        LogicalPlan::Projection {
            input,
            exprs,
            schema,
        } => match exprs.get(index) {
            Some(PlanExpr::Column {
                index: input_index, ..
            }) => trace_projection_column(input, schema.get(index), *input_index),
            Some(PlanExpr::Cast { expr, .. }) => {
                if let Some(input_index) = single_column_index(expr) {
                    trace_projection_column(input, schema.get(index), input_index)
                } else {
                    SourceTrace::Unknown
                }
            }
            Some(_) | None => SourceTrace::Unknown,
        },
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Distinct { input }
        | LogicalPlan::Explain { input, .. }
        | LogicalPlan::SemiJoin { left: input, .. }
        | LogicalPlan::AntiJoin { left: input, .. } => trace_output_column(input, index),
        LogicalPlan::Join { left, right, .. } => {
            let left_width = left.schema().len();
            if index < left_width {
                trace_output_column(left, index)
            } else {
                trace_output_column(right, index - left_width)
            }
        }
        LogicalPlan::Aggregate {
            input, group_by, ..
        }
        | LogicalPlan::PartialAggregate {
            input, group_by, ..
        }
        | LogicalPlan::FinalAggregate {
            input, group_by, ..
        } => group_by
            .get(index)
            .and_then(single_column_index)
            .map(|input_index| trace_output_column(input, input_index))
            .unwrap_or(SourceTrace::Unknown),
        _ => SourceTrace::Unknown,
    }
}

fn trace_projection_column(
    input: &LogicalPlan,
    output_col: Option<&ColumnInfo>,
    input_index: usize,
) -> SourceTrace {
    let trace = trace_output_column(input, input_index);
    let Some(output_col) = output_col else {
        return trace;
    };
    match &trace {
        SourceTrace::Known(source) if output_col.name == source.column => trace,
        SourceTrace::Known(source) => SourceTrace::Ambiguous(vec![source.clone()]),
        SourceTrace::Ambiguous(_) | SourceTrace::Unknown => trace,
    }
}

fn single_column_index(expr: &PlanExpr) -> Option<usize> {
    match expr {
        PlanExpr::Column { index, .. } => Some(*index),
        PlanExpr::Cast { expr, .. } => single_column_index(expr),
        _ => None,
    }
}

fn equi_join_pairs(condition: &JoinCondition, left_width: usize) -> Vec<(usize, usize)> {
    let JoinCondition::On(expr) = condition else {
        return Vec::new();
    };
    let mut pairs = Vec::new();
    collect_equi_join_pairs(expr, left_width, &mut pairs);
    pairs
}

fn collect_equi_join_pairs(expr: &PlanExpr, left_width: usize, pairs: &mut Vec<(usize, usize)>) {
    match expr {
        PlanExpr::BinaryOp {
            left,
            op: ast::BinaryOp::And,
            right,
            ..
        } => {
            collect_equi_join_pairs(left, left_width, pairs);
            collect_equi_join_pairs(right, left_width, pairs);
        }
        PlanExpr::BinaryOp {
            left,
            op: ast::BinaryOp::Eq,
            right,
            ..
        } => {
            if let (PlanExpr::Column { index: l, .. }, PlanExpr::Column { index: r, .. }) =
                (left.as_ref(), right.as_ref())
            {
                if *l < left_width && *r >= left_width {
                    pairs.push((*l, *r - left_width));
                } else if *r < left_width && *l >= left_width {
                    pairs.push((*r, *l - left_width));
                }
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analyzer::AnalyzerContext;
    use arrow::array::Decimal128Array;
    use arrow::datatypes::DataType as ArrowDataType;

    struct NarrowDecimalsGuard {
        previous: Option<bool>,
    }

    impl Drop for NarrowDecimalsGuard {
        fn drop(&mut self) {
            *NARROW_DECIMALS_TEST_OVERRIDE
                .get_or_init(|| std::sync::Mutex::new(None))
                .lock()
                .unwrap() = self.previous;
        }
    }

    fn set_narrow_decimals_for_test(value: bool) -> NarrowDecimalsGuard {
        let mut guard = NARROW_DECIMALS_TEST_OVERRIDE
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .unwrap();
        let previous = *guard;
        *guard = Some(value);
        NarrowDecimalsGuard { previous }
    }

    fn ci(name: &str, data_type: DataType) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            data_type,
            nullable: true,
        }
    }

    fn dec128() -> DataType {
        DataType::Decimal128 {
            precision: 15,
            scale: 2,
        }
    }

    fn dec64() -> DataType {
        DataType::Decimal64 {
            precision: 15,
            scale: 2,
        }
    }

    fn scan(table: &str, schema: Vec<ColumnInfo>) -> LogicalPlan {
        LogicalPlan::TableScan {
            table: TableReference::table(table),
            schema,
            alias: None,
            properties: Default::default(),
            dynamic_filters_consumed: Vec::new(),
        }
    }

    fn col(index: usize, name: &str) -> PlanExpr {
        PlanExpr::Column {
            index,
            name: name.to_string(),
            span: None,
        }
    }

    fn analyze(plan: LogicalPlan) -> LogicalPlan {
        NarrowDecimals::new()
            .analyze(plan, &mut AnalyzerContext::new())
            .unwrap()
    }

    fn join_lineitem_orders() -> LogicalPlan {
        let left = scan(
            "lineitem",
            vec![
                ci("l_orderkey", DataType::Int64),
                ci("l_extendedprice", dec128()),
                ci("l_discount", dec128()),
            ],
        );
        let right = scan("orders", vec![ci("o_orderkey", DataType::Int64)]);
        LogicalPlan::Join {
            left: Box::new(left),
            right: Box::new(right),
            join_type: ast::JoinType::Inner,
            condition: JoinCondition::On(PlanExpr::BinaryOp {
                left: Box::new(col(0, "l_orderkey")),
                op: ast::BinaryOp::Eq,
                right: Box::new(col(3, "o_orderkey")),
                span: None,
            }),
            dynamic_filter_ids: Vec::new(),
        }
    }

    #[test]
    fn narrow_and_castback() {
        let join = join_lineitem_orders();
        let product = PlanExpr::BinaryOp {
            left: Box::new(col(1, "l_extendedprice")),
            op: ast::BinaryOp::Multiply,
            right: Box::new(col(2, "l_discount")),
            span: None,
        };
        let project = LogicalPlan::Projection {
            input: Box::new(join),
            exprs: vec![product],
            schema: vec![ci("product", dec128())],
        };
        let plan = LogicalPlan::PartialAggregate {
            input: Box::new(project),
            group_by: Vec::new(),
            aggr_exprs: vec![PlanExpr::Function {
                name: "SUM".to_string(),
                args: vec![col(0, "product")],
                distinct: false,
                span: None,
            }],
            schema: vec![ci("sum", dec128())],
        };

        let out = analyze(plan);
        let LogicalPlan::PartialAggregate { input, .. } = out else {
            panic!("expected PartialAggregate");
        };
        let LogicalPlan::Projection { input, .. } = input.as_ref() else {
            panic!("expected arithmetic Projection");
        };
        let LogicalPlan::Projection {
            input,
            exprs,
            schema,
        } = input.as_ref()
        else {
            panic!("expected cast-back Projection before arithmetic");
        };
        assert!(matches!(
            exprs[1],
            PlanExpr::Cast {
                data_type: DataType::Decimal128 {
                    precision: 15,
                    scale: 2
                },
                ..
            }
        ));
        assert_eq!(schema[1].data_type, dec128());
        let LogicalPlan::Join { left, .. } = input.as_ref() else {
            panic!("expected Join below cast-back");
        };
        let LogicalPlan::Projection { schema, .. } = left.as_ref() else {
            panic!("expected scan-side narrowing Projection");
        };
        assert_eq!(schema[1].data_type, dec64());
        assert_eq!(schema[2].data_type, dec64());
    }

    #[test]
    fn value_roundtrip() {
        let arr = Decimal128Array::from(vec![0, 9_999_999_999_999i128])
            .with_precision_and_scale(15, 2)
            .unwrap();
        let as64 = arrow::compute::cast(&arr, &ArrowDataType::Decimal64(15, 2)).unwrap();
        let back = arrow::compute::cast(&as64, &ArrowDataType::Decimal128(15, 2)).unwrap();
        let back = back.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(back.value(0), 0);
        assert_eq!(back.value(1), 9_999_999_999_999i128);

        let blocked = ColumnSource {
            table: TableReference::table("t"),
            column: "too_wide".to_string(),
            data_type: DataType::Decimal128 {
                precision: 19,
                scale: 2,
            },
        };
        assert!(!source_can_narrow(&blocked));
    }

    #[test]
    fn block_join_key() {
        let left = scan("l", vec![ci("d", dec128())]);
        let right = scan("r", vec![ci("d", dec128())]);
        let plan = LogicalPlan::Join {
            left: Box::new(left),
            right: Box::new(right),
            join_type: ast::JoinType::Inner,
            condition: JoinCondition::On(PlanExpr::BinaryOp {
                left: Box::new(col(0, "l.d")),
                op: ast::BinaryOp::Eq,
                right: Box::new(col(1, "r.d")),
                span: None,
            }),
            dynamic_filter_ids: Vec::new(),
        };
        assert!(matches!(analyze(plan), LogicalPlan::Join { .. }));
    }

    #[test]
    fn block_root_output() {
        let join = join_lineitem_orders();
        let plan = LogicalPlan::Projection {
            input: Box::new(join),
            exprs: vec![col(1, "l_extendedprice")],
            schema: vec![ci("l_extendedprice", dec128())],
        };
        let LogicalPlan::Projection { input, .. } = analyze(plan) else {
            panic!("expected root projection");
        };
        let LogicalPlan::Join { left, .. } = input.as_ref() else {
            panic!("expected unchanged join input");
        };
        let LogicalPlan::Projection { schema, .. } = left.as_ref() else {
            panic!("expected scan-side projection for unrelated carried decimals");
        };
        assert_eq!(schema[1].data_type, dec128());
    }

    #[test]
    fn block_aggregate() {
        let join = join_lineitem_orders();
        let plan = LogicalPlan::PartialAggregate {
            input: Box::new(join),
            group_by: Vec::new(),
            aggr_exprs: vec![PlanExpr::Function {
                name: "SUM".to_string(),
                args: vec![col(1, "l_extendedprice")],
                distinct: false,
                span: None,
            }],
            schema: vec![ci("sum", dec128())],
        };
        let LogicalPlan::PartialAggregate { input, .. } = analyze(plan) else {
            panic!("expected PartialAggregate");
        };
        let LogicalPlan::Join { left, .. } = input.as_ref() else {
            panic!("expected join");
        };
        let LogicalPlan::Projection { schema, .. } = left.as_ref() else {
            panic!("expected scan-side projection for unrelated carried decimals");
        };
        assert_eq!(schema[1].data_type, dec128());
    }

    #[test]
    fn gate_off() {
        let _guard = set_narrow_decimals_for_test(false);
        assert!(!narrow_decimals_enabled());
        let before = join_lineitem_orders();
        let before_str = before.to_string();
        let after = if narrow_decimals_enabled() {
            NarrowDecimals::new()
                .analyze(before, &mut AnalyzerContext::new())
                .unwrap()
        } else {
            before
        };
        assert_eq!(after.to_string(), before_str);
    }
}
