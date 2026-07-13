//! Plan formatting with cardinality estimates for `EXPLAIN` output.
//!
//! Renders a `LogicalPlan` as a tree of operator lines, each annotated
//! with `Estimates: rows=N` derived from the cost model. Mirrors
//! Trino's `EXPLAIN` style so users can compare estimates side-by-side
//! when debugging join-reorder choices.
//!
//! `EXPLAIN ANALYZE` adds an `(actual: M)` suffix per node when actual
//! row counts are available — Phase 2.7 produces the estimate-only
//! variant; runtime instrumentation for the actual counts is a follow-up.

use crate::cost::{estimated_cardinality, CatalogStats};
use crate::plan::LogicalPlan;

/// Indentation step (in spaces) for each level of the plan tree.
const INDENT: usize = 2;

/// Renders `plan` as a multi-line string with cardinality estimates
/// derived from `stats`. Each line has the shape:
///
/// ```text
/// <indent><OperatorName>...
/// <indent>  Estimates: rows=<N>
/// ```
///
/// The output is stable and human-readable. When `stats` is empty, the
/// estimates use the cost model's defaults (`DEFAULT_TABLE_SIZE`).
pub fn format_plan_with_estimates(plan: &LogicalPlan, stats: &CatalogStats) -> String {
    let mut out = String::new();
    format_node(plan, stats, 0, &mut out);
    out
}

fn format_node(plan: &LogicalPlan, stats: &CatalogStats, depth: usize, out: &mut String) {
    let pad = " ".repeat(depth * INDENT);
    let est = estimated_cardinality(plan, stats);
    out.push_str(&pad);
    out.push_str(&summarize_node(plan));
    out.push('\n');
    out.push_str(&pad);
    out.push_str("  Estimates: rows=");
    out.push_str(&fmt_card(est));
    out.push('\n');
    for child in children(plan) {
        format_node(child, stats, depth + 1, out);
    }
}

/// One-line summary for an operator. Includes the operator name and a
/// small set of high-signal attributes (the joined columns for joins,
/// the predicate text for filters, etc.). Keeps each line short so
/// large plans remain readable.
fn summarize_node(plan: &LogicalPlan) -> String {
    use LogicalPlan as L;
    match plan {
        L::TableScan { table, .. } => format!("TableScan({})", table),
        L::Projection { exprs, .. } => format!("Projection (n={})", exprs.len()),
        L::Filter { predicate, .. } => format!("Filter [{}]", predicate),
        L::Sort { order_by, .. } => format!("Sort (n={})", order_by.len()),
        L::Limit { limit, offset, .. } => match (limit, offset) {
            (Some(n), Some(o)) => format!("Limit {n} offset {o}"),
            (Some(n), None) => format!("Limit {n}"),
            (None, Some(o)) => format!("Offset {o}"),
            (None, None) => "Limit (none)".to_string(),
        },
        L::Explain { analyze, .. } => {
            if *analyze {
                "Explain ANALYZE".to_string()
            } else {
                "Explain".to_string()
            }
        }
        L::Distinct { .. } => "Distinct".to_string(),
        L::Aggregate {
            group_by,
            aggr_exprs,
            ..
        } => format!(
            "Aggregate (group_by={}, aggs={})",
            group_by.len(),
            aggr_exprs.len()
        ),
        L::PartialAggregate {
            group_by,
            aggr_exprs,
            ..
        } => format!(
            "PartialAggregate (group_by={}, aggs={})",
            group_by.len(),
            aggr_exprs.len()
        ),
        L::FinalAggregate {
            group_by,
            aggr_exprs,
            ..
        } => format!(
            "FinalAggregate (group_by={}, aggs={})",
            group_by.len(),
            aggr_exprs.len()
        ),
        L::Join {
            join_type,
            condition,
            ..
        } => format!("{:?}Join [{:?}]", join_type, condition),
        L::SemiJoin { .. } => "SemiJoin".to_string(),
        L::AntiJoin { .. } => "AntiJoin".to_string(),
        L::ScalarSubquery { .. } => "ScalarSubquery".to_string(),
        L::UnionAll { inputs } => format!("UnionAll (n={})", inputs.len()),
        L::Intersect { .. } => "Intersect".to_string(),
        L::Except { .. } => "Except".to_string(),
        L::Window { functions, .. } => format!("Window (n={})", functions.len()),
        L::ExchangeNode { stage_id, .. } => format!("Exchange (stage={stage_id})"),
        L::CreateTable { name, .. } => format!("CreateTable({})", name),
        L::DropTable { name, .. } => format!("DropTable({})", name),
        L::CreateTableAsSelect { name, .. } => format!("CreateTableAsSelect({})", name),
        L::InsertInto { table, .. } => format!("InsertInto({})", table),
        L::DeleteFrom { table, .. } => format!("DeleteFrom({})", table),
        L::CreateView { name, .. } => format!("CreateView({})", name),
        L::DropView { name, .. } => format!("DropView({})", name),
        L::AssignUniqueId { id_column, .. } => format!("AssignUniqueId({})", id_column),
        L::OneRow => "OneRow".to_string(),
    }
}

fn children(plan: &LogicalPlan) -> Vec<&LogicalPlan> {
    use LogicalPlan as L;
    match plan {
        L::TableScan { .. }
        | L::ExchangeNode { .. }
        | L::CreateTable { .. }
        | L::DropTable { .. }
        | L::DeleteFrom { .. }
        | L::DropView { .. }
        | L::OneRow => vec![],
        L::Projection { input, .. }
        | L::Filter { input, .. }
        | L::Sort { input, .. }
        | L::Limit { input, .. }
        | L::Explain { input, .. }
        | L::Distinct { input }
        | L::Aggregate { input, .. }
        | L::PartialAggregate { input, .. }
        | L::FinalAggregate { input, .. }
        | L::Window { input, .. }
        | L::AssignUniqueId { input, .. } => vec![input.as_ref()],
        L::Join { left, right, .. }
        | L::SemiJoin { left, right, .. }
        | L::AntiJoin { left, right, .. }
        | L::Intersect { left, right }
        | L::Except { left, right } => vec![left.as_ref(), right.as_ref()],
        L::UnionAll { inputs } => inputs.iter().collect(),
        L::ScalarSubquery { subplan } => vec![subplan.as_ref()],
        L::CreateTableAsSelect { source, .. } | L::InsertInto { source, .. } => {
            vec![source.as_ref()]
        }
        L::CreateView { plan, .. } => vec![plan.as_ref()],
    }
}

fn fmt_card(value: f64) -> String {
    if value.is_finite() {
        format!("{:.0}", value)
    } else {
        "?".to_string()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::{JoinCondition, LogicalPlan, PlanExpr};
    use arneb_catalog::TableStatistics;
    use arneb_common::types::{ColumnInfo, DataType, TableReference};
    use arneb_sql_parser::ast::{BinaryOp, JoinType};
    use std::collections::HashMap;

    fn col(name: &str) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn scan(table: &str) -> LogicalPlan {
        LogicalPlan::TableScan {
            table: TableReference::table(table),
            schema: vec![col("k")],
            alias: None,
            properties: HashMap::new(),
            dynamic_filters_consumed: Vec::new(),
        }
    }

    fn col_expr(name: &str) -> PlanExpr {
        PlanExpr::Column {
            index: 0,
            name: name.to_string(),
            span: None,
        }
    }

    fn make_stats(row_count: u64) -> TableStatistics {
        TableStatistics {
            row_count: Some(row_count),
            ..TableStatistics::default()
        }
    }

    #[test]
    fn formats_single_scan_with_row_count_estimate() {
        let mut stats = CatalogStats::new();
        stats.insert(TableReference::table("lineitem"), make_stats(6_000_000));
        let plan = scan("lineitem");
        let out = format_plan_with_estimates(&plan, &stats);
        assert!(out.contains("TableScan(lineitem)"));
        assert!(out.contains("Estimates: rows=6000000"));
    }

    #[test]
    fn formats_single_scan_with_default_when_no_stats() {
        let plan = scan("missing");
        let out = format_plan_with_estimates(&plan, &CatalogStats::new());
        assert!(out.contains("TableScan(missing)"));
        // DEFAULT_TABLE_SIZE = 10_000
        assert!(out.contains("Estimates: rows=10000"));
    }

    #[test]
    fn formats_join_tree_with_estimates_at_each_node() {
        let mut stats = CatalogStats::new();
        stats.insert(TableReference::table("a"), make_stats(100));
        stats.insert(TableReference::table("b"), make_stats(50));
        let plan = LogicalPlan::Join {
            left: Box::new(scan("a")),
            right: Box::new(scan("b")),
            join_type: JoinType::Inner,
            condition: JoinCondition::On(PlanExpr::BinaryOp {
                left: Box::new(col_expr("k")),
                op: BinaryOp::Eq,
                right: Box::new(col_expr("k")),
                span: None,
            }),
            dynamic_filter_ids: Vec::new(),
        };
        let out = format_plan_with_estimates(&plan, &stats);
        // Three "Estimates:" lines (Join + two TableScans).
        assert_eq!(out.matches("Estimates: rows=").count(), 3);
        assert!(out.contains("TableScan(a)"));
        assert!(out.contains("TableScan(b)"));
        // Join is at depth 0, scans at depth 1 (indented).
        let mut lines = out.lines();
        let first = lines.next().unwrap();
        assert!(!first.starts_with(' '));
        // Confirm Join line type.
        assert!(first.starts_with("InnerJoin") || first.contains("Join"));
    }

    #[test]
    fn formats_aggregate_with_estimate() {
        let mut stats = CatalogStats::new();
        stats.insert(TableReference::table("orders"), make_stats(1_500_000));
        let plan = LogicalPlan::Aggregate {
            input: Box::new(scan("orders")),
            group_by: vec![],
            aggr_exprs: vec![],
            schema: vec![col("count")],
        };
        let out = format_plan_with_estimates(&plan, &stats);
        assert!(out.contains("Aggregate"));
        // Global aggregate = 1 row.
        assert!(out.contains("Estimates: rows=1"));
    }
}
