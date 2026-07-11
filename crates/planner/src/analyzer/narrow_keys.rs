//! Stats-gated Int64-to-Int32 narrowing for intermediate columns.

use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;

use arneb_catalog::ColumnStatistics;
use arneb_common::error::PlanError;
use arneb_common::types::{ColumnInfo, DataType, ScalarValue, TableReference};
use arneb_sql_parser::ast;

use crate::analyzer::{plan_expr_type, AnalysisPass, AnalyzerContext};
use crate::cost::CatalogStats;
use crate::plan::{JoinCondition, LogicalPlan, PlanExpr};

#[cfg(test)]
static NARROW_KEYS_TEST_OVERRIDE: OnceLock<std::sync::Mutex<Option<bool>>> = OnceLock::new();

/// Returns true when `ARNEB_NARROW_KEYS` enables the planner narrowing pass.
pub fn narrow_keys_enabled() -> bool {
    #[cfg(test)]
    if let Some(override_value) = NARROW_KEYS_TEST_OVERRIDE
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("NARROW_KEYS_TEST_OVERRIDE mutex poisoned")
        .as_ref()
    {
        return *override_value;
    }

    static ENABLED: OnceLock<bool> = OnceLock::new();

    *ENABLED.get_or_init(|| {
        let enabled = std::env::var("ARNEB_NARROW_KEYS").is_ok_and(|v| v == "1");
        if enabled {
            tracing::info!(
                target: "arneb::config",
                "ARNEB_NARROW_KEYS=on: narrowing int64 columns to int32 where stats prove overflow-safe and column not in output"
            );
        }
        enabled
    })
}

/// Measure-first stopgap: an explicit allowlist of column names (comma-separated,
/// `ARNEB_NARROW_KEYS_COLUMNS`) the operator declares fit i32. Bypasses the
/// catalog-stats overflow guard so the lever's memory benefit can be measured on
/// connectors (e.g. the hive bench) that don't yet supply column min/max. The
/// operator is responsible for correctness of the declaration; the SF30 cell-diff
/// gates it. Empty by default → no effect (the stats-based guard stands alone).
fn narrow_keys_allowlist() -> &'static std::collections::HashSet<String> {
    static ALLOW: OnceLock<std::collections::HashSet<String>> = OnceLock::new();
    ALLOW.get_or_init(|| {
        std::env::var("ARNEB_NARROW_KEYS_COLUMNS")
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
pub struct NarrowKeys;

impl NarrowKeys {
    pub fn new() -> Self {
        Self
    }
}

impl AnalysisPass for NarrowKeys {
    fn name(&self) -> &'static str {
        "NarrowKeys"
    }

    fn analyze(
        &self,
        plan: LogicalPlan,
        ctx: &mut AnalyzerContext,
    ) -> Result<LogicalPlan, PlanError> {
        if ctx.catalog_stats.is_empty() && narrow_keys_allowlist().is_empty() {
            tracing::info!(
                target: "arneb::config",
                "ARNEB_NARROW_KEYS=on: no column stats available, skipping narrowing"
            );
            // TODO: keep this no-op behavior until every production connector
            // can provide reliable column min/max stats at planning time.
            // (ARNEB_NARROW_KEYS_COLUMNS allowlist bypasses this for measurement.)
            return Ok(plan);
        }
        Ok(narrow_plan(plan, ctx.catalog_stats.as_ref()))
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
    fn known(&self) -> Option<&ColumnSource> {
        match self {
            SourceTrace::Known(source) => Some(source),
            SourceTrace::Ambiguous(_) | SourceTrace::Unknown => None,
        }
    }

    fn blockable_sources(self) -> Vec<ColumnSource> {
        match self {
            SourceTrace::Known(source) => vec![source],
            SourceTrace::Ambiguous(sources) => sources,
            SourceTrace::Unknown => Vec::new(),
        }
    }
}

fn narrow_plan(plan: LogicalPlan, stats: &CatalogStats) -> LogicalPlan {
    let root_outputs = root_output_columns(&plan);
    let mut blocked = root_output_sources(&plan);
    collect_join_key_blocks(&plan, stats, &root_outputs, &mut blocked);
    expand_join_key_blocks_to_fixpoint(&plan, &mut blocked);
    rewrite_plan(plan, stats, &root_outputs, &blocked)
}

fn root_output_columns(plan: &LogicalPlan) -> HashSet<String> {
    plan.schema().into_iter().map(|c| c.name).collect()
}

fn root_output_sources(plan: &LogicalPlan) -> HashSet<ColumnSource> {
    (0..plan.schema().len())
        .flat_map(|index| trace_output_column(plan, index).blockable_sources())
        .collect()
}

fn collect_join_key_blocks(
    plan: &LogicalPlan,
    stats: &CatalogStats,
    root_outputs: &HashSet<String>,
    blocked: &mut HashSet<ColumnSource>,
) {
    match plan {
        LogicalPlan::Join {
            left,
            right,
            condition,
            ..
        } => {
            collect_join_key_blocks(left, stats, root_outputs, blocked);
            collect_join_key_blocks(right, stats, root_outputs, blocked);
            collect_join_condition_expr_blocks(condition, stats, root_outputs, blocked);
            let left_width = left.schema().len();
            if let Some(pairs) = pure_equi_join_pairs(condition, left_width) {
                for (left_idx, right_idx) in pairs {
                    let left_source = trace_output_column(left, left_idx);
                    let right_source = trace_output_column(right, right_idx);
                    let left_safe = left_source
                        .known()
                        .is_some_and(|s| source_can_narrow(s, stats, root_outputs));
                    let right_safe = right_source
                        .known()
                        .is_some_and(|s| source_can_narrow(s, stats, root_outputs));
                    if !(left_safe && right_safe) {
                        blocked.extend(left_source.blockable_sources());
                        blocked.extend(right_source.blockable_sources());
                    }
                }
            } else if let JoinCondition::On(expr) = condition {
                blocked.extend(expr_sources_joined(expr, left, right));
            }
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
            collect_join_key_blocks(left, stats, root_outputs, blocked);
            collect_join_key_blocks(right, stats, root_outputs, blocked);
            block_plan_output_sources(left, blocked);
            block_plan_sources(right, blocked);
            blocked.extend(expr_sources(left_key, left));
            blocked.extend(expr_sources(right_key, right));
            block_joined_key_sources(left, right, left_key, right_key, blocked);
            collect_expr_join_key_blocks(left_key, stats, root_outputs, blocked);
            collect_expr_join_key_blocks(right_key, stats, root_outputs, blocked);
            if let Some(residual) = residual {
                blocked.extend(expr_sources_joined(residual, left, right));
                collect_expr_join_key_blocks(residual, stats, root_outputs, blocked);
            }
        }
        LogicalPlan::Projection { input, exprs, .. } => {
            collect_join_key_blocks(input, stats, root_outputs, blocked);
            for expr in exprs {
                collect_expr_join_key_blocks(expr, stats, root_outputs, blocked);
            }
        }
        LogicalPlan::Filter { input, predicate } => {
            collect_join_key_blocks(input, stats, root_outputs, blocked);
            collect_expr_join_key_blocks(predicate, stats, root_outputs, blocked);
        }
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggr_exprs,
            ..
        }
        | LogicalPlan::PartialAggregate {
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
            collect_join_key_blocks(input, stats, root_outputs, blocked);
            for expr in group_by.iter().chain(aggr_exprs) {
                collect_expr_join_key_blocks(expr, stats, root_outputs, blocked);
            }
        }
        LogicalPlan::Sort { input, order_by } => {
            collect_join_key_blocks(input, stats, root_outputs, blocked);
            for sort in order_by {
                collect_expr_join_key_blocks(&sort.expr, stats, root_outputs, blocked);
            }
        }
        LogicalPlan::Limit { input, .. }
        | LogicalPlan::Explain { input, .. }
        | LogicalPlan::Distinct { input }
        | LogicalPlan::Window { input, .. }
        | LogicalPlan::AssignUniqueId { input, .. } => {
            collect_join_key_blocks(input, stats, root_outputs, blocked);
        }
        LogicalPlan::UnionAll { inputs } => {
            for input in inputs {
                collect_join_key_blocks(input, stats, root_outputs, blocked);
            }
        }
        LogicalPlan::Intersect { left, right } | LogicalPlan::Except { left, right } => {
            collect_join_key_blocks(left, stats, root_outputs, blocked);
            collect_join_key_blocks(right, stats, root_outputs, blocked);
        }
        LogicalPlan::ScalarSubquery { subplan } => {
            block_plan_sources(subplan, blocked);
            collect_join_key_blocks(subplan, stats, root_outputs, blocked);
        }
        _ => {}
    }
}

fn collect_join_condition_expr_blocks(
    condition: &JoinCondition,
    stats: &CatalogStats,
    root_outputs: &HashSet<String>,
    blocked: &mut HashSet<ColumnSource>,
) {
    if let JoinCondition::On(expr) = condition {
        collect_expr_join_key_blocks(expr, stats, root_outputs, blocked);
    }
}

#[derive(Debug, Default)]
struct ColumnEquivalenceClasses {
    index_by_source: HashMap<ColumnSource, usize>,
    parent: Vec<usize>,
}

impl ColumnEquivalenceClasses {
    fn union(&mut self, left: ColumnSource, right: ColumnSource) {
        let left = self.index(left);
        let right = self.index(right);
        let left_root = self.find(left);
        let right_root = self.find(right);
        if left_root != right_root {
            self.parent[right_root] = left_root;
        }
    }

    fn index(&mut self, source: ColumnSource) -> usize {
        if let Some(index) = self.index_by_source.get(&source) {
            return *index;
        }
        let index = self.parent.len();
        self.index_by_source.insert(source, index);
        self.parent.push(index);
        index
    }

    fn find(&mut self, index: usize) -> usize {
        let parent = self.parent[index];
        if parent == index {
            index
        } else {
            let root = self.find(parent);
            self.parent[index] = root;
            root
        }
    }

    fn classes(mut self) -> Vec<Vec<ColumnSource>> {
        let sources = self
            .index_by_source
            .iter()
            .map(|(source, index)| (source.clone(), *index))
            .collect::<Vec<_>>();
        let mut classes: HashMap<usize, Vec<ColumnSource>> = HashMap::new();
        for (source, index) in sources {
            let root = self.find(index);
            classes.entry(root).or_default().push(source);
        }
        classes.into_values().collect()
    }
}

fn expand_join_key_blocks_to_fixpoint(plan: &LogicalPlan, blocked: &mut HashSet<ColumnSource>) {
    let join_keys_by_table = collect_join_key_sources_by_table(plan);
    loop {
        let before = blocked.len();
        expand_inner_join_equivalence_blocks(plan, blocked);
        expand_same_table_join_key_blocks(&join_keys_by_table, blocked);
        if blocked.len() == before {
            break;
        }
    }
}

fn expand_inner_join_equivalence_blocks(plan: &LogicalPlan, blocked: &mut HashSet<ColumnSource>) {
    let mut classes = ColumnEquivalenceClasses::default();
    collect_inner_join_equivalence_classes(plan, &mut classes);

    for class in classes.classes() {
        if class.iter().any(|source| blocked.contains(source)) {
            blocked.extend(class);
        }
    }
}

fn collect_join_key_sources_by_table(
    plan: &LogicalPlan,
) -> HashMap<TableReference, HashSet<ColumnSource>> {
    let mut keys_by_table = HashMap::new();
    collect_join_key_sources(plan, &mut keys_by_table);
    keys_by_table
}

fn collect_join_key_sources(
    plan: &LogicalPlan,
    keys_by_table: &mut HashMap<TableReference, HashSet<ColumnSource>>,
) {
    match plan {
        LogicalPlan::Join {
            left,
            right,
            condition,
            ..
        } => {
            collect_join_key_sources(left, keys_by_table);
            collect_join_key_sources(right, keys_by_table);
            let left_width = left.schema().len();
            for (left_idx, right_idx) in equi_join_pairs(condition, left_width) {
                collect_trace_as_join_key(trace_output_column(left, left_idx), keys_by_table);
                collect_trace_as_join_key(trace_output_column(right, right_idx), keys_by_table);
            }
        }
        LogicalPlan::SemiJoin {
            left,
            right,
            left_key,
            right_key,
            ..
        }
        | LogicalPlan::AntiJoin {
            left,
            right,
            left_key,
            right_key,
            ..
        } => {
            collect_join_key_sources(left, keys_by_table);
            collect_join_key_sources(right, keys_by_table);
            for source in expr_sources(left_key, left) {
                collect_source_as_join_key(source, keys_by_table);
            }
            for source in expr_sources(right_key, right) {
                collect_source_as_join_key(source, keys_by_table);
            }
        }
        LogicalPlan::Projection { input, .. }
        | LogicalPlan::Filter { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::PartialAggregate { input, .. }
        | LogicalPlan::FinalAggregate { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Explain { input, .. }
        | LogicalPlan::Distinct { input }
        | LogicalPlan::Window { input, .. }
        | LogicalPlan::AssignUniqueId { input, .. } => {
            collect_join_key_sources(input, keys_by_table);
        }
        LogicalPlan::UnionAll { inputs } => {
            for input in inputs {
                collect_join_key_sources(input, keys_by_table);
            }
        }
        LogicalPlan::Intersect { left, right } | LogicalPlan::Except { left, right } => {
            collect_join_key_sources(left, keys_by_table);
            collect_join_key_sources(right, keys_by_table);
        }
        LogicalPlan::ScalarSubquery { subplan } => collect_join_key_sources(subplan, keys_by_table),
        LogicalPlan::CreateTableAsSelect { source, .. }
        | LogicalPlan::InsertInto { source, .. }
        | LogicalPlan::CreateView { plan: source, .. } => {
            collect_join_key_sources(source, keys_by_table)
        }
        LogicalPlan::TableScan { .. }
        | LogicalPlan::ExchangeNode { .. }
        | LogicalPlan::CreateTable { .. }
        | LogicalPlan::DropTable { .. }
        | LogicalPlan::DeleteFrom { .. }
        | LogicalPlan::DropView { .. }
        | LogicalPlan::OneRow => {}
    }
}

fn collect_trace_as_join_key(
    trace: SourceTrace,
    keys_by_table: &mut HashMap<TableReference, HashSet<ColumnSource>>,
) {
    for source in trace.blockable_sources() {
        collect_source_as_join_key(source, keys_by_table);
    }
}

fn collect_source_as_join_key(
    source: ColumnSource,
    keys_by_table: &mut HashMap<TableReference, HashSet<ColumnSource>>,
) {
    keys_by_table
        .entry(source.table.clone())
        .or_default()
        .insert(source);
}

fn expand_same_table_join_key_blocks(
    join_keys_by_table: &HashMap<TableReference, HashSet<ColumnSource>>,
    blocked: &mut HashSet<ColumnSource>,
) {
    for keys in join_keys_by_table.values() {
        if keys.iter().any(|source| blocked.contains(source)) {
            blocked.extend(keys.iter().cloned());
        }
    }
}

fn collect_inner_join_equivalence_classes(
    plan: &LogicalPlan,
    classes: &mut ColumnEquivalenceClasses,
) {
    match plan {
        LogicalPlan::Join {
            left,
            right,
            join_type,
            condition,
            ..
        } => {
            collect_inner_join_equivalence_classes(left, classes);
            collect_inner_join_equivalence_classes(right, classes);
            collect_inner_join_equivalence_condition_classes(condition, classes);
            if matches!(join_type, ast::JoinType::Inner) {
                let left_width = left.schema().len();
                for (left_idx, right_idx) in equi_join_pairs(condition, left_width) {
                    let Some(left_source) = trace_output_column(left, left_idx).known().cloned()
                    else {
                        continue;
                    };
                    let Some(right_source) = trace_output_column(right, right_idx).known().cloned()
                    else {
                        continue;
                    };
                    classes.union(left_source, right_source);
                }
            }
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
            collect_inner_join_equivalence_classes(left, classes);
            collect_inner_join_equivalence_classes(right, classes);
            collect_inner_join_equivalence_expr_classes(left_key, classes);
            collect_inner_join_equivalence_expr_classes(right_key, classes);
            if let Some(residual) = residual {
                collect_inner_join_equivalence_expr_classes(residual, classes);
            }
        }
        LogicalPlan::Projection { input, exprs, .. } => {
            collect_inner_join_equivalence_classes(input, classes);
            for expr in exprs {
                collect_inner_join_equivalence_expr_classes(expr, classes);
            }
        }
        LogicalPlan::Filter { input, predicate } => {
            collect_inner_join_equivalence_classes(input, classes);
            collect_inner_join_equivalence_expr_classes(predicate, classes);
        }
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggr_exprs,
            ..
        }
        | LogicalPlan::PartialAggregate {
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
            collect_inner_join_equivalence_classes(input, classes);
            for expr in group_by.iter().chain(aggr_exprs) {
                collect_inner_join_equivalence_expr_classes(expr, classes);
            }
        }
        LogicalPlan::Sort { input, order_by } => {
            collect_inner_join_equivalence_classes(input, classes);
            for sort in order_by {
                collect_inner_join_equivalence_expr_classes(&sort.expr, classes);
            }
        }
        LogicalPlan::Limit { input, .. }
        | LogicalPlan::Explain { input, .. }
        | LogicalPlan::Distinct { input }
        | LogicalPlan::Window { input, .. }
        | LogicalPlan::AssignUniqueId { input, .. } => {
            collect_inner_join_equivalence_classes(input, classes);
        }
        LogicalPlan::UnionAll { inputs } => {
            for input in inputs {
                collect_inner_join_equivalence_classes(input, classes);
            }
        }
        LogicalPlan::Intersect { left, right } | LogicalPlan::Except { left, right } => {
            collect_inner_join_equivalence_classes(left, classes);
            collect_inner_join_equivalence_classes(right, classes);
        }
        LogicalPlan::ScalarSubquery { subplan } => {
            collect_inner_join_equivalence_classes(subplan, classes);
        }
        LogicalPlan::CreateTableAsSelect { source, .. }
        | LogicalPlan::InsertInto { source, .. }
        | LogicalPlan::CreateView { plan: source, .. } => {
            collect_inner_join_equivalence_classes(source, classes);
        }
        LogicalPlan::TableScan { .. }
        | LogicalPlan::ExchangeNode { .. }
        | LogicalPlan::CreateTable { .. }
        | LogicalPlan::DropTable { .. }
        | LogicalPlan::DeleteFrom { .. }
        | LogicalPlan::DropView { .. }
        | LogicalPlan::OneRow => {}
    }
}

fn collect_inner_join_equivalence_condition_classes(
    condition: &JoinCondition,
    classes: &mut ColumnEquivalenceClasses,
) {
    if let JoinCondition::On(expr) = condition {
        collect_inner_join_equivalence_expr_classes(expr, classes);
    }
}

fn collect_inner_join_equivalence_expr_classes(
    expr: &PlanExpr,
    classes: &mut ColumnEquivalenceClasses,
) {
    match expr {
        PlanExpr::ScalarSubquery { subplan, .. } => {
            collect_inner_join_equivalence_classes(subplan, classes);
        }
        PlanExpr::BinaryOp { left, right, .. } => {
            collect_inner_join_equivalence_expr_classes(left, classes);
            collect_inner_join_equivalence_expr_classes(right, classes);
        }
        PlanExpr::UnaryOp { expr, .. }
        | PlanExpr::IsNull { expr, .. }
        | PlanExpr::IsNotNull { expr, .. }
        | PlanExpr::Cast { expr, .. } => {
            collect_inner_join_equivalence_expr_classes(expr, classes);
        }
        PlanExpr::Function { args, .. } => {
            for arg in args {
                collect_inner_join_equivalence_expr_classes(arg, classes);
            }
        }
        PlanExpr::Between {
            expr, low, high, ..
        } => {
            collect_inner_join_equivalence_expr_classes(expr, classes);
            collect_inner_join_equivalence_expr_classes(low, classes);
            collect_inner_join_equivalence_expr_classes(high, classes);
        }
        PlanExpr::InList { expr, list, .. } => {
            collect_inner_join_equivalence_expr_classes(expr, classes);
            for item in list {
                collect_inner_join_equivalence_expr_classes(item, classes);
            }
        }
        PlanExpr::CaseExpr {
            operand,
            when_clauses,
            else_result,
            ..
        } => {
            if let Some(operand) = operand {
                collect_inner_join_equivalence_expr_classes(operand, classes);
            }
            for (condition, result) in when_clauses {
                collect_inner_join_equivalence_expr_classes(condition, classes);
                collect_inner_join_equivalence_expr_classes(result, classes);
            }
            if let Some(else_result) = else_result {
                collect_inner_join_equivalence_expr_classes(else_result, classes);
            }
        }
        PlanExpr::Column { .. }
        | PlanExpr::Literal { .. }
        | PlanExpr::Wildcard
        | PlanExpr::Parameter { .. } => {}
    }
}

fn collect_expr_join_key_blocks(
    expr: &PlanExpr,
    stats: &CatalogStats,
    root_outputs: &HashSet<String>,
    blocked: &mut HashSet<ColumnSource>,
) {
    match expr {
        PlanExpr::ScalarSubquery { subplan, .. } => {
            block_plan_sources(subplan, blocked);
            collect_join_key_blocks(subplan, stats, root_outputs, blocked);
        }
        PlanExpr::BinaryOp { left, right, .. } => {
            collect_expr_join_key_blocks(left, stats, root_outputs, blocked);
            collect_expr_join_key_blocks(right, stats, root_outputs, blocked);
        }
        PlanExpr::UnaryOp { expr, .. }
        | PlanExpr::IsNull { expr, .. }
        | PlanExpr::IsNotNull { expr, .. }
        | PlanExpr::Cast { expr, .. } => {
            collect_expr_join_key_blocks(expr, stats, root_outputs, blocked);
        }
        PlanExpr::Function { args, .. } => {
            for arg in args {
                collect_expr_join_key_blocks(arg, stats, root_outputs, blocked);
            }
        }
        PlanExpr::Between {
            expr, low, high, ..
        } => {
            collect_expr_join_key_blocks(expr, stats, root_outputs, blocked);
            collect_expr_join_key_blocks(low, stats, root_outputs, blocked);
            collect_expr_join_key_blocks(high, stats, root_outputs, blocked);
        }
        PlanExpr::InList { expr, list, .. } => {
            collect_expr_join_key_blocks(expr, stats, root_outputs, blocked);
            for item in list {
                collect_expr_join_key_blocks(item, stats, root_outputs, blocked);
            }
        }
        PlanExpr::CaseExpr {
            operand,
            when_clauses,
            else_result,
            ..
        } => {
            if let Some(operand) = operand {
                collect_expr_join_key_blocks(operand, stats, root_outputs, blocked);
            }
            for (condition, result) in when_clauses {
                collect_expr_join_key_blocks(condition, stats, root_outputs, blocked);
                collect_expr_join_key_blocks(result, stats, root_outputs, blocked);
            }
            if let Some(else_result) = else_result {
                collect_expr_join_key_blocks(else_result, stats, root_outputs, blocked);
            }
        }
        PlanExpr::Column { .. }
        | PlanExpr::Literal { .. }
        | PlanExpr::Wildcard
        | PlanExpr::Parameter { .. } => {}
    }
}

fn block_plan_sources(plan: &LogicalPlan, blocked: &mut HashSet<ColumnSource>) {
    block_plan_output_sources(plan, blocked);
    match plan {
        LogicalPlan::TableScan { .. } | LogicalPlan::ExchangeNode { .. } | LogicalPlan::OneRow => {}
        LogicalPlan::Projection { input, exprs, .. } => {
            block_plan_sources(input, blocked);
            for expr in exprs {
                blocked.extend(expr_sources(expr, input));
            }
        }
        LogicalPlan::Filter { input, predicate } => {
            block_plan_sources(input, blocked);
            blocked.extend(expr_sources(predicate, input));
        }
        LogicalPlan::Join {
            left,
            right,
            condition,
            ..
        } => {
            block_plan_sources(left, blocked);
            block_plan_sources(right, blocked);
            if let JoinCondition::On(expr) = condition {
                blocked.extend(expr_sources_joined(expr, left, right));
            }
        }
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggr_exprs,
            ..
        }
        | LogicalPlan::PartialAggregate {
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
            block_plan_sources(input, blocked);
            for expr in group_by.iter().chain(aggr_exprs) {
                blocked.extend(expr_sources(expr, input));
            }
        }
        LogicalPlan::Sort { input, order_by } => {
            block_plan_sources(input, blocked);
            for sort in order_by {
                blocked.extend(expr_sources(&sort.expr, input));
            }
        }
        LogicalPlan::Limit { input, .. }
        | LogicalPlan::Explain { input, .. }
        | LogicalPlan::Distinct { input }
        | LogicalPlan::AssignUniqueId { input, .. } => block_plan_sources(input, blocked),
        LogicalPlan::Window { input, functions } => {
            block_plan_sources(input, blocked);
            for function in functions {
                for expr in function
                    .args
                    .iter()
                    .chain(function.partition_by.iter())
                    .chain(function.order_by.iter().map(|sort| &sort.expr))
                {
                    blocked.extend(expr_sources(expr, input));
                }
            }
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
            block_plan_sources(left, blocked);
            block_plan_sources(right, blocked);
            blocked.extend(expr_sources(left_key, left));
            blocked.extend(expr_sources(right_key, right));
            block_joined_key_sources(left, right, left_key, right_key, blocked);
            if let Some(residual) = residual {
                blocked.extend(expr_sources_joined(residual, left, right));
            }
        }
        LogicalPlan::UnionAll { inputs } => {
            for input in inputs {
                block_plan_sources(input, blocked);
            }
        }
        LogicalPlan::Intersect { left, right } | LogicalPlan::Except { left, right } => {
            block_plan_sources(left, blocked);
            block_plan_sources(right, blocked);
        }
        LogicalPlan::ScalarSubquery { subplan } => block_plan_sources(subplan, blocked),
        LogicalPlan::CreateTableAsSelect { source, .. }
        | LogicalPlan::InsertInto { source, .. }
        | LogicalPlan::CreateView { plan: source, .. } => block_plan_sources(source, blocked),
        LogicalPlan::CreateTable { .. }
        | LogicalPlan::DropTable { .. }
        | LogicalPlan::DeleteFrom { .. }
        | LogicalPlan::DropView { .. } => {}
    }
}

fn block_plan_output_sources(plan: &LogicalPlan, blocked: &mut HashSet<ColumnSource>) {
    for index in 0..plan.schema().len() {
        blocked.extend(trace_output_column(plan, index).blockable_sources());
    }
}

fn block_joined_key_sources(
    left: &LogicalPlan,
    right: &LogicalPlan,
    left_key: &PlanExpr,
    right_key: &PlanExpr,
    blocked: &mut HashSet<ColumnSource>,
) {
    let left_sources = expr_sources(left_key, left);
    let right_sources = expr_sources(right_key, right);
    if !left_sources.is_empty() && !right_sources.is_empty() {
        blocked.extend(left_sources);
        blocked.extend(right_sources);
    }
}

fn expr_sources(expr: &PlanExpr, input: &LogicalPlan) -> Vec<ColumnSource> {
    let mut sources = Vec::new();
    collect_expr_sources(expr, input, &mut sources);
    sources
}

fn expr_sources_joined(
    expr: &PlanExpr,
    left: &LogicalPlan,
    right: &LogicalPlan,
) -> Vec<ColumnSource> {
    let mut sources = Vec::new();
    collect_joined_expr_sources(expr, left, right, &mut sources);
    sources
}

fn collect_expr_sources(expr: &PlanExpr, input: &LogicalPlan, sources: &mut Vec<ColumnSource>) {
    match expr {
        PlanExpr::Column { index, .. } => {
            sources.extend(trace_output_column(input, *index).blockable_sources());
        }
        PlanExpr::BinaryOp { left, right, .. } => {
            collect_expr_sources(left, input, sources);
            collect_expr_sources(right, input, sources);
        }
        PlanExpr::UnaryOp { expr, .. }
        | PlanExpr::IsNull { expr, .. }
        | PlanExpr::IsNotNull { expr, .. }
        | PlanExpr::Cast { expr, .. } => collect_expr_sources(expr, input, sources),
        PlanExpr::Function { args, .. } => {
            for arg in args {
                collect_expr_sources(arg, input, sources);
            }
        }
        PlanExpr::Between {
            expr, low, high, ..
        } => {
            collect_expr_sources(expr, input, sources);
            collect_expr_sources(low, input, sources);
            collect_expr_sources(high, input, sources);
        }
        PlanExpr::InList { expr, list, .. } => {
            collect_expr_sources(expr, input, sources);
            for item in list {
                collect_expr_sources(item, input, sources);
            }
        }
        PlanExpr::CaseExpr {
            operand,
            when_clauses,
            else_result,
            ..
        } => {
            if let Some(operand) = operand {
                collect_expr_sources(operand, input, sources);
            }
            for (condition, result) in when_clauses {
                collect_expr_sources(condition, input, sources);
                collect_expr_sources(result, input, sources);
            }
            if let Some(else_result) = else_result {
                collect_expr_sources(else_result, input, sources);
            }
        }
        PlanExpr::ScalarSubquery { .. }
        | PlanExpr::Literal { .. }
        | PlanExpr::Wildcard
        | PlanExpr::Parameter { .. } => {}
    }
}

fn collect_joined_expr_sources(
    expr: &PlanExpr,
    left_input: &LogicalPlan,
    right_input: &LogicalPlan,
    sources: &mut Vec<ColumnSource>,
) {
    match expr {
        PlanExpr::Column { index, .. } => {
            let left_width = left_input.schema().len();
            if *index < left_width {
                sources.extend(trace_output_column(left_input, *index).blockable_sources());
            } else {
                sources.extend(
                    trace_output_column(right_input, *index - left_width).blockable_sources(),
                );
            }
        }
        PlanExpr::BinaryOp { left, right, .. } => {
            collect_joined_expr_sources(left, left_input, right_input, sources);
            collect_joined_expr_sources(right, left_input, right_input, sources);
        }
        PlanExpr::UnaryOp { expr, .. }
        | PlanExpr::IsNull { expr, .. }
        | PlanExpr::IsNotNull { expr, .. }
        | PlanExpr::Cast { expr, .. } => {
            collect_joined_expr_sources(expr, left_input, right_input, sources);
        }
        PlanExpr::Function { args, .. } => {
            for arg in args {
                collect_joined_expr_sources(arg, left_input, right_input, sources);
            }
        }
        PlanExpr::Between {
            expr, low, high, ..
        } => {
            collect_joined_expr_sources(expr, left_input, right_input, sources);
            collect_joined_expr_sources(low, left_input, right_input, sources);
            collect_joined_expr_sources(high, left_input, right_input, sources);
        }
        PlanExpr::InList { expr, list, .. } => {
            collect_joined_expr_sources(expr, left_input, right_input, sources);
            for item in list {
                collect_joined_expr_sources(item, left_input, right_input, sources);
            }
        }
        PlanExpr::CaseExpr {
            operand,
            when_clauses,
            else_result,
            ..
        } => {
            if let Some(operand) = operand {
                collect_joined_expr_sources(operand, left_input, right_input, sources);
            }
            for (condition, result) in when_clauses {
                collect_joined_expr_sources(condition, left_input, right_input, sources);
                collect_joined_expr_sources(result, left_input, right_input, sources);
            }
            if let Some(else_result) = else_result {
                collect_joined_expr_sources(else_result, left_input, right_input, sources);
            }
        }
        PlanExpr::ScalarSubquery { .. }
        | PlanExpr::Literal { .. }
        | PlanExpr::Wildcard
        | PlanExpr::Parameter { .. } => {}
    }
}

fn rewrite_plan(
    plan: LogicalPlan,
    stats: &CatalogStats,
    root_outputs: &HashSet<String>,
    blocked: &HashSet<ColumnSource>,
) -> LogicalPlan {
    match plan {
        LogicalPlan::TableScan { .. } => narrow_scan(plan, stats, root_outputs, blocked),
        LogicalPlan::Projection {
            input,
            exprs,
            schema,
        } => {
            let input = rewrite_plan(*input, stats, root_outputs, blocked);
            let input_schema = input.schema();
            let exprs = exprs
                .into_iter()
                .map(|expr| rewrite_expr(expr, stats, root_outputs, blocked))
                .collect::<Vec<_>>();
            let schema = refresh_projection_schema(schema, &exprs, &input_schema);
            LogicalPlan::Projection {
                input: Box::new(input),
                exprs,
                schema,
            }
        }
        LogicalPlan::Filter { input, predicate } => {
            let input = rewrite_plan(*input, stats, root_outputs, blocked);
            LogicalPlan::Filter {
                input: Box::new(input),
                predicate: rewrite_expr(predicate, stats, root_outputs, blocked),
            }
        }
        LogicalPlan::Join {
            left,
            right,
            join_type,
            condition,
            dynamic_filter_ids,
        } => {
            let left = rewrite_plan(*left, stats, root_outputs, blocked);
            let right = rewrite_plan(*right, stats, root_outputs, blocked);
            LogicalPlan::Join {
                left: Box::new(left),
                right: Box::new(right),
                join_type,
                condition: rewrite_join_condition(condition, stats, root_outputs, blocked),
                dynamic_filter_ids,
            }
        }
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggr_exprs,
            schema,
        } => {
            let input = rewrite_plan(*input, stats, root_outputs, blocked);
            let input_schema = input.schema();
            let group_by = group_by
                .into_iter()
                .map(|expr| rewrite_expr(expr, stats, root_outputs, blocked))
                .collect::<Vec<_>>();
            let aggr_exprs = aggr_exprs
                .into_iter()
                .map(|expr| rewrite_expr(expr, stats, root_outputs, blocked))
                .collect::<Vec<_>>();
            let schema = refresh_expr_schema(
                schema,
                group_by.iter().chain(aggr_exprs.iter()),
                &input_schema,
            );
            LogicalPlan::Aggregate {
                input: Box::new(input),
                group_by,
                aggr_exprs,
                schema,
            }
        }
        LogicalPlan::Sort { input, order_by } => LogicalPlan::Sort {
            input: Box::new(rewrite_plan(*input, stats, root_outputs, blocked)),
            order_by: order_by
                .into_iter()
                .map(|mut sort| {
                    sort.expr = rewrite_expr(sort.expr, stats, root_outputs, blocked);
                    sort
                })
                .collect(),
        },
        LogicalPlan::Limit {
            input,
            limit,
            offset,
        } => LogicalPlan::Limit {
            input: Box::new(rewrite_plan(*input, stats, root_outputs, blocked)),
            limit,
            offset,
        },
        LogicalPlan::Explain { input, analyze } => LogicalPlan::Explain {
            input: Box::new(rewrite_plan(*input, stats, root_outputs, blocked)),
            analyze,
        },
        LogicalPlan::PartialAggregate {
            input,
            group_by,
            aggr_exprs,
            schema,
        } => {
            let input = rewrite_plan(*input, stats, root_outputs, blocked);
            let input_schema = input.schema();
            let group_by = group_by
                .into_iter()
                .map(|expr| rewrite_expr(expr, stats, root_outputs, blocked))
                .collect::<Vec<_>>();
            let aggr_exprs = aggr_exprs
                .into_iter()
                .map(|expr| rewrite_expr(expr, stats, root_outputs, blocked))
                .collect::<Vec<_>>();
            let schema = refresh_expr_schema(
                schema,
                group_by.iter().chain(aggr_exprs.iter()),
                &input_schema,
            );
            LogicalPlan::PartialAggregate {
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
        } => {
            let input = rewrite_plan(*input, stats, root_outputs, blocked);
            let input_schema = input.schema();
            let group_by = group_by
                .into_iter()
                .map(|expr| rewrite_expr(expr, stats, root_outputs, blocked))
                .collect::<Vec<_>>();
            let aggr_exprs = aggr_exprs
                .into_iter()
                .map(|expr| rewrite_expr(expr, stats, root_outputs, blocked))
                .collect::<Vec<_>>();
            let schema = refresh_expr_schema(
                schema,
                group_by.iter().chain(aggr_exprs.iter()),
                &input_schema,
            );
            LogicalPlan::FinalAggregate {
                input: Box::new(input),
                group_by,
                aggr_exprs,
                schema,
            }
        }
        LogicalPlan::SemiJoin {
            left,
            right,
            left_key,
            right_key,
            residual,
            dynamic_filter_ids,
        } => {
            let left = rewrite_plan(*left, stats, root_outputs, blocked);
            let right = rewrite_plan(*right, stats, root_outputs, blocked);
            LogicalPlan::SemiJoin {
                left: Box::new(left),
                right: Box::new(right),
                left_key: rewrite_expr(left_key, stats, root_outputs, blocked),
                right_key: rewrite_expr(right_key, stats, root_outputs, blocked),
                residual: residual.map(|expr| rewrite_expr(expr, stats, root_outputs, blocked)),
                dynamic_filter_ids,
            }
        }
        LogicalPlan::AntiJoin {
            left,
            right,
            left_key,
            right_key,
            residual,
        } => {
            let left = rewrite_plan(*left, stats, root_outputs, blocked);
            let right = rewrite_plan(*right, stats, root_outputs, blocked);
            LogicalPlan::AntiJoin {
                left: Box::new(left),
                right: Box::new(right),
                left_key: rewrite_expr(left_key, stats, root_outputs, blocked),
                right_key: rewrite_expr(right_key, stats, root_outputs, blocked),
                residual: residual.map(|expr| rewrite_expr(expr, stats, root_outputs, blocked)),
            }
        }
        LogicalPlan::UnionAll { inputs } => LogicalPlan::UnionAll {
            inputs: inputs
                .into_iter()
                .map(|input| rewrite_plan(input, stats, root_outputs, blocked))
                .collect(),
        },
        LogicalPlan::Distinct { input } => LogicalPlan::Distinct {
            input: Box::new(rewrite_plan(*input, stats, root_outputs, blocked)),
        },
        LogicalPlan::Intersect { left, right } => LogicalPlan::Intersect {
            left: Box::new(rewrite_plan(*left, stats, root_outputs, blocked)),
            right: Box::new(rewrite_plan(*right, stats, root_outputs, blocked)),
        },
        LogicalPlan::Except { left, right } => LogicalPlan::Except {
            left: Box::new(rewrite_plan(*left, stats, root_outputs, blocked)),
            right: Box::new(rewrite_plan(*right, stats, root_outputs, blocked)),
        },
        LogicalPlan::Window { input, functions } => LogicalPlan::Window {
            input: Box::new(rewrite_plan(*input, stats, root_outputs, blocked)),
            functions: functions
                .into_iter()
                .map(|mut function| {
                    function.args = function
                        .args
                        .into_iter()
                        .map(|expr| rewrite_expr(expr, stats, root_outputs, blocked))
                        .collect();
                    function.partition_by = function
                        .partition_by
                        .into_iter()
                        .map(|expr| rewrite_expr(expr, stats, root_outputs, blocked))
                        .collect();
                    function.order_by = function
                        .order_by
                        .into_iter()
                        .map(|mut sort| {
                            sort.expr = rewrite_expr(sort.expr, stats, root_outputs, blocked);
                            sort
                        })
                        .collect();
                    function
                })
                .collect(),
        },
        LogicalPlan::AssignUniqueId { input, id_column } => LogicalPlan::AssignUniqueId {
            input: Box::new(rewrite_plan(*input, stats, root_outputs, blocked)),
            id_column,
        },
        LogicalPlan::ScalarSubquery { subplan } => LogicalPlan::ScalarSubquery {
            subplan: Box::new(rewrite_plan(*subplan, stats, root_outputs, blocked)),
        },
        other => other,
    }
}

fn rewrite_join_condition(
    condition: JoinCondition,
    stats: &CatalogStats,
    root_outputs: &HashSet<String>,
    blocked: &HashSet<ColumnSource>,
) -> JoinCondition {
    match condition {
        JoinCondition::On(expr) => {
            JoinCondition::On(rewrite_expr(expr, stats, root_outputs, blocked))
        }
        JoinCondition::None => JoinCondition::None,
    }
}

fn rewrite_expr(
    expr: PlanExpr,
    stats: &CatalogStats,
    root_outputs: &HashSet<String>,
    blocked: &HashSet<ColumnSource>,
) -> PlanExpr {
    match expr {
        PlanExpr::BinaryOp {
            left,
            op,
            right,
            span,
        } => PlanExpr::BinaryOp {
            left: Box::new(rewrite_expr(*left, stats, root_outputs, blocked)),
            op,
            right: Box::new(rewrite_expr(*right, stats, root_outputs, blocked)),
            span,
        },
        PlanExpr::UnaryOp { op, expr, span } => PlanExpr::UnaryOp {
            op,
            expr: Box::new(rewrite_expr(*expr, stats, root_outputs, blocked)),
            span,
        },
        PlanExpr::Function {
            name,
            args,
            distinct,
            span,
        } => PlanExpr::Function {
            name,
            args: args
                .into_iter()
                .map(|arg| rewrite_expr(arg, stats, root_outputs, blocked))
                .collect(),
            distinct,
            span,
        },
        PlanExpr::IsNull { expr, span } => PlanExpr::IsNull {
            expr: Box::new(rewrite_expr(*expr, stats, root_outputs, blocked)),
            span,
        },
        PlanExpr::IsNotNull { expr, span } => PlanExpr::IsNotNull {
            expr: Box::new(rewrite_expr(*expr, stats, root_outputs, blocked)),
            span,
        },
        PlanExpr::Between {
            expr,
            negated,
            low,
            high,
            span,
        } => PlanExpr::Between {
            expr: Box::new(rewrite_expr(*expr, stats, root_outputs, blocked)),
            negated,
            low: Box::new(rewrite_expr(*low, stats, root_outputs, blocked)),
            high: Box::new(rewrite_expr(*high, stats, root_outputs, blocked)),
            span,
        },
        PlanExpr::InList {
            expr,
            list,
            negated,
            span,
        } => PlanExpr::InList {
            expr: Box::new(rewrite_expr(*expr, stats, root_outputs, blocked)),
            list: list
                .into_iter()
                .map(|item| rewrite_expr(item, stats, root_outputs, blocked))
                .collect(),
            negated,
            span,
        },
        PlanExpr::Cast {
            expr,
            data_type,
            span,
        } => PlanExpr::Cast {
            expr: Box::new(rewrite_expr(*expr, stats, root_outputs, blocked)),
            data_type,
            span,
        },
        PlanExpr::ScalarSubquery { subplan, span } => PlanExpr::ScalarSubquery {
            subplan: Box::new(rewrite_plan(*subplan, stats, root_outputs, blocked)),
            span,
        },
        PlanExpr::CaseExpr {
            operand,
            when_clauses,
            else_result,
            span,
        } => PlanExpr::CaseExpr {
            operand: operand
                .map(|expr| Box::new(rewrite_expr(*expr, stats, root_outputs, blocked))),
            when_clauses: when_clauses
                .into_iter()
                .map(|(condition, result)| {
                    (
                        rewrite_expr(condition, stats, root_outputs, blocked),
                        rewrite_expr(result, stats, root_outputs, blocked),
                    )
                })
                .collect(),
            else_result: else_result
                .map(|expr| Box::new(rewrite_expr(*expr, stats, root_outputs, blocked))),
            span,
        },
        PlanExpr::Column { .. }
        | PlanExpr::Literal { .. }
        | PlanExpr::Wildcard
        | PlanExpr::Parameter { .. } => expr,
    }
}

fn narrow_scan(
    plan: LogicalPlan,
    stats: &CatalogStats,
    root_outputs: &HashSet<String>,
    blocked: &HashSet<ColumnSource>,
) -> LogicalPlan {
    let LogicalPlan::TableScan { table, schema, .. } = &plan else {
        return plan;
    };

    let mut changed = false;
    let mut exprs = Vec::with_capacity(schema.len());
    let mut output_schema = Vec::with_capacity(schema.len());
    for (index, col) in schema.iter().enumerate() {
        let source = ColumnSource {
            table: table.clone(),
            column: col.name.clone(),
            data_type: col.data_type.clone(),
        };
        let base_col = PlanExpr::Column {
            index,
            name: col.name.clone(),
            span: None,
        };
        if !blocked.contains(&source) && source_can_narrow(&source, stats, root_outputs) {
            changed = true;
            exprs.push(PlanExpr::Cast {
                expr: Box::new(base_col),
                data_type: DataType::Int32,
                span: None,
            });
            output_schema.push(ColumnInfo {
                name: col.name.clone(),
                data_type: DataType::Int32,
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

fn refresh_projection_schema(
    mut schema: Vec<ColumnInfo>,
    exprs: &[PlanExpr],
    input_schema: &[ColumnInfo],
) -> Vec<ColumnInfo> {
    for (col, expr) in schema.iter_mut().zip(exprs) {
        if let Some(data_type) = refreshed_expr_type(expr, input_schema) {
            col.data_type = data_type;
        }
    }
    schema
}

fn refresh_expr_schema<'a>(
    mut schema: Vec<ColumnInfo>,
    exprs: impl Iterator<Item = &'a PlanExpr>,
    input_schema: &[ColumnInfo],
) -> Vec<ColumnInfo> {
    for (col, expr) in schema.iter_mut().zip(exprs) {
        if let Some(data_type) = refreshed_expr_type(expr, input_schema) {
            col.data_type = data_type;
        }
    }
    schema
}

fn refreshed_expr_type(expr: &PlanExpr, input_schema: &[ColumnInfo]) -> Option<DataType> {
    plan_expr_type(expr, input_schema)
}

fn source_can_narrow(
    source: &ColumnSource,
    stats: &CatalogStats,
    root_outputs: &HashSet<String>,
) -> bool {
    if root_outputs.contains(&source.column) {
        return false;
    }
    if source.data_type != DataType::Int64 {
        return false;
    }
    if narrow_keys_allowlist().contains(&source.column) {
        return true;
    }
    stats
        .get(&source.table)
        .and_then(|table| table.columns.get(&source.column))
        .is_some_and(column_stats_fit_i32)
}

fn column_stats_fit_i32(stats: &ColumnStatistics) -> bool {
    let Some(min) = scalar_to_i64(stats.min_value.as_ref()) else {
        return false;
    };
    let Some(max) = scalar_to_i64(stats.max_value.as_ref()) else {
        return false;
    };
    min >= i32::MIN as i64 && max <= i32::MAX as i64
}

fn scalar_to_i64(value: Option<&ScalarValue>) -> Option<i64> {
    match value? {
        ScalarValue::Int64(v) => Some(*v),
        ScalarValue::Int32(v) => Some(*v as i64),
        _ => None,
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

fn pure_equi_join_pairs(
    condition: &JoinCondition,
    left_width: usize,
) -> Option<Vec<(usize, usize)>> {
    let JoinCondition::On(expr) = condition else {
        return None;
    };
    let mut pairs = Vec::new();
    if collect_pure_equi_join_pairs(expr, left_width, &mut pairs) && !pairs.is_empty() {
        Some(pairs)
    } else {
        None
    }
}

fn collect_pure_equi_join_pairs(
    expr: &PlanExpr,
    left_width: usize,
    pairs: &mut Vec<(usize, usize)>,
) -> bool {
    match expr {
        PlanExpr::BinaryOp {
            left,
            op: ast::BinaryOp::And,
            right,
            ..
        } => {
            collect_pure_equi_join_pairs(left, left_width, pairs)
                && collect_pure_equi_join_pairs(right, left_width, pairs)
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
                    return true;
                }
                if *r < left_width && *l >= left_width {
                    pairs.push((*r, *l - left_width));
                    return true;
                }
            }
            false
        }
        _ => false,
    }
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
    use std::sync::Arc;

    use arneb_catalog::{ColumnStatistics, TableStatistics};

    use super::*;

    type TestColumnStats<'a> = (&'a str, Option<i64>, Option<i64>);
    type TestTableStats<'a> = (&'a str, Vec<TestColumnStats<'a>>);

    #[cfg(test)]
    struct NarrowKeysOverride {
        _guard: std::sync::MutexGuard<'static, ()>,
    }

    #[cfg(test)]
    impl Drop for NarrowKeysOverride {
        fn drop(&mut self) {
            *NARROW_KEYS_TEST_OVERRIDE
                .get_or_init(|| std::sync::Mutex::new(None))
                .lock()
                .expect("NARROW_KEYS_TEST_OVERRIDE mutex poisoned") = None;
        }
    }

    #[cfg(test)]
    fn set_narrow_keys_for_test(enabled: bool) -> NarrowKeysOverride {
        static LOCK: OnceLock<std::sync::Mutex<()>> = OnceLock::new();
        let guard = LOCK
            .get_or_init(|| std::sync::Mutex::new(()))
            .lock()
            .expect("NARROW_KEYS_TEST_LOCK mutex poisoned");
        *NARROW_KEYS_TEST_OVERRIDE
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("NARROW_KEYS_TEST_OVERRIDE mutex poisoned") = Some(enabled);
        NarrowKeysOverride { _guard: guard }
    }

    fn ci(name: &str, data_type: DataType) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            data_type,
            nullable: true,
        }
    }

    fn scan(table: &str, cols: Vec<ColumnInfo>) -> LogicalPlan {
        LogicalPlan::TableScan {
            table: TableReference::table(table),
            schema: cols,
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

    fn lit_i64(value: i64) -> PlanExpr {
        PlanExpr::Literal {
            value: ScalarValue::Int64(value),
            span: None,
        }
    }

    fn lit_utf8(value: &str) -> PlanExpr {
        PlanExpr::Literal {
            value: ScalarValue::Utf8(value.to_string()),
            span: None,
        }
    }

    fn project(input: LogicalPlan, index: usize, name: &str) -> LogicalPlan {
        LogicalPlan::Projection {
            input: Box::new(input),
            exprs: vec![col(index, name)],
            schema: vec![ci(name, DataType::Int64)],
        }
    }

    fn project_many(
        input: LogicalPlan,
        exprs: Vec<PlanExpr>,
        schema: Vec<ColumnInfo>,
    ) -> LogicalPlan {
        LogicalPlan::Projection {
            input: Box::new(input),
            exprs,
            schema,
        }
    }

    fn stats(table: &str, columns: Vec<(&str, Option<i64>, Option<i64>)>) -> Arc<CatalogStats> {
        let mut catalog = CatalogStats::new();
        let mut table_stats = TableStatistics::default();
        for (name, min, max) in columns {
            table_stats.columns.insert(
                name.to_string(),
                ColumnStatistics {
                    min_value: min.map(ScalarValue::Int64),
                    max_value: max.map(ScalarValue::Int64),
                    ..Default::default()
                },
            );
        }
        catalog.insert(TableReference::table(table), table_stats);
        Arc::new(catalog)
    }

    fn stats_many(tables: Vec<TestTableStats<'_>>) -> Arc<CatalogStats> {
        let mut catalog = CatalogStats::new();
        for (table, columns) in tables {
            let mut table_stats = TableStatistics::default();
            for (name, min, max) in columns {
                table_stats.columns.insert(
                    name.to_string(),
                    ColumnStatistics {
                        min_value: min.map(ScalarValue::Int64),
                        max_value: max.map(ScalarValue::Int64),
                        ..Default::default()
                    },
                );
            }
            catalog.insert(TableReference::table(table), table_stats);
        }
        Arc::new(catalog)
    }

    fn analyze(plan: LogicalPlan, stats: Arc<CatalogStats>) -> LogicalPlan {
        let mut ctx = AnalyzerContext::with_stats(stats);
        NarrowKeys::new().analyze(plan, &mut ctx).unwrap()
    }

    // Diagnostic: walk the plan, return true if any Filter predicate mentions `needle`.
    fn has_filter_mentioning(plan: &LogicalPlan, needle: &str) -> bool {
        fn expr_mentions(expr: &PlanExpr, needle: &str) -> bool {
            let mut found = false;
            let s = format!("{expr:?}");
            if s.contains(needle) {
                found = true;
            }
            found
        }
        let mut found = false;
        match plan {
            LogicalPlan::Filter { input, predicate } => {
                if expr_mentions(predicate, needle) {
                    found = true;
                }
                found |= has_filter_mentioning(input, needle);
            }
            LogicalPlan::Projection { input, .. }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::PartialAggregate { input, .. }
            | LogicalPlan::FinalAggregate { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Distinct { input }
            | LogicalPlan::Window { input, .. }
            | LogicalPlan::Explain { input, .. }
            | LogicalPlan::AssignUniqueId { input, .. } => {
                found |= has_filter_mentioning(input, needle);
            }
            LogicalPlan::Join { left, right, .. }
            | LogicalPlan::Intersect { left, right }
            | LogicalPlan::Except { left, right } => {
                found |= has_filter_mentioning(left, needle);
                found |= has_filter_mentioning(right, needle);
            }
            LogicalPlan::SemiJoin { left, right, .. }
            | LogicalPlan::AntiJoin { left, right, .. } => {
                found |= has_filter_mentioning(left, needle);
                found |= has_filter_mentioning(right, needle);
            }
            LogicalPlan::UnionAll { inputs } => {
                for i in inputs {
                    found |= has_filter_mentioning(i, needle);
                }
            }
            LogicalPlan::ScalarSubquery { subplan } => {
                found |= has_filter_mentioning(subplan, needle);
            }
            _ => {}
        }
        found
    }

    // Diagnostic: print every fragment's (type, has-filter, top-node) so we can
    // see WHERE a probe-side filter landed — folded into a Source worker fragment
    // (good) vs stranded above the scan-exchange (the q12 narrow regression).
    fn dump_fragments(frag: &crate::fragment::PlanFragment, needle: &str, depth: usize) {
        let top = match &frag.root {
            LogicalPlan::Filter { .. } => "Filter",
            LogicalPlan::Projection { .. } => "Projection",
            LogicalPlan::Join { .. } => "Join",
            LogicalPlan::Aggregate { .. } => "Aggregate",
            LogicalPlan::PartialAggregate { .. } => "PartialAggregate",
            LogicalPlan::FinalAggregate { .. } => "FinalAggregate",
            LogicalPlan::TableScan { .. } => "TableScan",
            LogicalPlan::ExchangeNode { .. } => "ExchangeNode",
            other => {
                let _ = other;
                "other"
            }
        };
        eprintln!(
            "{:indent$}frag id={} type={:?} top={} has_{}={}",
            "",
            frag.id.0,
            frag.fragment_type,
            top,
            needle,
            has_filter_mentioning(&frag.root, needle),
            indent = depth * 2,
        );
        for src in &frag.source_fragments {
            dump_fragments(src, needle, depth + 1);
        }
    }

    #[allow(dead_code)]
    fn source_frag_has_filter(frag: &crate::fragment::PlanFragment, needle: &str) -> bool {
        let mut found = matches!(frag.fragment_type, crate::fragment::FragmentType::Source)
            && has_filter_mentioning(&frag.root, needle);
        for src in &frag.source_fragments {
            found |= source_frag_has_filter(src, needle);
        }
        found
    }

    fn plan_has_table_scan(plan: &LogicalPlan, table: &str) -> bool {
        match plan {
            LogicalPlan::TableScan { table: t, .. } => t.table == table,
            LogicalPlan::Projection { input, .. }
            | LogicalPlan::Filter { input, .. }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::PartialAggregate { input, .. }
            | LogicalPlan::FinalAggregate { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Distinct { input }
            | LogicalPlan::Window { input, .. }
            | LogicalPlan::Explain { input, .. }
            | LogicalPlan::AssignUniqueId { input, .. } => plan_has_table_scan(input, table),
            LogicalPlan::Join { left, right, .. }
            | LogicalPlan::Intersect { left, right }
            | LogicalPlan::Except { left, right }
            | LogicalPlan::SemiJoin { left, right, .. }
            | LogicalPlan::AntiJoin { left, right, .. } => {
                plan_has_table_scan(left, table) || plan_has_table_scan(right, table)
            }
            LogicalPlan::UnionAll { inputs } => {
                inputs.iter().any(|i| plan_has_table_scan(i, table))
            }
            LogicalPlan::ScalarSubquery { subplan } => plan_has_table_scan(subplan, table),
            _ => false,
        }
    }

    // The fragment that DIRECTLY owns `table`'s TableScan (root contains the
    // scan, not via an ExchangeNode) must also carry `needle`'s filter, i.e. the
    // worker filters before shipping. Returns Some(true/false) for that fragment.
    fn scan_owner_frag_has_filter(
        frag: &crate::fragment::PlanFragment,
        table: &str,
        needle: &str,
    ) -> Option<bool> {
        if plan_has_table_scan(&frag.root, table) {
            return Some(has_filter_mentioning(&frag.root, needle));
        }
        for src in &frag.source_fragments {
            if let Some(v) = scan_owner_frag_has_filter(src, table, needle) {
                return Some(v);
            }
        }
        None
    }

    fn frag_has_filter(frag: &crate::fragment::PlanFragment, needle: &str) -> bool {
        let mut found = has_filter_mentioning(&frag.root, needle);
        for src in &frag.source_fragments {
            found |= frag_has_filter(src, needle);
        }
        found
    }

    // Regression: the broadcast build-side-swap + v2b-inline fragmenter path
    // (fragment.rs) inlines the join into the probe SOURCE fragment by taking
    // `left_frags[0].root` (the bare scan) and DISCARDING the `left_plan`
    // wrapper. Narrow-OFF the probe-side WHERE filter folds INTO the scan
    // fragment (Filter directly above the scan-exchange), so it survives.
    // Narrow-ON the int32 cast-Projection sits between the Filter and the
    // scan, so the filter can no longer fold — it stays in `left_plan` and is
    // silently dropped, producing q19's ~28x over-count at SF30 (the probe-side
    // predicate vanishes while the build-side predicate is preserved).
    #[test]
    fn narrow_probe_filter_survives_v2b_inline_broadcast() {
        let _guard = set_narrow_keys_for_test(true);

        // q19-shape: lineitem JOIN part ON l_partkey=p_partkey WHERE l_shipinstruct='X'
        let lineitem = scan(
            "lineitem",
            vec![
                ci("l_partkey", DataType::Int64),
                ci("l_shipinstruct", DataType::Utf8),
            ],
        );
        let part = scan(
            "part",
            vec![
                ci("p_partkey", DataType::Int64),
                ci("p_brand", DataType::Utf8),
            ],
        );
        let join = LogicalPlan::Join {
            left: Box::new(lineitem),
            right: Box::new(part),
            join_type: ast::JoinType::Inner,
            condition: JoinCondition::On(PlanExpr::BinaryOp {
                left: Box::new(col(0, "l_partkey")),
                op: ast::BinaryOp::Eq,
                right: Box::new(col(2, "p_partkey")),
                span: None,
            }),
            dynamic_filter_ids: Vec::new(),
        };
        let filtered = LogicalPlan::Filter {
            input: Box::new(join),
            predicate: PlanExpr::BinaryOp {
                left: Box::new(col(1, "l_shipinstruct")),
                op: ast::BinaryOp::Eq,
                right: Box::new(lit_utf8("DELIVER IN PERSON")),
                span: None,
            },
        };
        // COUNT(*)-shaped top, matching the SF30 isolation measurement query.
        let plan = LogicalPlan::Aggregate {
            input: Box::new(filtered),
            group_by: vec![],
            aggr_exprs: vec![PlanExpr::Function {
                name: "COUNT".to_string(),
                args: vec![lit_i64(1)],
                distinct: false,
                span: None,
            }],
            schema: vec![ci("count", DataType::Int64)],
        };

        // Analyzer stats (min/max prove the int32 narrow is overflow-safe).
        let st = stats_many(vec![
            ("lineitem", vec![("l_partkey", Some(1), Some(6_000_000))]),
            ("part", vec![("p_partkey", Some(1), Some(6_000_000))]),
        ]);
        let mut ctx = AnalyzerContext::with_stats(st);
        let analyzed = crate::analyzer::Analyzer::default_pipeline()
            .run(plan, &mut ctx)
            .unwrap();
        // The logical pipeline must preserve the probe-side filter.
        assert!(
            has_filter_mentioning(&analyzed, "l_shipinstruct"),
            "analyzer dropped the probe-side filter"
        );

        // Fragmenter stats with row_count so the build-side swap + v2b-inline
        // broadcast path fires: part (small) broadcast-eligible, lineitem (big)
        // not.
        let mut frag_stats = CatalogStats::new();
        let mut li = TableStatistics {
            row_count: Some(1_800_000_000),
            ..Default::default()
        };
        li.columns.insert(
            "l_partkey".into(),
            ColumnStatistics {
                min_value: Some(ScalarValue::Int64(1)),
                max_value: Some(ScalarValue::Int64(6_000_000)),
                ..Default::default()
            },
        );
        frag_stats.insert(TableReference::table("lineitem"), li);
        let mut pt = TableStatistics {
            row_count: Some(6_000_000),
            ..Default::default()
        };
        pt.columns.insert(
            "p_partkey".into(),
            ColumnStatistics {
                min_value: Some(ScalarValue::Int64(1)),
                max_value: Some(ScalarValue::Int64(6_000_000)),
                ..Default::default()
            },
        );
        frag_stats.insert(TableReference::table("part"), pt);
        let frag_stats = Arc::new(frag_stats);

        let mut fragmenter = crate::fragment::PlanFragmenter::new()
            .with_worker_count(2)
            .with_broadcast_threshold(Some(200_000_000))
            .with_stats(Some(frag_stats));
        let root = fragmenter.fragment(analyzed);

        assert!(
            frag_has_filter(&root, "l_shipinstruct"),
            "v2b-inline broadcast path dropped the probe-side filter under narrow-ON \
             (the bare scan fragment root was used as probe, discarding the \
             Filter/cast-Projection wrapper)"
        );
    }

    // Regression for the q12 narrow PERF regression: in a q12-shape plan
    // (orders ⋈ lineitem, all filters on lineitem) the lineitem filter must
    // FOLD into the lineitem SCAN worker fragment so the worker filters before
    // shipping. OFF: Filter→Scan folds (worker ships filtered, small). ON: the
    // int32 cast-Projection between Filter and Scan blocks the fold → the scan
    // fragment ships the FULL 180M lineitem and the filter runs post-exchange
    // (q12 measured 6.26GB / 2.6x lat at SF30). The fold must see through the
    // narrow cast-Projection.
    #[test]
    fn narrow_lineitem_filter_folds_into_scan_fragment() {
        fn build_and_fragment(narrow: bool) -> crate::fragment::PlanFragment {
            let _guard = set_narrow_keys_for_test(narrow);
            let orders = scan(
                "orders",
                vec![
                    ci("o_orderkey", DataType::Int64),
                    ci("o_orderpriority", DataType::Utf8),
                ],
            );
            let lineitem = scan(
                "lineitem",
                vec![
                    ci("l_orderkey", DataType::Int64),
                    ci("l_shipmode", DataType::Utf8),
                ],
            );
            let join = LogicalPlan::Join {
                left: Box::new(orders),
                right: Box::new(lineitem),
                join_type: ast::JoinType::Inner,
                condition: JoinCondition::On(PlanExpr::BinaryOp {
                    left: Box::new(col(0, "o_orderkey")),
                    op: ast::BinaryOp::Eq,
                    right: Box::new(col(2, "l_orderkey")),
                    span: None,
                }),
                dynamic_filter_ids: Vec::new(),
            };
            let filtered = LogicalPlan::Filter {
                input: Box::new(join),
                predicate: PlanExpr::BinaryOp {
                    left: Box::new(col(3, "l_shipmode")),
                    op: ast::BinaryOp::Eq,
                    right: Box::new(lit_utf8("MAIL")),
                    span: None,
                },
            };
            let plan = LogicalPlan::Aggregate {
                input: Box::new(filtered),
                group_by: vec![col(3, "l_shipmode")],
                aggr_exprs: vec![PlanExpr::Function {
                    name: "COUNT".to_string(),
                    args: vec![lit_i64(1)],
                    distinct: false,
                    span: None,
                }],
                schema: vec![ci("l_shipmode", DataType::Utf8), ci("cnt", DataType::Int64)],
            };
            let st = stats_many(vec![
                ("orders", vec![("o_orderkey", Some(1), Some(45_000_000))]),
                ("lineitem", vec![("l_orderkey", Some(1), Some(180_000_000))]),
            ]);
            let mut ctx = AnalyzerContext::with_stats(st);
            let analyzed = crate::analyzer::Analyzer::default_pipeline()
                .run(plan, &mut ctx)
                .unwrap();
            let mut fs = CatalogStats::new();
            fs.insert(
                TableReference::table("orders"),
                TableStatistics {
                    row_count: Some(45_000_000),
                    ..Default::default()
                },
            );
            fs.insert(
                TableReference::table("lineitem"),
                TableStatistics {
                    row_count: Some(180_000_000),
                    ..Default::default()
                },
            );
            crate::fragment::PlanFragmenter::new()
                .with_worker_count(2)
                .with_broadcast_threshold(Some(1_000_000_000))
                .with_stats(Some(Arc::new(fs)))
                .fragment(analyzed)
        }

        let off = build_and_fragment(false);
        eprintln!("=== q12-shape NARROW-OFF fragments ===");
        dump_fragments(&off, "l_shipmode", 0);
        let off_folded = scan_owner_frag_has_filter(&off, "lineitem", "l_shipmode");

        let on = build_and_fragment(true);
        eprintln!("=== q12-shape NARROW-ON fragments ===");
        dump_fragments(&on, "l_shipmode", 0);
        let on_folded = scan_owner_frag_has_filter(&on, "lineitem", "l_shipmode");

        eprintln!(
            "OFF scan-owner-has-filter={off_folded:?}  ON scan-owner-has-filter={on_folded:?}"
        );

        // Sanity: OFF must fold the filter into the lineitem scan fragment.
        assert_eq!(
            off_folded,
            Some(true),
            "narrow-OFF: lineitem filter should fold into the scan worker fragment"
        );
        // The fix: narrow-ON must ALSO fold the filter into the scan fragment
        // (through the cast-Projection) so the worker ships filtered rows.
        assert_eq!(
            on_folded,
            Some(true),
            "narrow-ON: lineitem filter did NOT fold into the scan fragment — the \
             int32 cast-Projection blocked the fold, so the full lineitem is shipped \
             and filtered post-exchange (q12 6.26GB / 2.6x regression)"
        );
    }

    fn assert_projection_col_type(plan: &LogicalPlan, index: usize, data_type: DataType) {
        let LogicalPlan::Projection { schema, .. } = plan else {
            panic!("expected projection, got {plan:?}");
        };
        assert_eq!(schema[index].data_type, data_type);
    }

    fn assert_schema_names(plan: &LogicalPlan, names: &[&str]) {
        let actual: Vec<String> = plan.schema().iter().map(|col| col.name.clone()).collect();
        let expected: Vec<String> = names.iter().map(|name| (*name).to_string()).collect();
        assert_eq!(actual, expected);
    }

    fn assert_schema_lengths_consistent(plan: &LogicalPlan) {
        match plan {
            LogicalPlan::Projection {
                input,
                exprs,
                schema,
            } => {
                assert_eq!(schema.len(), exprs.len());
                assert_schema_lengths_consistent(input);
            }
            LogicalPlan::Filter { input, predicate } => {
                assert_expr_schema_lengths_consistent(predicate);
                assert_schema_lengths_consistent(input);
            }
            LogicalPlan::Join { left, right, .. } => {
                assert_eq!(
                    plan.schema().len(),
                    left.schema().len() + right.schema().len()
                );
                assert_schema_lengths_consistent(left);
                assert_schema_lengths_consistent(right);
            }
            LogicalPlan::Aggregate {
                input,
                group_by,
                aggr_exprs,
                schema,
            }
            | LogicalPlan::PartialAggregate {
                input,
                group_by,
                aggr_exprs,
                schema,
            }
            | LogicalPlan::FinalAggregate {
                input,
                group_by,
                aggr_exprs,
                schema,
            } => {
                assert_eq!(schema.len(), group_by.len() + aggr_exprs.len());
                assert_schema_lengths_consistent(input);
            }
            LogicalPlan::Sort { input, order_by } => {
                for sort in order_by {
                    assert_expr_schema_lengths_consistent(&sort.expr);
                }
                assert_schema_lengths_consistent(input);
            }
            LogicalPlan::Limit { input, .. }
            | LogicalPlan::Explain { input, .. }
            | LogicalPlan::Distinct { input }
            | LogicalPlan::AssignUniqueId { input, .. } => assert_schema_lengths_consistent(input),
            LogicalPlan::Window { input, functions } => {
                assert_eq!(plan.schema().len(), input.schema().len() + functions.len());
                for function in functions {
                    for expr in function
                        .args
                        .iter()
                        .chain(function.partition_by.iter())
                        .chain(function.order_by.iter().map(|sort| &sort.expr))
                    {
                        assert_expr_schema_lengths_consistent(expr);
                    }
                }
                assert_schema_lengths_consistent(input);
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
                assert_eq!(plan.schema().len(), left.schema().len());
                assert_expr_schema_lengths_consistent(left_key);
                assert_expr_schema_lengths_consistent(right_key);
                if let Some(residual) = residual {
                    assert_expr_schema_lengths_consistent(residual);
                }
                assert_schema_lengths_consistent(left);
                assert_schema_lengths_consistent(right);
            }
            LogicalPlan::UnionAll { inputs } => {
                if let Some(first) = inputs.first() {
                    for input in inputs {
                        assert_eq!(input.schema().len(), first.schema().len());
                        assert_schema_lengths_consistent(input);
                    }
                }
            }
            LogicalPlan::Intersect { left, right } | LogicalPlan::Except { left, right } => {
                assert_eq!(left.schema().len(), right.schema().len());
                assert_schema_lengths_consistent(left);
                assert_schema_lengths_consistent(right);
            }
            LogicalPlan::ScalarSubquery { subplan } => {
                assert!(subplan.schema().len() <= 1);
                assert_schema_lengths_consistent(subplan);
            }
            LogicalPlan::CreateTableAsSelect { source, .. }
            | LogicalPlan::InsertInto { source, .. }
            | LogicalPlan::CreateView { plan: source, .. } => {
                assert_schema_lengths_consistent(source)
            }
            LogicalPlan::TableScan { .. }
            | LogicalPlan::ExchangeNode { .. }
            | LogicalPlan::CreateTable { .. }
            | LogicalPlan::DropTable { .. }
            | LogicalPlan::DeleteFrom { .. }
            | LogicalPlan::DropView { .. }
            | LogicalPlan::OneRow => {}
        }
    }

    fn assert_expr_schema_lengths_consistent(expr: &PlanExpr) {
        match expr {
            PlanExpr::ScalarSubquery { subplan, .. } => assert_schema_lengths_consistent(subplan),
            PlanExpr::BinaryOp { left, right, .. } => {
                assert_expr_schema_lengths_consistent(left);
                assert_expr_schema_lengths_consistent(right);
            }
            PlanExpr::UnaryOp { expr, .. }
            | PlanExpr::IsNull { expr, .. }
            | PlanExpr::IsNotNull { expr, .. }
            | PlanExpr::Cast { expr, .. } => assert_expr_schema_lengths_consistent(expr),
            PlanExpr::Function { args, .. } => {
                for arg in args {
                    assert_expr_schema_lengths_consistent(arg);
                }
            }
            PlanExpr::Between {
                expr, low, high, ..
            } => {
                assert_expr_schema_lengths_consistent(expr);
                assert_expr_schema_lengths_consistent(low);
                assert_expr_schema_lengths_consistent(high);
            }
            PlanExpr::InList { expr, list, .. } => {
                assert_expr_schema_lengths_consistent(expr);
                for item in list {
                    assert_expr_schema_lengths_consistent(item);
                }
            }
            PlanExpr::CaseExpr {
                operand,
                when_clauses,
                else_result,
                ..
            } => {
                if let Some(operand) = operand {
                    assert_expr_schema_lengths_consistent(operand);
                }
                for (condition, result) in when_clauses {
                    assert_expr_schema_lengths_consistent(condition);
                    assert_expr_schema_lengths_consistent(result);
                }
                if let Some(else_result) = else_result {
                    assert_expr_schema_lengths_consistent(else_result);
                }
            }
            PlanExpr::Column { .. }
            | PlanExpr::Literal { .. }
            | PlanExpr::Wildcard
            | PlanExpr::Parameter { .. } => {}
        }
    }

    fn assert_semi_anti_key_types_match(plan: &LogicalPlan) {
        match plan {
            LogicalPlan::SemiJoin {
                left,
                right,
                left_key,
                right_key,
                ..
            }
            | LogicalPlan::AntiJoin {
                left,
                right,
                left_key,
                right_key,
                ..
            } => {
                let left_type = plan_expr_type(left_key, &left.schema()).unwrap();
                let right_type = plan_expr_type(right_key, &right.schema()).unwrap();
                assert_eq!(left_type, right_type);
                assert_semi_anti_key_types_match(left);
                assert_semi_anti_key_types_match(right);
            }
            LogicalPlan::Projection { input, exprs, .. } => {
                for expr in exprs {
                    assert_expr_subquery_key_types_match(expr);
                }
                assert_semi_anti_key_types_match(input);
            }
            LogicalPlan::Filter { input, predicate } => {
                assert_expr_subquery_key_types_match(predicate);
                assert_semi_anti_key_types_match(input);
            }
            LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Explain { input, .. }
            | LogicalPlan::PartialAggregate { input, .. }
            | LogicalPlan::FinalAggregate { input, .. }
            | LogicalPlan::Distinct { input }
            | LogicalPlan::Window { input, .. }
            | LogicalPlan::AssignUniqueId { input, .. } => assert_semi_anti_key_types_match(input),
            LogicalPlan::Join { left, right, .. }
            | LogicalPlan::Intersect { left, right }
            | LogicalPlan::Except { left, right } => {
                assert_semi_anti_key_types_match(left);
                assert_semi_anti_key_types_match(right);
            }
            LogicalPlan::UnionAll { inputs } => {
                for input in inputs {
                    assert_semi_anti_key_types_match(input);
                }
            }
            LogicalPlan::ScalarSubquery { subplan } => assert_semi_anti_key_types_match(subplan),
            _ => {}
        }
    }

    fn assert_expr_subquery_key_types_match(expr: &PlanExpr) {
        match expr {
            PlanExpr::ScalarSubquery { subplan, .. } => assert_semi_anti_key_types_match(subplan),
            PlanExpr::BinaryOp { left, right, .. } => {
                assert_expr_subquery_key_types_match(left);
                assert_expr_subquery_key_types_match(right);
            }
            PlanExpr::UnaryOp { expr, .. }
            | PlanExpr::IsNull { expr, .. }
            | PlanExpr::IsNotNull { expr, .. }
            | PlanExpr::Cast { expr, .. } => assert_expr_subquery_key_types_match(expr),
            PlanExpr::Function { args, .. } => {
                for arg in args {
                    assert_expr_subquery_key_types_match(arg);
                }
            }
            PlanExpr::Between {
                expr, low, high, ..
            } => {
                assert_expr_subquery_key_types_match(expr);
                assert_expr_subquery_key_types_match(low);
                assert_expr_subquery_key_types_match(high);
            }
            PlanExpr::InList { expr, list, .. } => {
                assert_expr_subquery_key_types_match(expr);
                for item in list {
                    assert_expr_subquery_key_types_match(item);
                }
            }
            PlanExpr::CaseExpr {
                operand,
                when_clauses,
                else_result,
                ..
            } => {
                if let Some(operand) = operand {
                    assert_expr_subquery_key_types_match(operand);
                }
                for (condition, result) in when_clauses {
                    assert_expr_subquery_key_types_match(condition);
                    assert_expr_subquery_key_types_match(result);
                }
                if let Some(else_result) = else_result {
                    assert_expr_subquery_key_types_match(else_result);
                }
            }
            PlanExpr::Column { .. }
            | PlanExpr::Literal { .. }
            | PlanExpr::Wildcard
            | PlanExpr::Parameter { .. } => {}
        }
    }

    #[test]
    fn narrow_keys_gate_test_override_controls_enabled_state() {
        let _guard = set_narrow_keys_for_test(true);
        assert!(narrow_keys_enabled());
    }

    #[test]
    fn narrow_keys_fitting_non_output_int64_gets_cast_to_int32() {
        let input = scan(
            "lineitem",
            vec![
                ci("l_orderkey", DataType::Int64),
                ci("payload", DataType::Int64),
            ],
        );
        let plan = project(input, 1, "payload");
        let out = analyze(
            plan,
            stats("lineitem", vec![("l_orderkey", Some(1), Some(100))]),
        );

        let LogicalPlan::Projection { input, .. } = out else {
            panic!("expected root projection");
        };
        let LogicalPlan::Projection { exprs, schema, .. } = *input else {
            panic!("expected narrowing projection above scan");
        };
        assert!(matches!(
            exprs[0],
            PlanExpr::Cast {
                data_type: DataType::Int32,
                ..
            }
        ));
        assert_eq!(schema[0].data_type, DataType::Int32);
    }

    #[test]
    fn narrow_keys_overflowing_int64_stats_do_not_cast() {
        let input = scan(
            "orders",
            vec![
                ci("o_orderkey", DataType::Int64),
                ci("payload", DataType::Int64),
            ],
        );
        let plan = project(input, 1, "payload");
        let out = analyze(
            plan,
            stats(
                "orders",
                vec![("o_orderkey", Some(1), Some(i32::MAX as i64 + 1))],
            ),
        );

        let LogicalPlan::Projection { input, .. } = out else {
            panic!("expected root projection");
        };
        assert!(matches!(*input, LogicalPlan::TableScan { .. }));
    }

    #[test]
    fn narrow_keys_root_output_column_is_not_cast() {
        let input = scan("orders", vec![ci("o_orderkey", DataType::Int64)]);
        let plan = project(input, 0, "o_orderkey");
        let out = analyze(
            plan,
            stats("orders", vec![("o_orderkey", Some(1), Some(100))]),
        );

        let LogicalPlan::Projection { input, .. } = out else {
            panic!("expected root projection");
        };
        assert!(matches!(*input, LogicalPlan::TableScan { .. }));
    }

    #[test]
    fn narrow_keys_root_output_alias_is_not_cast() {
        let input = scan("orders", vec![ci("o_orderkey", DataType::Int64)]);
        let plan = project(input, 0, "order_key_alias");
        let out = analyze(
            plan,
            stats("orders", vec![("o_orderkey", Some(1), Some(100))]),
        );

        let LogicalPlan::Projection { input, schema, .. } = out else {
            panic!("expected root projection");
        };
        assert_eq!(schema[0].data_type, DataType::Int64);
        assert!(matches!(*input, LogicalPlan::TableScan { .. }));
    }

    #[test]
    fn narrow_keys_join_pair_requires_both_sides_int32_safe() {
        let left = scan(
            "lineitem",
            vec![
                ci("l_orderkey", DataType::Int64),
                ci("l_payload", DataType::Int64),
            ],
        );
        let right = scan(
            "orders",
            vec![
                ci("o_orderkey", DataType::Int64),
                ci("o_payload", DataType::Int64),
            ],
        );
        let join = LogicalPlan::Join {
            left: Box::new(left),
            right: Box::new(right),
            join_type: ast::JoinType::Inner,
            condition: JoinCondition::On(PlanExpr::BinaryOp {
                left: Box::new(col(0, "l_orderkey")),
                op: ast::BinaryOp::Eq,
                right: Box::new(col(2, "o_orderkey")),
                span: None,
            }),
            dynamic_filter_ids: Vec::new(),
        };
        let plan = project(join, 1, "l_payload");
        let out = analyze(
            plan,
            stats("lineitem", vec![("l_orderkey", Some(1), Some(100))]),
        );

        let LogicalPlan::Projection { input, .. } = out else {
            panic!("expected root projection");
        };
        let LogicalPlan::Join { left, right, .. } = *input else {
            panic!("expected join below root projection");
        };
        assert!(matches!(*left, LogicalPlan::TableScan { .. }));
        assert!(matches!(*right, LogicalPlan::TableScan { .. }));
    }

    #[test]
    fn narrow_keys_inner_join_internal_key_narrows_both_sides() {
        let left = scan(
            "part",
            vec![
                ci("p_partkey", DataType::Int64),
                ci("p_brand", DataType::Utf8),
            ],
        );
        let right = scan(
            "partsupp",
            vec![
                ci("ps_partkey", DataType::Int64),
                ci("ps_suppkey", DataType::Int64),
            ],
        );
        let join = LogicalPlan::Join {
            left: Box::new(left),
            right: Box::new(right),
            join_type: ast::JoinType::Inner,
            condition: JoinCondition::On(PlanExpr::BinaryOp {
                left: Box::new(col(0, "p_partkey")),
                op: ast::BinaryOp::Eq,
                right: Box::new(col(2, "ps_partkey")),
                span: None,
            }),
            dynamic_filter_ids: Vec::new(),
        };
        let plan = project_many(
            join,
            vec![col(1, "p_brand")],
            vec![ci("p_brand", DataType::Utf8)],
        );

        let out = analyze(
            plan,
            stats_many(vec![
                (
                    "part",
                    vec![("p_partkey", Some(1), Some(100)), ("p_brand", None, None)],
                ),
                ("partsupp", vec![("ps_partkey", Some(1), Some(100))]),
            ]),
        );

        let LogicalPlan::Projection { input, .. } = out else {
            panic!("expected root projection");
        };
        let LogicalPlan::Join { left, right, .. } = *input else {
            panic!("expected join below root projection");
        };
        assert_projection_col_type(&left, 0, DataType::Int32);
        assert_projection_col_type(&right, 0, DataType::Int32);
        assert_eq!(left.schema()[0].data_type, right.schema()[0].data_type);
        assert_eq!(left.schema()[0].data_type, DataType::Int32);
        assert_schema_lengths_consistent(&LogicalPlan::Join {
            left,
            right,
            join_type: ast::JoinType::Inner,
            condition: JoinCondition::None,
            dynamic_filter_ids: Vec::new(),
        });
    }

    #[test]
    fn narrow_keys_scan_projection_preserves_column_ordinals_and_names() {
        let input = scan(
            "lineitem",
            vec![
                ci("l_partkey", DataType::Int64),
                ci("l_quantity", DataType::Int64),
                ci("l_comment", DataType::Utf8),
            ],
        );
        let plan = project_many(
            input,
            vec![col(2, "l_comment")],
            vec![ci("l_comment", DataType::Utf8)],
        );

        let out = analyze(
            plan,
            stats_many(vec![(
                "lineitem",
                vec![
                    ("l_partkey", Some(1), Some(100)),
                    ("l_quantity", Some(1), Some(50)),
                ],
            )]),
        );

        let LogicalPlan::Projection { input, .. } = out else {
            panic!("expected root projection");
        };
        assert_schema_names(&input, &["l_partkey", "l_quantity", "l_comment"]);
        assert_eq!(input.schema()[0].data_type, DataType::Int32);
        assert_eq!(input.schema()[1].data_type, DataType::Int32);
        assert_eq!(input.schema()[2].data_type, DataType::Utf8);
        let LogicalPlan::Projection { exprs, .. } = input.as_ref() else {
            panic!("expected narrowing projection");
        };
        assert_eq!(exprs.len(), 3);
        for (expected, expr) in exprs.iter().enumerate() {
            let index = single_column_index(expr).expect("narrow output must trace to one column");
            assert_eq!(index, expected);
        }
    }

    #[test]
    fn narrow_keys_inner_join_with_non_equi_residual_blocks_keys() {
        let lineitem = scan(
            "lineitem",
            vec![
                ci("l_partkey", DataType::Int64),
                ci("l_quantity", DataType::Int64),
                ci("l_payload", DataType::Utf8),
            ],
        );
        let part = scan("part", vec![ci("p_partkey", DataType::Int64)]);
        let join = LogicalPlan::Join {
            left: Box::new(lineitem),
            right: Box::new(part),
            join_type: ast::JoinType::Inner,
            condition: JoinCondition::On(PlanExpr::BinaryOp {
                left: Box::new(PlanExpr::BinaryOp {
                    left: Box::new(col(0, "l_partkey")),
                    op: ast::BinaryOp::Eq,
                    right: Box::new(col(3, "p_partkey")),
                    span: None,
                }),
                op: ast::BinaryOp::And,
                right: Box::new(PlanExpr::BinaryOp {
                    left: Box::new(col(1, "l_quantity")),
                    op: ast::BinaryOp::Lt,
                    right: Box::new(lit_i64(30)),
                    span: None,
                }),
                span: None,
            }),
            dynamic_filter_ids: Vec::new(),
        };
        let plan = project_many(
            join,
            vec![col(2, "l_payload")],
            vec![ci("l_payload", DataType::Utf8)],
        );

        let out = analyze(
            plan,
            stats_many(vec![
                (
                    "lineitem",
                    vec![
                        ("l_partkey", Some(1), Some(100)),
                        ("l_quantity", Some(1), Some(50)),
                    ],
                ),
                ("part", vec![("p_partkey", Some(1), Some(100))]),
            ]),
        );

        let LogicalPlan::Projection { input, .. } = out else {
            panic!("expected root projection");
        };
        let LogicalPlan::Join { left, right, .. } = input.as_ref() else {
            panic!("expected join below root projection");
        };
        assert!(matches!(left.as_ref(), LogicalPlan::TableScan { .. }));
        assert!(matches!(right.as_ref(), LogicalPlan::TableScan { .. }));
        assert_eq!(left.schema()[0].data_type, DataType::Int64);
        assert_eq!(right.schema()[0].data_type, DataType::Int64);
    }

    #[test]
    fn narrow_keys_same_table_blocked_join_key_blocks_peer_join_keys() {
        let orders = scan(
            "orders",
            vec![
                ci("o_orderkey", DataType::Int64),
                ci("o_custkey", DataType::Int64),
            ],
        );
        let customer = scan("customer", vec![ci("c_custkey", DataType::Int64)]);
        let lineitem = scan("lineitem", vec![ci("l_orderkey", DataType::Int64)]);
        let orders_customer = LogicalPlan::Join {
            left: Box::new(orders),
            right: Box::new(customer),
            join_type: ast::JoinType::Inner,
            condition: JoinCondition::On(PlanExpr::BinaryOp {
                left: Box::new(col(1, "o_custkey")),
                op: ast::BinaryOp::Eq,
                right: Box::new(col(2, "c_custkey")),
                span: None,
            }),
            dynamic_filter_ids: Vec::new(),
        };
        let join = LogicalPlan::Join {
            left: Box::new(orders_customer),
            right: Box::new(lineitem),
            join_type: ast::JoinType::Inner,
            condition: JoinCondition::On(PlanExpr::BinaryOp {
                left: Box::new(col(0, "o_orderkey")),
                op: ast::BinaryOp::Eq,
                right: Box::new(col(3, "l_orderkey")),
                span: None,
            }),
            dynamic_filter_ids: Vec::new(),
        };
        let plan = project_many(
            join,
            vec![col(2, "c_custkey")],
            vec![ci("c_custkey", DataType::Int64)],
        );

        let out = analyze(
            plan,
            stats_many(vec![
                (
                    "orders",
                    vec![
                        ("o_orderkey", Some(1), Some(100)),
                        ("o_custkey", Some(1), Some(100)),
                    ],
                ),
                ("customer", vec![("c_custkey", Some(1), Some(100))]),
                ("lineitem", vec![("l_orderkey", Some(1), Some(100))]),
            ]),
        );

        let LogicalPlan::Projection { input, .. } = out else {
            panic!("expected root projection");
        };
        let LogicalPlan::Join { left, .. } = input.as_ref() else {
            panic!("expected top join below root projection");
        };
        let LogicalPlan::Join {
            left: orders,
            right: customer,
            ..
        } = left.as_ref()
        else {
            panic!("expected orders/customer join");
        };
        assert!(matches!(orders.as_ref(), LogicalPlan::TableScan { .. }));
        assert!(matches!(customer.as_ref(), LogicalPlan::TableScan { .. }));
        assert_eq!(orders.schema()[0].data_type, DataType::Int64);
        assert_eq!(orders.schema()[1].data_type, DataType::Int64);
    }

    #[test]
    fn narrow_keys_pure_equi_multi_hop_join_keys_still_narrow() {
        let customer = scan(
            "customer",
            vec![
                ci("c_custkey", DataType::Int64),
                ci("c_nationkey", DataType::Int64),
                ci("c_name", DataType::Utf8),
            ],
        );
        let orders = scan("orders", vec![ci("o_custkey", DataType::Int64)]);
        let nation = scan("nation", vec![ci("n_nationkey", DataType::Int64)]);
        let customer_orders = LogicalPlan::Join {
            left: Box::new(customer),
            right: Box::new(orders),
            join_type: ast::JoinType::Inner,
            condition: JoinCondition::On(PlanExpr::BinaryOp {
                left: Box::new(col(0, "c_custkey")),
                op: ast::BinaryOp::Eq,
                right: Box::new(col(3, "o_custkey")),
                span: None,
            }),
            dynamic_filter_ids: Vec::new(),
        };
        let join = LogicalPlan::Join {
            left: Box::new(customer_orders),
            right: Box::new(nation),
            join_type: ast::JoinType::Inner,
            condition: JoinCondition::On(PlanExpr::BinaryOp {
                left: Box::new(col(1, "c_nationkey")),
                op: ast::BinaryOp::Eq,
                right: Box::new(col(4, "n_nationkey")),
                span: None,
            }),
            dynamic_filter_ids: Vec::new(),
        };
        let plan = project_many(
            join,
            vec![col(2, "c_name")],
            vec![ci("c_name", DataType::Utf8)],
        );

        let out = analyze(
            plan,
            stats_many(vec![
                (
                    "customer",
                    vec![
                        ("c_custkey", Some(1), Some(100)),
                        ("c_nationkey", Some(1), Some(100)),
                    ],
                ),
                ("orders", vec![("o_custkey", Some(1), Some(100))]),
                ("nation", vec![("n_nationkey", Some(1), Some(100))]),
            ]),
        );

        let LogicalPlan::Projection { input, .. } = out else {
            panic!("expected root projection");
        };
        let LogicalPlan::Join {
            left: customer_orders,
            right: nation,
            ..
        } = input.as_ref()
        else {
            panic!("expected top join below root projection");
        };
        let LogicalPlan::Join {
            left: customer,
            right: orders,
            ..
        } = customer_orders.as_ref()
        else {
            panic!("expected customer/orders join");
        };
        assert_projection_col_type(customer, 0, DataType::Int32);
        assert_projection_col_type(customer, 1, DataType::Int32);
        assert_projection_col_type(orders, 0, DataType::Int32);
        assert_projection_col_type(nation, 0, DataType::Int32);
    }

    #[test]
    fn narrow_keys_inner_join_equivalence_blocks_peer_poisoned_by_anti_join() {
        let part = scan(
            "part",
            vec![
                ci("p_partkey", DataType::Int64),
                ci("p_brand", DataType::Utf8),
            ],
        );
        let partsupp = scan(
            "partsupp",
            vec![
                ci("ps_partkey", DataType::Int64),
                ci("ps_suppkey", DataType::Int64),
            ],
        );
        let supplier = scan("supplier", vec![ci("s_suppkey", DataType::Int64)]);
        let partsupp_without_supplier = LogicalPlan::AntiJoin {
            left: Box::new(partsupp),
            right: Box::new(supplier),
            left_key: col(1, "ps_suppkey"),
            right_key: col(0, "s_suppkey"),
            residual: None,
        };
        let join = LogicalPlan::Join {
            left: Box::new(part),
            right: Box::new(partsupp_without_supplier),
            join_type: ast::JoinType::Inner,
            condition: JoinCondition::On(PlanExpr::BinaryOp {
                left: Box::new(col(0, "p_partkey")),
                op: ast::BinaryOp::Eq,
                right: Box::new(col(2, "ps_partkey")),
                span: None,
            }),
            dynamic_filter_ids: Vec::new(),
        };
        let plan = project_many(
            join,
            vec![col(1, "p_brand")],
            vec![ci("p_brand", DataType::Utf8)],
        );

        let out = analyze(
            plan,
            stats_many(vec![
                (
                    "part",
                    vec![("p_partkey", Some(1), Some(100)), ("p_brand", None, None)],
                ),
                (
                    "partsupp",
                    vec![
                        ("ps_partkey", Some(1), Some(100)),
                        ("ps_suppkey", Some(1), Some(100)),
                    ],
                ),
                ("supplier", vec![("s_suppkey", Some(1), Some(100))]),
            ]),
        );

        let LogicalPlan::Projection { input, .. } = out else {
            panic!("expected root projection");
        };
        let LogicalPlan::Join { left, right, .. } = *input else {
            panic!("expected join below root projection");
        };
        assert!(matches!(*left, LogicalPlan::TableScan { .. }));
        assert_eq!(left.schema()[0].data_type, DataType::Int64);
        let LogicalPlan::AntiJoin {
            left: anti_left,
            right: anti_right,
            ..
        } = right.as_ref()
        else {
            panic!("expected anti join on right side");
        };
        assert!(matches!(anti_left.as_ref(), LogicalPlan::TableScan { .. }));
        assert!(matches!(anti_right.as_ref(), LogicalPlan::TableScan { .. }));
        assert_eq!(anti_left.schema()[0].data_type, DataType::Int64);
    }

    #[test]
    fn narrow_keys_inner_join_equivalence_without_blocked_member_narrows_class() {
        let left = scan(
            "left_table",
            vec![
                ci("a_key", DataType::Int64),
                ci("a_payload", DataType::Utf8),
            ],
        );
        let right = scan("right_table", vec![ci("b_key", DataType::Int64)]);
        let join = LogicalPlan::Join {
            left: Box::new(left),
            right: Box::new(right),
            join_type: ast::JoinType::Inner,
            condition: JoinCondition::On(PlanExpr::BinaryOp {
                left: Box::new(col(0, "a_key")),
                op: ast::BinaryOp::Eq,
                right: Box::new(col(2, "b_key")),
                span: None,
            }),
            dynamic_filter_ids: Vec::new(),
        };
        let plan = project_many(
            join,
            vec![col(1, "a_payload")],
            vec![ci("a_payload", DataType::Utf8)],
        );

        let out = analyze(
            plan,
            stats_many(vec![
                ("left_table", vec![("a_key", Some(1), Some(100))]),
                ("right_table", vec![("b_key", Some(1), Some(100))]),
            ]),
        );

        let LogicalPlan::Projection { input, .. } = out else {
            panic!("expected root projection");
        };
        let LogicalPlan::Join { left, right, .. } = *input else {
            panic!("expected join below root projection");
        };
        assert_projection_col_type(&left, 0, DataType::Int32);
        assert_projection_col_type(&right, 0, DataType::Int32);
        assert_eq!(left.schema()[0].data_type, right.schema()[0].data_type);
    }

    #[test]
    fn narrow_keys_semi_join_internal_key_is_blocked() {
        let left = scan(
            "lineitem",
            vec![
                ci("l_orderkey", DataType::Int64),
                ci("l_suppkey", DataType::Int64),
            ],
        );
        let right = scan("orders", vec![ci("o_orderkey", DataType::Int64)]);
        let semi = LogicalPlan::SemiJoin {
            left: Box::new(left),
            right: Box::new(right),
            left_key: col(0, "l_orderkey"),
            right_key: col(0, "o_orderkey"),
            residual: None,
            dynamic_filter_ids: Vec::new(),
        };
        let plan = project(semi, 1, "l_suppkey");

        let out = analyze(
            plan,
            stats_many(vec![
                (
                    "lineitem",
                    vec![
                        ("l_orderkey", Some(1), Some(100)),
                        ("l_suppkey", Some(1), Some(100)),
                    ],
                ),
                ("orders", vec![("o_orderkey", Some(1), Some(100))]),
            ]),
        );

        let LogicalPlan::Projection { input, .. } = out else {
            panic!("expected root projection");
        };
        let LogicalPlan::SemiJoin { left, right, .. } = input.as_ref() else {
            panic!("expected semi join below root projection");
        };
        assert!(matches!(left.as_ref(), LogicalPlan::TableScan { .. }));
        assert!(matches!(right.as_ref(), LogicalPlan::TableScan { .. }));
        assert_eq!(left.schema()[0].data_type, DataType::Int64);
        assert_eq!(right.schema()[0].data_type, DataType::Int64);
        assert_eq!(left.schema()[0].data_type, right.schema()[0].data_type);
        assert_eq!(input.schema(), left.schema());
        assert_schema_lengths_consistent(&input);
    }

    #[test]
    fn narrow_keys_anti_join_internal_key_is_blocked() {
        let left = scan(
            "lineitem",
            vec![
                ci("l_orderkey", DataType::Int64),
                ci("l_comment", DataType::Utf8),
            ],
        );
        let right = scan("orders", vec![ci("o_orderkey", DataType::Int64)]);
        let anti = LogicalPlan::AntiJoin {
            left: Box::new(left),
            right: Box::new(right),
            left_key: col(0, "l_orderkey"),
            right_key: col(0, "o_orderkey"),
            residual: None,
        };
        let plan = project_many(
            anti,
            vec![col(1, "l_comment")],
            vec![ci("l_comment", DataType::Utf8)],
        );

        let out = analyze(
            plan,
            stats_many(vec![
                (
                    "lineitem",
                    vec![
                        ("l_orderkey", Some(1), Some(100)),
                        ("l_comment", None, None),
                    ],
                ),
                ("orders", vec![("o_orderkey", Some(1), Some(100))]),
            ]),
        );

        let LogicalPlan::Projection { input, .. } = out else {
            panic!("expected root projection");
        };
        let LogicalPlan::AntiJoin { left, right, .. } = input.as_ref() else {
            panic!("expected anti join below root projection");
        };
        assert!(matches!(left.as_ref(), LogicalPlan::TableScan { .. }));
        assert!(matches!(right.as_ref(), LogicalPlan::TableScan { .. }));
        assert_eq!(left.schema()[0].data_type, DataType::Int64);
        assert_eq!(right.schema()[0].data_type, DataType::Int64);
        assert_eq!(input.schema(), left.schema());
        assert_schema_lengths_consistent(&input);
    }

    #[test]
    fn narrow_keys_aggregate_schema_refreshes_from_narrowed_child() {
        let left = scan(
            "part",
            vec![
                ci("p_partkey", DataType::Int64),
                ci("p_brand", DataType::Utf8),
            ],
        );
        let right = scan(
            "partsupp",
            vec![
                ci("ps_partkey", DataType::Int64),
                ci("ps_suppkey", DataType::Int64),
            ],
        );
        let join = LogicalPlan::Join {
            left: Box::new(left),
            right: Box::new(right),
            join_type: ast::JoinType::Inner,
            condition: JoinCondition::On(PlanExpr::BinaryOp {
                left: Box::new(col(0, "p_partkey")),
                op: ast::BinaryOp::Eq,
                right: Box::new(col(2, "ps_partkey")),
                span: None,
            }),
            dynamic_filter_ids: Vec::new(),
        };
        let aggregate = LogicalPlan::Aggregate {
            input: Box::new(join),
            group_by: vec![col(1, "p_brand"), col(0, "p_partkey")],
            aggr_exprs: Vec::new(),
            schema: vec![
                ci("p_brand", DataType::Utf8),
                ci("p_partkey", DataType::Int64),
            ],
        };
        let plan = project_many(
            aggregate,
            vec![col(0, "p_brand")],
            vec![ci("p_brand", DataType::Utf8)],
        );

        let out = analyze(
            plan,
            stats_many(vec![
                (
                    "part",
                    vec![("p_partkey", Some(1), Some(100)), ("p_brand", None, None)],
                ),
                ("partsupp", vec![("ps_partkey", Some(1), Some(100))]),
            ]),
        );

        let LogicalPlan::Projection { input, .. } = out else {
            panic!("expected root projection");
        };
        let LogicalPlan::Aggregate {
            input: aggregate_input,
            schema,
            ..
        } = *input
        else {
            panic!("expected aggregate below root projection");
        };
        assert_eq!(schema[1].data_type, DataType::Int32);
        assert_eq!(aggregate_input.schema()[0].data_type, DataType::Int32);
    }

    #[test]
    fn narrow_keys_join_key_through_aggregate_narrows_both_sides() {
        let left = LogicalPlan::Aggregate {
            input: Box::new(scan("lineitem", vec![ci("l_orderkey", DataType::Int64)])),
            group_by: vec![col(0, "l_orderkey")],
            aggr_exprs: Vec::new(),
            schema: vec![ci("l_orderkey", DataType::Int64)],
        };
        let right = scan("orders", vec![ci("o_orderkey", DataType::Int64)]);
        let join = LogicalPlan::Join {
            left: Box::new(left),
            right: Box::new(right),
            join_type: ast::JoinType::Inner,
            condition: JoinCondition::On(PlanExpr::BinaryOp {
                left: Box::new(col(0, "l_orderkey")),
                op: ast::BinaryOp::Eq,
                right: Box::new(col(1, "o_orderkey")),
                span: None,
            }),
            dynamic_filter_ids: Vec::new(),
        };
        let plan = project_many(join, Vec::new(), Vec::new());

        let out = analyze(
            plan,
            stats_many(vec![
                ("lineitem", vec![("l_orderkey", Some(1), Some(100))]),
                ("orders", vec![("o_orderkey", Some(1), Some(100))]),
            ]),
        );

        let LogicalPlan::Projection { input, .. } = out else {
            panic!("expected root projection");
        };
        let LogicalPlan::Join { left, right, .. } = *input else {
            panic!("expected join below root projection");
        };
        assert_eq!(left.schema()[0].data_type, right.schema()[0].data_type);
        assert_eq!(left.schema()[0].data_type, DataType::Int32);
    }

    #[test]
    fn narrow_keys_q19_shape_preserves_arithmetic_aggregate_indices_and_type() {
        let lineitem = scan(
            "lineitem",
            vec![
                ci("l_partkey", DataType::Int64),
                ci("l_extendedprice", DataType::Float64),
                ci("l_discount", DataType::Float64),
            ],
        );
        let part = scan(
            "part",
            vec![
                ci("p_partkey", DataType::Int64),
                ci("p_brand", DataType::Utf8),
            ],
        );
        let join = LogicalPlan::Join {
            left: Box::new(lineitem),
            right: Box::new(part),
            join_type: ast::JoinType::Inner,
            condition: JoinCondition::On(PlanExpr::BinaryOp {
                left: Box::new(col(0, "l_partkey")),
                op: ast::BinaryOp::Eq,
                right: Box::new(col(3, "p_partkey")),
                span: None,
            }),
            dynamic_filter_ids: Vec::new(),
        };
        let revenue_arg = PlanExpr::BinaryOp {
            left: Box::new(col(1, "l_extendedprice")),
            op: ast::BinaryOp::Multiply,
            right: Box::new(PlanExpr::BinaryOp {
                left: Box::new(lit_i64(1)),
                op: ast::BinaryOp::Minus,
                right: Box::new(col(2, "l_discount")),
                span: None,
            }),
            span: None,
        };
        let aggregate = LogicalPlan::Aggregate {
            input: Box::new(join),
            group_by: Vec::new(),
            aggr_exprs: vec![PlanExpr::Function {
                name: "SUM".to_string(),
                args: vec![revenue_arg],
                distinct: false,
                span: None,
            }],
            schema: vec![ci("revenue", DataType::Null)],
        };
        let plan = project_many(
            aggregate,
            vec![col(0, "revenue")],
            vec![ci("revenue", DataType::Null)],
        );

        let out = analyze(
            plan,
            stats_many(vec![
                ("lineitem", vec![("l_partkey", Some(1), Some(200_000))]),
                ("part", vec![("p_partkey", Some(1), Some(200_000))]),
            ]),
        );

        assert_eq!(out.schema()[0].data_type, DataType::Float64);
        let LogicalPlan::Projection { input, exprs, .. } = out else {
            panic!("expected root projection");
        };
        assert!(matches!(exprs[0], PlanExpr::Column { index: 0, .. }));
        let LogicalPlan::Aggregate {
            input,
            aggr_exprs,
            schema,
            ..
        } = input.as_ref()
        else {
            panic!("expected aggregate below root projection");
        };
        assert_eq!(schema[0].data_type, DataType::Float64);

        let LogicalPlan::Join {
            left,
            right,
            condition,
            ..
        } = input.as_ref()
        else {
            panic!("expected join below aggregate");
        };
        assert_projection_col_type(left, 0, DataType::Int32);
        assert_projection_col_type(right, 0, DataType::Int32);
        assert_eq!(left.schema()[1].data_type, DataType::Float64);
        assert_eq!(left.schema()[2].data_type, DataType::Float64);
        if let JoinCondition::On(PlanExpr::BinaryOp { left, right, .. }) = condition {
            assert!(matches!(left.as_ref(), PlanExpr::Column { index: 0, .. }));
            assert!(matches!(right.as_ref(), PlanExpr::Column { index: 3, .. }));
        } else {
            panic!("expected equi-join condition");
        }

        let PlanExpr::Function { args, .. } = &aggr_exprs[0] else {
            panic!("expected SUM aggregate");
        };
        let PlanExpr::BinaryOp {
            left,
            right,
            op: ast::BinaryOp::Multiply,
            ..
        } = &args[0]
        else {
            panic!("expected revenue multiplication");
        };
        assert!(matches!(
            left.as_ref(),
            PlanExpr::Column {
                index: 1,
                name,
                ..
            } if name == "l_extendedprice"
        ));
        let PlanExpr::BinaryOp {
            right,
            op: ast::BinaryOp::Minus,
            ..
        } = right.as_ref()
        else {
            panic!("expected discount subtraction");
        };
        assert!(matches!(
            right.as_ref(),
            PlanExpr::Column {
                index: 2,
                name,
                ..
            } if name == "l_discount"
        ));
        assert_eq!(
            refreshed_expr_type(&aggr_exprs[0], &input.schema()),
            Some(DataType::Float64)
        );
        assert_schema_lengths_consistent(input);
    }

    #[test]
    fn narrow_keys_q19_shape_post_join_filter_keeps_other_column_indices() {
        let lineitem = scan(
            "lineitem",
            vec![
                ci("l_partkey", DataType::Int64),
                ci("l_quantity", DataType::Int64),
                ci("l_shipmode", DataType::Utf8),
            ],
        );
        let part = scan(
            "part",
            vec![
                ci("p_partkey", DataType::Int64),
                ci("p_brand", DataType::Utf8),
            ],
        );
        let join = LogicalPlan::Join {
            left: Box::new(lineitem),
            right: Box::new(part),
            join_type: ast::JoinType::Inner,
            condition: JoinCondition::On(PlanExpr::BinaryOp {
                left: Box::new(col(0, "l_partkey")),
                op: ast::BinaryOp::Eq,
                right: Box::new(col(3, "p_partkey")),
                span: None,
            }),
            dynamic_filter_ids: Vec::new(),
        };
        let filter = LogicalPlan::Filter {
            input: Box::new(join),
            predicate: PlanExpr::BinaryOp {
                left: Box::new(PlanExpr::BinaryOp {
                    left: Box::new(col(1, "l_quantity")),
                    op: ast::BinaryOp::Lt,
                    right: Box::new(lit_i64(30)),
                    span: None,
                }),
                op: ast::BinaryOp::And,
                right: Box::new(PlanExpr::BinaryOp {
                    left: Box::new(col(4, "p_brand")),
                    op: ast::BinaryOp::Eq,
                    right: Box::new(lit_utf8("Brand#12")),
                    span: None,
                }),
                span: None,
            },
        };
        let plan = project_many(
            filter,
            vec![col(2, "l_shipmode")],
            vec![ci("l_shipmode", DataType::Utf8)],
        );

        let out = analyze(
            plan,
            stats_many(vec![
                (
                    "lineitem",
                    vec![
                        ("l_partkey", Some(1), Some(200_000)),
                        ("l_quantity", Some(1), Some(50)),
                    ],
                ),
                ("part", vec![("p_partkey", Some(1), Some(200_000))]),
            ]),
        );

        let LogicalPlan::Projection { input, .. } = out else {
            panic!("expected root projection");
        };
        let LogicalPlan::Filter { input, predicate } = input.as_ref() else {
            panic!("expected post-join filter");
        };
        let LogicalPlan::Join { left, right, .. } = input.as_ref() else {
            panic!("expected join below filter");
        };
        assert_schema_names(left, &["l_partkey", "l_quantity", "l_shipmode"]);
        assert_schema_names(right, &["p_partkey", "p_brand"]);
        assert_eq!(left.schema()[0].data_type, DataType::Int32);
        assert_eq!(left.schema()[1].data_type, DataType::Int32);
        assert_eq!(right.schema()[0].data_type, DataType::Int32);

        let PlanExpr::BinaryOp {
            left: quantity_predicate,
            op: ast::BinaryOp::And,
            right: brand_predicate,
            ..
        } = predicate
        else {
            panic!("expected conjunctive filter predicate");
        };
        let PlanExpr::BinaryOp {
            left,
            op: ast::BinaryOp::Lt,
            ..
        } = quantity_predicate.as_ref()
        else {
            panic!("expected l_quantity predicate");
        };
        let PlanExpr::Column { index, name, .. } = left.as_ref() else {
            panic!("expected l_quantity column");
        };
        assert_eq!((*index, name.as_str()), (1, "l_quantity"));
        assert_eq!(input.schema()[*index].name, "l_quantity");
        assert_eq!(input.schema()[*index].data_type, DataType::Int32);

        let PlanExpr::BinaryOp {
            left,
            op: ast::BinaryOp::Eq,
            ..
        } = brand_predicate.as_ref()
        else {
            panic!("expected p_brand predicate");
        };
        let PlanExpr::Column { index, name, .. } = left.as_ref() else {
            panic!("expected p_brand column");
        };
        assert_eq!((*index, name.as_str()), (4, "p_brand"));
        assert_eq!(input.schema()[*index].name, "p_brand");
        assert_eq!(input.schema()[*index].data_type, DataType::Utf8);
    }

    #[test]
    fn narrow_keys_same_table_nested_semi_anti_keys_narrow_consistently() {
        let l1 = scan(
            "lineitem",
            vec![
                ci("l_orderkey", DataType::Int64),
                ci("l_suppkey", DataType::Int64),
                ci("payload", DataType::Utf8),
            ],
        );
        let l2 = scan(
            "lineitem",
            vec![
                ci("l_orderkey", DataType::Int64),
                ci("l_suppkey", DataType::Int64),
            ],
        );
        let l3 = scan(
            "lineitem",
            vec![
                ci("l_orderkey", DataType::Int64),
                ci("l_suppkey", DataType::Int64),
            ],
        );
        let semi = LogicalPlan::SemiJoin {
            left: Box::new(l1),
            right: Box::new(l2),
            left_key: col(0, "l_orderkey"),
            right_key: col(0, "l_orderkey"),
            residual: Some(PlanExpr::BinaryOp {
                left: Box::new(col(1, "l_suppkey")),
                op: ast::BinaryOp::NotEq,
                right: Box::new(col(4, "l_suppkey")),
                span: None,
            }),
            dynamic_filter_ids: Vec::new(),
        };
        let anti = LogicalPlan::AntiJoin {
            left: Box::new(semi),
            right: Box::new(l3),
            left_key: col(0, "l_orderkey"),
            right_key: col(0, "l_orderkey"),
            residual: Some(PlanExpr::BinaryOp {
                left: Box::new(col(1, "l_suppkey")),
                op: ast::BinaryOp::NotEq,
                right: Box::new(col(4, "l_suppkey")),
                span: None,
            }),
        };
        let plan = project_many(
            anti,
            vec![col(2, "payload")],
            vec![ci("payload", DataType::Utf8)],
        );

        let out = analyze(
            plan,
            stats(
                "lineitem",
                vec![
                    ("l_orderkey", Some(1), Some(100)),
                    ("l_suppkey", Some(1), Some(100)),
                ],
            ),
        );

        assert_eq!(out.schema(), vec![ci("payload", DataType::Utf8)]);
        let LogicalPlan::Projection { input, .. } = out else {
            panic!("expected root projection");
        };
        assert_semi_anti_key_types_match(&input);
        let LogicalPlan::AntiJoin { left, right, .. } = input.as_ref() else {
            panic!("expected anti join below root projection");
        };
        assert_eq!(left.schema()[0].data_type, DataType::Int64);
        assert_eq!(right.schema()[0].data_type, DataType::Int64);
        assert_eq!(input.schema(), left.schema());
        assert_schema_lengths_consistent(&input);
    }

    #[test]
    fn narrow_keys_scalar_subquery_expression_sources_are_blocked() {
        let subquery = LogicalPlan::ScalarSubquery {
            subplan: Box::new(project(
                scan("lineitem", vec![ci("l_orderkey", DataType::Int64)]),
                0,
                "l_orderkey",
            )),
        };
        let plan = LogicalPlan::Projection {
            input: Box::new(LogicalPlan::OneRow),
            exprs: vec![PlanExpr::ScalarSubquery {
                subplan: Box::new(subquery),
                span: None,
            }],
            schema: vec![ci("scalar_key", DataType::Int64)],
        };

        let out = analyze(
            plan,
            stats("lineitem", vec![("l_orderkey", Some(1), Some(100))]),
        );

        assert_eq!(out.schema(), vec![ci("scalar_key", DataType::Int64)]);
        let LogicalPlan::Projection { exprs, .. } = out else {
            panic!("expected root projection");
        };
        let PlanExpr::ScalarSubquery { subplan, .. } = &exprs[0] else {
            panic!("expected scalar subquery expression");
        };
        let LogicalPlan::ScalarSubquery { subplan } = subplan.as_ref() else {
            panic!("expected logical scalar subquery");
        };
        assert_eq!(subplan.schema()[0].data_type, DataType::Int64);
        assert_schema_lengths_consistent(subplan);
    }
}
