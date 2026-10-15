//! `SemiJoinReduction` analyzer pass.
//!
//! Pre-filters the input of a decorrelated aggregate with the key set
//! that its outer side has already been reduced to, so the aggregate
//! never materializes groups that the outer join can never match.
//!
//! The TPC-H Q20 shape this is for (after scalar-subquery
//! decorrelation):
//!
//! ```text
//! Join: Left ON ps_partkey = __corr_key_0 AND ps_suppkey = __corr_key_1
//!   SemiJoin: ps_partkey = p_partkey            ← outer side L
//!     TableScan: partsupp
//!     Projection: p_partkey                      ← key set S (small)
//!       Filter: p_name LIKE 'forest%'
//!         TableScan: part
//!   Projection: l_partkey, l_suppkey, 0.5 * SUM(l_quantity)
//!     Aggregate: group_by=[l_partkey, l_suppkey], aggr=[SUM(l_quantity)]
//!       Filter: l_shipdate ...                  ← aggregate input R
//!         TableScan: lineitem
//! ```
//!
//! Every L row has `ps_partkey ∈ S`, and a LEFT (or INNER) join only
//! emits aggregate groups whose key equals some L row's key, so groups
//! with `l_partkey ∉ S` are dead work. Joining R to `DISTINCT(S)` on
//! `l_partkey = p_partkey` before aggregating keeps each surviving
//! group's rows intact (one match per row, no duplication) and drops
//! the rest — the same reduction Trino reaches at runtime through
//! dynamic filtering, done statically here.

use arneb_common::error::PlanError;
use arneb_sql_parser::ast::{BinaryOp, JoinType};

use crate::analyzer::{AnalysisPass, AnalyzerContext};
use crate::cost::{estimated_cardinality, CatalogStats};
use crate::plan::{JoinCondition, LogicalPlan, PlanExpr};

pub struct SemiJoinReduction;

impl SemiJoinReduction {
    pub fn new() -> Self {
        Self
    }
}

impl Default for SemiJoinReduction {
    fn default() -> Self {
        Self::new()
    }
}

impl AnalysisPass for SemiJoinReduction {
    fn name(&self) -> &'static str {
        "SemiJoinReduction"
    }

    fn analyze(
        &self,
        plan: LogicalPlan,
        ctx: &mut AnalyzerContext,
    ) -> Result<LogicalPlan, PlanError> {
        if ctx.catalog_stats.is_empty() {
            return Ok(plan);
        }
        Ok(rewrite(plan, &ctx.catalog_stats))
    }
}

fn rewrite(plan: LogicalPlan, stats: &CatalogStats) -> LogicalPlan {
    let mut plan = recurse_children(plan, &mut |child| rewrite(child, stats));
    if let LogicalPlan::Join {
        left,
        right,
        join_type: JoinType::Left | JoinType::Inner,
        condition: JoinCondition::On(on),
        ..
    } = &mut plan
    {
        let width = left.schema().len();
        for (outer, inner) in equality_columns(on, width) {
            if let Some((keys, key_index)) = restricted_keys(left, outer) {
                if reduce_aggregate(right, inner, keys, key_index, stats) {
                    break;
                }
            }
        }
    }
    plan
}

fn column_index(expr: &PlanExpr) -> Option<usize> {
    match expr {
        PlanExpr::Column { index, .. } => Some(*index),
        _ => None,
    }
}

// Only conjunctive equalities prove that every matching group is in the key set.
fn equality_columns(expr: &PlanExpr, width: usize) -> Vec<(usize, usize)> {
    let mut result = Vec::new();
    if let PlanExpr::BinaryOp {
        left, op, right, ..
    } = expr
    {
        if *op == BinaryOp::And {
            result.extend(equality_columns(left, width));
            result.extend(equality_columns(right, width));
        } else if *op == BinaryOp::Eq {
            if let (Some(a), Some(b)) = (column_index(left), column_index(right)) {
                if a < width && b >= width {
                    result.push((a, b - width));
                }
                if b < width && a >= width {
                    result.push((b, a - width));
                }
            }
        }
    }
    result
}

fn restricted_keys(plan: &LogicalPlan, index: usize) -> Option<(&LogicalPlan, usize)> {
    match plan {
        LogicalPlan::SemiJoin {
            left,
            right,
            left_key,
            right_key,
            ..
        } => {
            if column_index(left_key) == Some(index) {
                Some((right, column_index(right_key)?))
            } else {
                restricted_keys(left, index)
            }
        }
        LogicalPlan::Projection { input, exprs, .. } => {
            restricted_keys(input, column_index(exprs.get(index)?)?)
        }
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Distinct { input } => restricted_keys(input, index),
        _ => None,
    }
}

// Trace direct columns to a scan with a known row count. Without key NDV,
// use the unfiltered row count as a domain estimate for the cost heuristic.
fn key_ndv(plan: &LogicalPlan, index: usize, stats: &CatalogStats) -> Option<f64> {
    match plan {
        LogicalPlan::TableScan { table, schema, .. } => {
            let table_stats = stats.get(table)?;
            let row_count = table_stats.row_count?;
            let ndv = table_stats
                .columns
                .get(&schema.get(index)?.name)
                .and_then(|column| column.ndv)
                .unwrap_or(row_count);
            (ndv > 0).then_some(ndv as f64)
        }
        LogicalPlan::Projection { input, exprs, .. } => {
            key_ndv(input, column_index(exprs.get(index)?)?, stats)
        }
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Distinct { input } => key_ndv(input, index, stats),
        _ => None,
    }
}

// Re-evaluating a volatile filter or an unordered LIMIT could produce a
// different set on the two branches, invalidating the membership proof.
fn repeatable_keys(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::TableScan { .. } => true,
        LogicalPlan::Projection { input, exprs, .. } => {
            exprs.iter().all(|expr| column_index(expr).is_some()) && repeatable_keys(input)
        }
        LogicalPlan::Filter { input, predicate } => {
            repeatable_predicate(predicate) && repeatable_keys(input)
        }
        LogicalPlan::Sort { input, .. } | LogicalPlan::Distinct { input } => repeatable_keys(input),
        _ => false,
    }
}

fn repeatable_predicate(expr: &PlanExpr) -> bool {
    match expr {
        PlanExpr::Column { .. } | PlanExpr::Literal { .. } => true,
        PlanExpr::BinaryOp { left, right, .. } => {
            repeatable_predicate(left) && repeatable_predicate(right)
        }
        PlanExpr::UnaryOp { expr, .. }
        | PlanExpr::IsNull { expr, .. }
        | PlanExpr::IsNotNull { expr, .. } => repeatable_predicate(expr),
        _ => false,
    }
}

fn col(index: usize, plan: &LogicalPlan) -> PlanExpr {
    PlanExpr::Column {
        index,
        name: plan.schema()[index].name.clone(),
        span: None,
    }
}

fn reduce_aggregate(
    plan: &mut LogicalPlan,
    index: usize,
    keys: &LogicalPlan,
    key_index: usize,
    stats: &CatalogStats,
) -> bool {
    match plan {
        LogicalPlan::Projection { input, exprs, .. } => {
            let Some(index) = exprs.get(index).and_then(column_index) else {
                return false;
            };
            reduce_aggregate(input, index, keys, key_index, stats)
        }
        LogicalPlan::Filter { input, .. } | LogicalPlan::Sort { input, .. } => {
            reduce_aggregate(input, index, keys, key_index, stats)
        }
        LogicalPlan::Aggregate {
            input, group_by, ..
        } => {
            if !repeatable_keys(keys) {
                return false;
            }
            let Some(input_index) = group_by.get(index).and_then(column_index) else {
                return false;
            };
            let (Some(domain), Some(input_domain)) = (
                key_ndv(keys, key_index, stats),
                key_ndv(input, input_index, stats),
            ) else {
                return false;
            };
            let key_rows = estimated_cardinality(keys, stats);
            let input_rows = estimated_cardinality(input, stats);
            // Demand at least 2x key reduction and enough saved aggregate rows
            // to pay for scanning and deduplicating the extra key set.
            let fraction = (key_rows / domain.min(input_domain)).min(1.0);
            if fraction > 0.5 || input_rows * (1.0 - fraction) <= 2.0 * key_rows {
                return false;
            }
            let schema = input.schema();
            if schema.get(input_index).map(|c| &c.data_type)
                != keys.schema().get(key_index).map(|c| &c.data_type)
            {
                return false;
            }
            let key = col(key_index, keys);
            let distinct = LogicalPlan::Aggregate {
                input: Box::new(keys.clone()),
                group_by: vec![key],
                aggr_exprs: vec![],
                schema: vec![keys.schema()[key_index].clone()],
            };
            let on = PlanExpr::BinaryOp {
                left: Box::new(col(input_index, input)),
                op: BinaryOp::Eq,
                right: Box::new(PlanExpr::Column {
                    index: schema.len(),
                    name: distinct.schema()[0].name.clone(),
                    span: None,
                }),
                span: None,
            };
            let join = LogicalPlan::Join {
                left: input.clone(),
                right: Box::new(distinct),
                join_type: JoinType::Inner,
                condition: JoinCondition::On(on),
                dynamic_filter_ids: vec![],
            };
            // Restore the aggregate input's exact schema and column positions.
            let exprs = (0..schema.len()).map(|i| col(i, input)).collect();
            **input = LogicalPlan::Projection {
                input: Box::new(join),
                exprs,
                schema,
            };
            true
        }
        _ => false,
    }
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
            dynamic_filter_ids,
        } => L::Join {
            left: Box::new(f(*left)),
            right: Box::new(f(*right)),
            join_type,
            condition,
            dynamic_filter_ids,
        },
        L::SemiJoin {
            left,
            right,
            left_key,
            right_key,
            residual,
            dynamic_filter_ids,
            ..
        } => L::SemiJoin {
            left: Box::new(f(*left)),
            right: Box::new(f(*right)),
            left_key,
            right_key,
            residual,
            dynamic_filter_ids,
            probe_side_filter_ids: Vec::new(),
        },
        L::AntiJoin {
            left,
            right,
            left_key,
            right_key,
            residual,
            ..
        } => L::AntiJoin {
            left: Box::new(f(*left)),
            right: Box::new(f(*right)),
            left_key,
            right_key,
            residual,
            probe_side_filter_ids: Vec::new(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;

    use arneb_catalog::{
        CatalogManager, ColumnStatistics, MemoryCatalog, MemorySchema, MemoryTable, TableProvider,
        TableStatistics,
    };
    use arneb_common::types::{ColumnInfo, DataType, ScalarValue, TableReference};
    use arneb_sql_parser::ast::{self, BinaryOp};

    use crate::cost::CatalogStats;
    use crate::plan::{JoinCondition, PlanExpr};
    use crate::QueryPlanner;

    const Q20_SQL: &str = include_str!("../../../../benchmarks/tpch/queries/q20.sql");

    /// Q20 with the `p_name LIKE 'forest%'` filter removed: the key set
    /// is every part, so no reduction is possible.
    const Q20_UNFILTERED_PART_SQL: &str = "
SELECT s_name, s_address
FROM supplier
JOIN nation ON s_nationkey = n_nationkey
WHERE s_suppkey IN (
    SELECT ps_suppkey
    FROM partsupp
    WHERE ps_partkey IN (SELECT p_partkey FROM part)
        AND ps_availqty > (
            SELECT 0.5 * SUM(l_quantity)
            FROM lineitem
            WHERE l_partkey = ps_partkey
                AND l_suppkey = ps_suppkey
                AND l_shipdate >= DATE '1994-01-01'
                AND l_shipdate < DATE '1994-01-01' + INTERVAL '1' YEAR
        )
)
AND n_name = 'CANADA'
ORDER BY s_name";

    #[derive(Debug)]
    struct StatsTable {
        schema: Vec<ColumnInfo>,
        stats: TableStatistics,
    }

    impl TableProvider for StatsTable {
        fn schema(&self) -> Vec<ColumnInfo> {
            self.schema.clone()
        }

        fn statistics(&self) -> Option<TableStatistics> {
            Some(self.stats.clone())
        }
    }

    fn ci(name: &str, data_type: DataType) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            data_type,
            nullable: false,
        }
    }

    fn stats_for(row_count: u64, column_ndvs: &[(&str, u64)]) -> TableStatistics {
        let mut columns = HashMap::new();
        for (col, ndv) in column_ndvs {
            columns.insert(
                col.to_string(),
                ColumnStatistics {
                    ndv: Some(*ndv),
                    ..ColumnStatistics::default()
                },
            );
        }
        TableStatistics {
            row_count: Some(row_count),
            size_bytes: None,
            columns,
        }
    }

    fn supplier_schema() -> Vec<ColumnInfo> {
        vec![
            ci("s_suppkey", DataType::Int64),
            ci("s_name", DataType::Utf8),
            ci("s_address", DataType::Utf8),
            ci("s_nationkey", DataType::Int64),
        ]
    }

    fn nation_schema() -> Vec<ColumnInfo> {
        vec![
            ci("n_nationkey", DataType::Int64),
            ci("n_name", DataType::Utf8),
        ]
    }

    fn partsupp_schema() -> Vec<ColumnInfo> {
        vec![
            ci("ps_partkey", DataType::Int64),
            ci("ps_suppkey", DataType::Int64),
            ci("ps_availqty", DataType::Int64),
        ]
    }

    fn part_schema() -> Vec<ColumnInfo> {
        vec![
            ci("p_partkey", DataType::Int64),
            ci("p_name", DataType::Utf8),
        ]
    }

    fn lineitem_schema() -> Vec<ColumnInfo> {
        vec![
            ci("l_partkey", DataType::Int64),
            ci("l_suppkey", DataType::Int64),
            ci("l_quantity", DataType::Float64),
            ci("l_shipdate", DataType::Date32),
        ]
    }

    /// What the catalog reports for each table.
    #[derive(Clone, Copy)]
    enum StatsMode {
        /// Row counts plus per-column NDVs.
        Full,
        /// Row counts only — what the Hive catalog provides in
        /// production (column NDVs are deliberately not fetched).
        RowCountsOnly,
        /// No statistics at all.
        None,
    }

    /// SF30-proportioned stats: part is 6M rows, lineitem 180M.
    fn q20_stats(with_ndv: bool) -> [(&'static str, Vec<ColumnInfo>, TableStatistics); 5] {
        let mut tables = q20_full_stats();
        if !with_ndv {
            for (_, _, stats) in &mut tables {
                stats.columns.clear();
            }
        }
        tables
    }

    fn q20_full_stats() -> [(&'static str, Vec<ColumnInfo>, TableStatistics); 5] {
        [
            (
                "supplier",
                supplier_schema(),
                stats_for(300_000, &[("s_suppkey", 300_000), ("s_nationkey", 25)]),
            ),
            (
                "nation",
                nation_schema(),
                stats_for(25, &[("n_nationkey", 25), ("n_name", 25)]),
            ),
            (
                "partsupp",
                partsupp_schema(),
                stats_for(
                    24_000_000,
                    &[("ps_partkey", 6_000_000), ("ps_suppkey", 300_000)],
                ),
            ),
            (
                "part",
                part_schema(),
                stats_for(6_000_000, &[("p_partkey", 6_000_000)]),
            ),
            (
                "lineitem",
                lineitem_schema(),
                stats_for(
                    180_000_000,
                    &[("l_partkey", 6_000_000), ("l_suppkey", 300_000)],
                ),
            ),
        ]
    }

    fn q20_catalog(mode: StatsMode) -> CatalogManager {
        let manager = CatalogManager::new("default", "public");
        let catalog = Arc::new(MemoryCatalog::new());
        let schema = Arc::new(MemorySchema::new());
        for (name, columns, stats) in q20_stats(matches!(mode, StatsMode::Full)) {
            let provider: Arc<dyn TableProvider> = match mode {
                StatsMode::Full | StatsMode::RowCountsOnly => Arc::new(StatsTable {
                    schema: columns,
                    stats,
                }),
                StatsMode::None => Arc::new(MemoryTable::new(columns)),
            };
            schema.register_table(name, provider);
        }
        catalog.register_schema("public", schema);
        manager.register_catalog("default", catalog);
        manager
    }

    async fn plan_sql(sql: &str, mode: StatsMode) -> LogicalPlan {
        let catalog = q20_catalog(mode);
        let planner = QueryPlanner::new(&catalog);
        let stmt = arneb_sql_parser::parse(sql).expect("parse");
        planner.plan_statement(&stmt).await.expect("plan")
    }

    /// The rendered subtree rooted at the first line containing
    /// `marker`: that line plus every following line indented deeper.
    fn subtree_text(rendered: &str, marker: &str) -> String {
        let lines: Vec<&str> = rendered.lines().collect();
        let start = lines
            .iter()
            .position(|l| l.contains(marker))
            .unwrap_or_else(|| panic!("no line containing {marker:?} in plan:\n{rendered}"));
        let indent = |l: &str| l.len() - l.trim_start().len();
        let root_indent = indent(lines[start]);
        let mut out = vec![lines[start]];
        for line in &lines[start + 1..] {
            if indent(line) <= root_indent {
                break;
            }
            out.push(line);
        }
        out.join("\n")
    }

    /// True iff some line of `text`, with indentation stripped, is
    /// exactly `wanted` (an aggregate with no aggregate expressions
    /// renders without an `aggr=` suffix).
    fn has_line(text: &str, wanted: &str) -> bool {
        text.lines().any(|l| l.trim_start() == wanted)
    }

    const LINEITEM_AGG: &str = "Aggregate: group_by=[l_partkey, l_suppkey], aggr=[SUM(l_quantity)]";

    #[tokio::test]
    async fn q20_lineitem_aggregate_is_reduced_by_forest_part_keys() {
        let plan = plan_sql(Q20_SQL, StatsMode::Full).await;
        assert_lineitem_aggregate_reduced(&format!("{plan}"));
    }

    /// The Hive catalog reports row counts and byte sizes but no column
    /// NDVs; the rewrite must still fire from row counts alone or it
    /// never fires in production.
    #[tokio::test]
    async fn q20_with_row_counts_only_is_reduced() {
        let plan = plan_sql(Q20_SQL, StatsMode::RowCountsOnly).await;
        assert_lineitem_aggregate_reduced(&format!("{plan}"));
    }

    fn assert_lineitem_aggregate_reduced(rendered: &str) {
        let agg = subtree_text(rendered, LINEITEM_AGG);

        // The aggregate itself and the decorrelation LEFT JOIN above it
        // are untouched: the rewrite only changes the aggregate's input.
        assert_eq!(
            rendered.matches(LINEITEM_AGG).count(),
            1,
            "plan:\n{rendered}"
        );
        assert!(
            rendered.contains(
                "Join: Left ON ps_partkey = __corr_key_0_0 AND ps_suppkey = __corr_key_0_1"
            ),
            "decorrelation LEFT JOIN must survive; plan:\n{rendered}"
        );
        assert!(
            rendered.contains("SemiJoin: ps_partkey = p_partkey"),
            "outer partsupp semi-join must survive; plan:\n{rendered}"
        );

        // Below the aggregate: lineitem is inner-joined to the distinct
        // forest part keys on l_partkey, and the lineitem date filters
        // stay on the lineitem side.
        assert!(
            agg.contains("Join: Inner ON l_partkey = p_partkey")
                || agg.contains("Join: Inner ON p_partkey = l_partkey"),
            "expected an inner join to the part key set under the aggregate; subtree:\n{agg}"
        );
        assert!(
            agg.contains("p_name LIKE 'forest%'"),
            "the key set must carry the part filter; subtree:\n{agg}"
        );
        assert!(
            has_line(&agg, "Aggregate: group_by=[p_partkey]"),
            "the key set must be made distinct so no lineitem row is duplicated; subtree:\n{agg}"
        );
        assert!(
            agg.contains("TableScan: lineitem") && agg.contains("TableScan: part"),
            "subtree:\n{agg}"
        );
        assert!(
            agg.contains("l_shipdate"),
            "lineitem date filters must remain under the aggregate; subtree:\n{agg}"
        );
    }

    #[tokio::test]
    async fn q20_unfiltered_part_key_set_is_not_reduced() {
        let plan = plan_sql(Q20_UNFILTERED_PART_SQL, StatsMode::RowCountsOnly).await;
        let rendered = format!("{plan}");
        let agg = subtree_text(&rendered, LINEITEM_AGG);
        assert!(
            !agg.contains("Join:") && !agg.contains("TableScan: part"),
            "every part is a key: reducing lineitem by it saves nothing; subtree:\n{agg}"
        );
    }

    #[tokio::test]
    async fn q20_without_stats_is_not_reduced() {
        let plan = plan_sql(Q20_SQL, StatsMode::None).await;
        let rendered = format!("{plan}");
        let agg = subtree_text(&rendered, LINEITEM_AGG);
        assert!(
            !agg.contains("Join:") && !agg.contains("TableScan: part"),
            "no stats, no cost-driven rewrite; subtree:\n{agg}"
        );
    }

    // ---- hand-built shapes for join types the SQL path cannot produce ----

    fn scan(table: &str, cols: &[&str]) -> LogicalPlan {
        LogicalPlan::TableScan {
            table: TableReference::table(table),
            schema: cols.iter().map(|c| ci(c, DataType::Int64)).collect(),
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

    fn eq(left: PlanExpr, right: PlanExpr) -> PlanExpr {
        PlanExpr::BinaryOp {
            left: Box::new(left),
            op: BinaryOp::Eq,
            right: Box::new(right),
            span: None,
        }
    }

    fn forest_keys() -> LogicalPlan {
        LogicalPlan::Projection {
            input: Box::new(LogicalPlan::Filter {
                input: Box::new(scan("part", &["p_partkey", "p_name"])),
                predicate: PlanExpr::BinaryOp {
                    left: Box::new(col(1, "p_name")),
                    op: BinaryOp::Like,
                    right: Box::new(PlanExpr::Literal {
                        value: ScalarValue::Utf8("forest%".to_string()),
                        span: None,
                    }),
                    span: None,
                },
            }),
            exprs: vec![col(0, "p_partkey")],
            schema: vec![ci("p_partkey", DataType::Int64)],
        }
    }

    fn outer_side() -> LogicalPlan {
        LogicalPlan::SemiJoin {
            left: Box::new(scan(
                "partsupp",
                &["ps_partkey", "ps_suppkey", "ps_availqty"],
            )),
            right: Box::new(forest_keys()),
            left_key: col(0, "ps_partkey"),
            right_key: col(0, "p_partkey"),
            residual: None,
            dynamic_filter_ids: Vec::new(),
            probe_side_filter_ids: Vec::new(),
        }
    }

    fn lineitem_aggregate() -> LogicalPlan {
        LogicalPlan::Projection {
            input: Box::new(LogicalPlan::Aggregate {
                input: Box::new(scan("lineitem", &["l_partkey", "l_suppkey", "l_quantity"])),
                group_by: vec![col(0, "l_partkey"), col(1, "l_suppkey")],
                aggr_exprs: vec![PlanExpr::Function {
                    name: "SUM".to_string(),
                    args: vec![col(2, "l_quantity")],
                    distinct: false,
                    span: None,
                }],
                schema: vec![
                    ci("l_partkey", DataType::Int64),
                    ci("l_suppkey", DataType::Int64),
                    ci("SUM(l_quantity)", DataType::Int64),
                ],
            }),
            exprs: vec![
                col(0, "l_partkey"),
                col(1, "l_suppkey"),
                col(2, "SUM(l_quantity)"),
            ],
            schema: vec![
                ci("__corr_key_0_0", DataType::Int64),
                ci("__corr_key_0_1", DataType::Int64),
                ci("__corr_scalar_0", DataType::Int64),
            ],
        }
    }

    fn decorrelated_join(join_type: ast::JoinType) -> LogicalPlan {
        // Outer side width is 3, so the aggregate's columns are 3..6.
        let condition = PlanExpr::BinaryOp {
            left: Box::new(eq(col(0, "ps_partkey"), col(3, "__corr_key_0_0"))),
            op: BinaryOp::And,
            right: Box::new(eq(col(1, "ps_suppkey"), col(4, "__corr_key_0_1"))),
            span: None,
        };
        LogicalPlan::Join {
            left: Box::new(outer_side()),
            right: Box::new(lineitem_aggregate()),
            join_type,
            condition: JoinCondition::On(condition),
            dynamic_filter_ids: Vec::new(),
        }
    }

    fn hand_built_stats() -> Arc<CatalogStats> {
        let mut stats = CatalogStats::new();
        for (name, _, table_stats) in q20_stats(false) {
            stats.insert(TableReference::table(name), table_stats);
        }
        Arc::new(stats)
    }

    fn run_pass(plan: LogicalPlan) -> LogicalPlan {
        let mut ctx = AnalyzerContext::with_stats(hand_built_stats());
        SemiJoinReduction::new()
            .analyze(plan, &mut ctx)
            .expect("pass must not error")
    }

    #[test]
    fn hand_built_left_join_is_reduced() {
        let after = run_pass(decorrelated_join(ast::JoinType::Left));
        let rendered = format!("{after}");
        let agg = subtree_text(&rendered, "Aggregate: group_by=[l_partkey, l_suppkey]");
        assert!(
            agg.contains("Join: Inner ON l_partkey = p_partkey")
                || agg.contains("Join: Inner ON p_partkey = l_partkey"),
            "subtree:\n{agg}"
        );
        assert!(
            has_line(&agg, "Aggregate: group_by=[p_partkey]"),
            "subtree:\n{agg}"
        );
        assert_eq!(
            after.schema().len(),
            decorrelated_join(ast::JoinType::Left).schema().len(),
            "the join's output schema must not change"
        );
    }

    #[test]
    fn hand_built_right_join_is_not_reduced() {
        // A RIGHT join emits aggregate groups with no outer match, so
        // dropping groups whose key is outside S would change results.
        let before = decorrelated_join(ast::JoinType::Right);
        let expected = format!("{before}");
        let after = run_pass(before);
        assert_eq!(format!("{after}"), expected);
    }

    #[test]
    fn semi_join_on_a_non_correlation_column_is_not_reduced() {
        // The outer semi-join restricts ps_suppkey, but the aggregate is
        // joined on (ps_partkey, ps_suppkey) through __corr_key_0_0 =
        // ps_partkey only in this variant: a key set over a column the
        // correlation never compares cannot prune the aggregate.
        let outer = LogicalPlan::SemiJoin {
            left: Box::new(scan(
                "partsupp",
                &["ps_partkey", "ps_suppkey", "ps_availqty"],
            )),
            right: Box::new(LogicalPlan::Projection {
                input: Box::new(LogicalPlan::Filter {
                    input: Box::new(scan("supplier", &["s_suppkey", "s_nationkey"])),
                    predicate: eq(
                        col(1, "s_nationkey"),
                        PlanExpr::Literal {
                            value: ScalarValue::Int64(3),
                            span: None,
                        },
                    ),
                }),
                exprs: vec![col(0, "s_suppkey")],
                schema: vec![ci("s_suppkey", DataType::Int64)],
            }),
            left_key: col(1, "ps_suppkey"),
            right_key: col(0, "s_suppkey"),
            residual: None,
            dynamic_filter_ids: Vec::new(),
            probe_side_filter_ids: Vec::new(),
        };
        let before = LogicalPlan::Join {
            left: Box::new(outer),
            right: Box::new(lineitem_aggregate()),
            join_type: ast::JoinType::Left,
            condition: JoinCondition::On(eq(col(0, "ps_partkey"), col(3, "__corr_key_0_0"))),
            dynamic_filter_ids: Vec::new(),
        };
        let expected = format!("{before}");
        let after = run_pass(before);
        assert_eq!(format!("{after}"), expected);
    }
}
