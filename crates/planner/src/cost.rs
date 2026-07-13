//! Cardinality propagation cost model.
//!
//! Defines `Cost = f64` representing expected output row count, and
//! `estimated_cardinality(plan, stats)` walking a `LogicalPlan` to compute
//! that cost. The model is the basis for the cost-based join reorderer.
//!
//! Conservative semantics: every estimate is clamped to `[1.0, f64::MAX]`,
//! never panics, never returns NaN or negative.

use std::collections::HashMap;
use std::sync::Arc;

use arneb_catalog::{ColumnStatistics, TableStatistics};
use arneb_common::types::TableReference;
use arneb_sql_parser::ast;

use crate::plan::{JoinCondition, LogicalPlan, PlanExpr};
use crate::selectivity::{self, ColumnStatsLookup};

/// Cost of a logical plan node. Currently expected output row count.
pub type Cost = f64;

/// Default row count used when a `TableScan`'s underlying provider returns
/// no statistics. Picked to be small enough that the reorderer prefers
/// known-larger tables on the probe side, but large enough not to dwarf
/// real stats.
pub const DEFAULT_TABLE_SIZE: u64 = 10_000;

/// Per-query snapshot of statistics for every `TableScan`-referenced table.
///
/// Populated once at the start of planning (after the `LogicalPlan` is
/// built, before the analyzer runs) by walking every `TableScan` and
/// invoking `TableProvider::statistics()`. Threaded through
/// `AnalyzerContext` so cost-using passes (notably `JoinReorder`) can read
/// stats without re-hitting the catalog.
#[derive(Debug, Clone, Default)]
pub struct CatalogStats {
    tables: HashMap<TableReference, Arc<TableStatistics>>,
}

impl CatalogStats {
    /// Creates an empty snapshot. Useful as a fallback when no statistics
    /// are available; the cost model degrades to defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the statistics for `reference`, or `None`.
    pub fn get(&self, reference: &TableReference) -> Option<&TableStatistics> {
        self.tables.get(reference).map(|arc| arc.as_ref())
    }

    /// Inserts statistics for `reference`. Replaces any existing entry.
    pub fn insert(&mut self, reference: TableReference, stats: TableStatistics) {
        self.tables.insert(reference, Arc::new(stats));
    }

    /// Returns the number of tables with statistics in this snapshot.
    pub fn len(&self) -> usize {
        self.tables.len()
    }

    /// Returns whether the snapshot is empty.
    pub fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }
}

/// Resolver bridging the cost model's `(input, stats)` view into the
/// `ColumnStatsLookup` shape expected by `selectivity::selectivity`.
///
/// Looks up `ColumnStatistics` by walking the `input` plan to find a
/// `TableScan` whose `TableStatistics` (from `CatalogStats`) holds a
/// matching column. Stops at the first match — when a column name is
/// ambiguous across joined tables, the first scan wins (good enough for
/// v1; future work can promote the resolution to use schema indices).
struct PlanLookup<'a> {
    input: &'a LogicalPlan,
    stats: &'a CatalogStats,
}

impl ColumnStatsLookup for PlanLookup<'_> {
    fn lookup(&self, column: &str) -> Option<ColumnStatistics> {
        walk_scans(self.input, &mut |table_ref| {
            self.stats
                .get(table_ref)
                .and_then(|t| t.columns.get(column).cloned())
        })
    }
}

/// Estimates the output cardinality of `plan` in rows.
///
/// Always returns a finite, non-negative `f64`. The result is clamped to
/// `[1.0, f64::MAX]` so downstream consumers (notably the inner-join NDV
/// formula `left*right / max(ndv_l, ndv_r)`) cannot divide by zero.
pub fn estimated_cardinality(plan: &LogicalPlan, stats: &CatalogStats) -> Cost {
    let raw = match plan {
        LogicalPlan::TableScan { table, .. } => stats
            .get(table)
            .and_then(|s| s.row_count)
            .unwrap_or(DEFAULT_TABLE_SIZE) as f64,

        LogicalPlan::Filter { input, predicate } => {
            let child = estimated_cardinality(input, stats);
            let lookup = PlanLookup { input, stats };
            child * selectivity::selectivity(predicate, &lookup)
        }

        LogicalPlan::Projection { input, .. } => estimated_cardinality(input, stats),

        LogicalPlan::Sort { input, .. } => estimated_cardinality(input, stats),

        LogicalPlan::Limit { input, limit, .. } => {
            let child = estimated_cardinality(input, stats);
            match limit {
                Some(n) => child.min(*n as f64),
                None => child,
            }
        }

        LogicalPlan::Explain { input, .. } => estimated_cardinality(input, stats),

        LogicalPlan::Distinct { input } => {
            // Without per-column NDV walk, the safest conservative is
            // "distinct does not increase rows". Tightened in 2.3 when
            // selectivity hooks in.
            estimated_cardinality(input, stats)
        }

        LogicalPlan::Aggregate {
            input,
            group_by,
            schema,
            ..
        }
        | LogicalPlan::PartialAggregate {
            input,
            group_by,
            schema,
            ..
        }
        | LogicalPlan::FinalAggregate {
            input,
            group_by,
            schema,
            ..
        } => {
            let child = estimated_cardinality(input, stats);
            if group_by.is_empty() {
                // Global aggregate collapses to one row.
                1.0
            } else {
                let ndv_product = group_by_ndv_product(group_by, input, stats, schema);
                child.min(ndv_product)
            }
        }

        LogicalPlan::Window { input, .. } => estimated_cardinality(input, stats),

        LogicalPlan::AssignUniqueId { input, .. } => estimated_cardinality(input, stats),

        LogicalPlan::Join {
            left,
            right,
            join_type,
            condition,
            ..
        } => estimate_join(left, right, *join_type, condition, stats),

        LogicalPlan::SemiJoin { left, .. } | LogicalPlan::AntiJoin { left, .. } => {
            // Semi/anti: at most |left|; conservatively use 50% selectivity
            // when stats are not informative.
            estimated_cardinality(left, stats) * 0.5
        }

        LogicalPlan::UnionAll { inputs } => inputs
            .iter()
            .map(|p| estimated_cardinality(p, stats))
            .sum::<f64>(),

        LogicalPlan::Intersect { left, right } => {
            let l = estimated_cardinality(left, stats);
            let r = estimated_cardinality(right, stats);
            l.min(r)
        }

        LogicalPlan::Except { left, .. } => estimated_cardinality(left, stats),

        LogicalPlan::ScalarSubquery { subplan } => {
            // The subquery produces at most one row by definition.
            estimated_cardinality(subplan, stats).min(1.0)
        }

        LogicalPlan::ExchangeNode { .. } => DEFAULT_TABLE_SIZE as f64,

        // DDL/DML: no rows materialized to the planner cost model. Return
        // 1.0 so downstream consumers don't divide by zero.
        LogicalPlan::CreateTable { .. }
        | LogicalPlan::DropTable { .. }
        | LogicalPlan::CreateTableAsSelect { .. }
        | LogicalPlan::InsertInto { .. }
        | LogicalPlan::DeleteFrom { .. }
        | LogicalPlan::CreateView { .. }
        | LogicalPlan::DropView { .. } => 1.0,

        // Synthetic single-row source (`SELECT 1`, `SELECT 1 + 1`).
        LogicalPlan::OneRow => 1.0,
    };
    clamp(raw)
}

// ---------------------------------------------------------------------------
// A2.2 broadcast-eligibility size estimate (2026-05-28)
// ---------------------------------------------------------------------------

/// Estimate the average row width in bytes for an Arneb logical schema.
///
/// Per-column estimates use Arrow's fixed widths where known and a
/// conservative ~16 bytes for variable-length types (Utf8 / Binary /
/// LargeUtf8) — this matches Trino's `VariableWidthBlock` heuristic for
/// VARCHAR build-side estimation. Used by `estimated_bytes` for
/// broadcast-join eligibility decisions; not a memory accounting tool.
pub fn estimate_row_width_bytes(schema: &[arneb_common::types::ColumnInfo]) -> usize {
    use arneb_common::types::DataType;
    schema
        .iter()
        .map(|col| match &col.data_type {
            DataType::Null => 0,
            DataType::Boolean | DataType::Int8 => 1,
            DataType::Int16 => 2,
            DataType::Int32 | DataType::Date32 | DataType::Float32 => 4,
            DataType::Int64 | DataType::Float64 | DataType::Timestamp { .. } => 8,
            DataType::Decimal64 { .. } => 8,
            DataType::Decimal128 { .. } => 16,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary => 16,
            DataType::List(_) | DataType::Map(_, _) | DataType::Struct(_) => 32,
            // `DataType` is `#[non_exhaustive]`; conservative default for
            // future variants. The fragmenter falls back to "not broadcastable"
            // when bytes overflow, so over-estimating is safe.
            _ => 32,
        })
        .sum()
}

/// Estimate the total in-memory size of `plan`'s output in bytes
/// (`estimated_cardinality(plan, stats) × estimate_row_width_bytes(schema)`).
///
/// Used by the fragmenter to gate broadcast-join eligibility against the
/// `ExecutionContext.broadcast_max_build_bytes` threshold. Saturates at
/// `usize::MAX` rather than panicking on overflow; returns 0 when the
/// plan's schema is empty (no columns => can't broadcast meaningful data).
pub fn estimated_bytes(plan: &LogicalPlan, stats: &CatalogStats) -> usize {
    let rows = estimated_cardinality(plan, stats);
    let width = estimate_row_width_bytes(&plan.schema());
    if width == 0 {
        return 0;
    }
    let bytes = (rows * width as f64).max(0.0);
    if bytes >= usize::MAX as f64 {
        usize::MAX
    } else {
        bytes as usize
    }
}

// ---------------------------------------------------------------------------
// Partition-aware Selinger cost
// ---------------------------------------------------------------------------

/// Default rowgroup-aligned target bytes per Parquet partition. Used to
/// infer parallelism from `TableStatistics::size_bytes` when the
/// connector hasn't published an explicit partition count.
///
/// Tuned at 32MB to match the median Parquet file size produced by our
/// docker compose TPC-H seed (files range 4–55 MiB); a higher threshold
/// (e.g. 128MB) collapses tables like SF1 lineitem (149MB across 4
/// files) to a single-partition estimate, defeating Selinger DP's
/// reorder decisions for two-table queries (Q14/Q19).
const BYTES_PER_PARTITION: u64 = 32 * 1024 * 1024;

/// Hardware-aware cap on how many partitions a single TableScan is
/// assumed to expose. Mirrors the executor's `num_cpus`-defaulted
/// target_partitions for plan-time cost decisions.
fn target_partitions() -> u64 {
    std::thread::available_parallelism()
        .map(|n| n.get() as u64)
        .unwrap_or(8)
}

/// Estimate the number of parallel partitions a `TableStatistics` will
/// produce at execution time. Approximated as `size_bytes /
/// BYTES_PER_PARTITION`, clamped to `[1, target_partitions()]`. Returns
/// `1` when `size_bytes` is unknown.
pub fn estimate_partitions_for_stats(s: &TableStatistics) -> u64 {
    let bytes = s.size_bytes.unwrap_or(0);
    if bytes == 0 {
        return 1;
    }
    (bytes / BYTES_PER_PARTITION)
        .max(1)
        .min(target_partitions())
}

/// Walks down the *left spine* of `plan` (Joins always recurse into
/// `left`, sub-plan wrappers recurse into `input`) to find the
/// outermost `TableScan` and return its estimated partition count.
/// Drives the parallelism factor used by [`selinger_cost`] — the entire
/// left-deep tree shares its leftmost leaf's partition count, mirroring
/// `HashJoinExec::output_partitioning` (Step 3.6).
pub fn leftmost_leaf_partitions(plan: &LogicalPlan, stats: &CatalogStats) -> u64 {
    match plan {
        LogicalPlan::Join { left, .. } => leftmost_leaf_partitions(left, stats),
        LogicalPlan::TableScan { table, .. } => stats
            .get(table)
            .map(estimate_partitions_for_stats)
            .unwrap_or(1),
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Projection { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Distinct { input } => leftmost_leaf_partitions(input, stats),
        _ => 1,
    }
}

/// Partition-aware cost function for the Selinger DP. Returns a
/// wall-clock-proxy cost that accounts for executor parallelism:
///
/// - **Probe / output cost** of every join in the left spine is divided
///   by the uniform partition factor — these stages parallelise per the
///   v1 probe-side parallel hash join (Step 3.6).
/// - **Build cost** at every level is the right-subtree cardinality
///   *without* parallelism division — `HashJoinExec` collects and
///   builds the right side sequentially (single `OnceCell`).
/// - **Leaf scan cost** is paralleled by the uniform factor.
///
/// Step SP (per-file row-range splits) means every Hive table exposes
/// `~target_partitions` partitions at execution time regardless of
/// file/byte size, so the DP no longer needs to use the leftmost-leaf
/// partition count as a tie-breaker. Using a uniform factor lets the
/// build_cost term differentiate orderings cleanly — the DP picks the
/// order with the smallest sum of right-subtree cardinalities, which
/// matches our v1 hash-join's actual wall-clock bottleneck. Without
/// this fix, Selinger picked Q12's lineitem-LEFT/orders-RIGHT (build
/// 1.5M) over orders-LEFT/lineitem-RIGHT (build 245K) because orders'
/// 50MB size mapped to a 1-partition estimate that inflated cost
/// division for orderings with orders on the outer.
pub fn selinger_cost(plan: &LogicalPlan, stats: &CatalogStats) -> Cost {
    let outermost = target_partitions().max(1) as f64;
    selinger_cost_inner(plan, stats, outermost)
}

/// Enables a join-order scoring mode that charges each left-deep join's
/// intermediate output at full cardinality. Default OFF.
pub fn selective_dim_first_enabled() -> bool {
    std::env::var("ARNEB_SELECTIVE_DIM_FIRST").is_ok_and(|v| v == "1")
}

pub(crate) const SELECTIVE_DIM_TINY_FILTER_ROWS: Cost = 10.0;
pub(crate) const SELECTIVE_DIM_CHAIN_SMALL_ROWS: Cost = 50_000.0;

fn selinger_cost_inner(plan: &LogicalPlan, stats: &CatalogStats, p: f64) -> Cost {
    match plan {
        LogicalPlan::Join {
            left,
            right,
            condition,
            ..
        } => {
            let left_cost = selinger_cost_inner(left, stats, p);
            let output_card = estimated_cardinality(plan, stats);
            let build_cost = estimated_cardinality(right, stats);
            let output_cost = output_card / p;
            if selective_dim_first_enabled() && matches!(condition, JoinCondition::On(_)) {
                let reduction = if output_card > 0.0 {
                    build_cost / output_card
                } else {
                    f64::INFINITY
                };
                let left_card = estimated_cardinality(left, stats);
                if reduction >= 10.0
                    && left_card <= 100_000.0
                    && (10_000_000.0..=100_000_000.0).contains(&build_cost)
                    && min_filtered_leaf_cardinality(left, stats) <= SELECTIVE_DIM_TINY_FILTER_ROWS
                {
                    return left_cost + output_card;
                }
            }
            left_cost + build_cost + output_cost
        }
        _ => estimated_cardinality(plan, stats) / p,
    }
}

fn min_filtered_leaf_cardinality(plan: &LogicalPlan, stats: &CatalogStats) -> Cost {
    match plan {
        LogicalPlan::Join { left, right, .. } => min_filtered_leaf_cardinality(left, stats)
            .min(min_filtered_leaf_cardinality(right, stats)),
        LogicalPlan::Filter { .. } | LogicalPlan::TableScan { .. } => {
            estimated_cardinality(plan, stats)
        }
        LogicalPlan::Projection { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Explain { input, .. }
        | LogicalPlan::Distinct { input }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::PartialAggregate { input, .. }
        | LogicalPlan::FinalAggregate { input, .. }
        | LogicalPlan::Window { input, .. }
        | LogicalPlan::AssignUniqueId { input, .. } => min_filtered_leaf_cardinality(input, stats),
        _ => estimated_cardinality(plan, stats),
    }
}

/// Estimates the output cardinality of a join.
fn estimate_join(
    left: &LogicalPlan,
    right: &LogicalPlan,
    join_type: ast::JoinType,
    condition: &JoinCondition,
    stats: &CatalogStats,
) -> Cost {
    let left_card = estimated_cardinality(left, stats);
    let right_card = estimated_cardinality(right, stats);

    let inner_estimate = match condition {
        // Cross join has no condition — product is exact.
        JoinCondition::None => left_card * right_card,
        // ON expression: best-effort uniform-NDV formula
        // `(L * R) / max(ndv_l, ndv_r, 1)`.
        JoinCondition::On(expr) => {
            let ndv = best_effort_join_ndv(expr, left, right, stats).max(1.0);
            (left_card * right_card) / ndv
        }
    };

    match join_type {
        ast::JoinType::Inner => inner_estimate,
        ast::JoinType::Left => left_card.max(inner_estimate),
        ast::JoinType::Right => right_card.max(inner_estimate),
        ast::JoinType::Full => left_card + right_card + inner_estimate,
        ast::JoinType::Cross => left_card * right_card,
    }
}

/// Best-effort NDV estimate for an equi-join key referenced by an `ON`
/// expression. Walks the `PlanExpr` tree for column references and looks
/// up their `ColumnStatistics::ndv` in either side's stats. Returns
/// `DEFAULT_TABLE_SIZE as f64` when nothing is known (yields the input
/// row count as a conservative NDV, which means `inner_estimate` collapses
/// to `min(L, R)`).
fn best_effort_join_ndv(
    expr: &PlanExpr,
    left: &LogicalPlan,
    right: &LogicalPlan,
    stats: &CatalogStats,
) -> f64 {
    let mut best: f64 = 1.0;
    visit_columns(expr, &mut |name| {
        if let Some(ndv) =
            lookup_column_ndv(name, left, stats).or_else(|| lookup_column_ndv(name, right, stats))
        {
            best = best.max(ndv as f64);
        }
    });
    if best > 1.0 {
        return best;
    }
    // No column NDV resolved (common with HMS-managed tables that lack
    // column statistics). Fall back to the **unfiltered scan cardinality
    // of the table that OWNS each join-key column**, taking the min across
    // the key columns as the equi-key distinct-count proxy.
    //
    // Why per-key-owner: an equi-key's distinct-count is bounded by its
    // underlying (PK) table. For `lineitem ⋈ filtered_part` on partkey
    // that is `part` (200k) — preserving the filter's selectivity through
    // the join `(6M filtered_L * 20k filtered_R) / 200k = ~600k`.
    //
    // The earlier `unfiltered_scan_card(left).min(right)` proxy took the
    // min over EVERY base scan in the subtree. That is correct for a
    // 2-table join, but BREAKS on a 3+ table chain: once a small selective
    // dimension (TPC-H Q08 `part`, 13k) is already in the intermediate, it
    // caps EVERY later join key's NDV at part's size, inflating downstream
    // join estimates (q08 `lineitem⋈part⋈orders`: orderkey wrongly capped
    // at 2M -> output (6M*3.75M)/2M = 11.25M instead of ~1.5M) and pushing
    // the selective dimension LATE in the join order. Resolving each key by
    // its owning table keeps the 2-table answer identical while fixing the
    // chain case. Falls back to the old subtree-min when no key column
    // resolves to a scan (e.g. a key over a computed column).
    let mut owner_cards: Vec<f64> = Vec::new();
    visit_columns(expr, &mut |name| {
        if let Some(c) = key_owner_unfiltered_card(name, left, stats)
            .or_else(|| key_owner_unfiltered_card(name, right, stats))
        {
            owner_cards.push(c);
        }
    });
    if let Some(min_owner) = owner_cards.into_iter().reduce(f64::min) {
        return min_owner.max(1.0);
    }
    let l_unfiltered = unfiltered_scan_card(left, stats);
    let r_unfiltered = unfiltered_scan_card(right, stats);
    l_unfiltered.min(r_unfiltered).max(1.0)
}

/// Unfiltered `row_count` of the `TableScan` in `plan` whose schema
/// declares `col_name` — the equi-key's distinct-count proxy when no
/// column NDV stat exists. Walks through row-preserving wrappers and into
/// both join children; returns the FIRST owning scan's size (a self-join's
/// twin scans share a row_count, so first-match is fine). `None` when no
/// scan in the subtree owns the column.
fn key_owner_unfiltered_card(
    col_name: &str,
    plan: &LogicalPlan,
    stats: &CatalogStats,
) -> Option<f64> {
    match plan {
        LogicalPlan::TableScan { table, schema, .. } => {
            schema.iter().any(|c| c.name == col_name).then(|| {
                stats
                    .get(table)
                    .and_then(|s| s.row_count)
                    .unwrap_or(DEFAULT_TABLE_SIZE) as f64
            })
        }
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Projection { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Distinct { input }
        | LogicalPlan::Explain { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::PartialAggregate { input, .. }
        | LogicalPlan::FinalAggregate { input, .. }
        | LogicalPlan::Window { input, .. }
        | LogicalPlan::AssignUniqueId { input, .. } => {
            key_owner_unfiltered_card(col_name, input, stats)
        }
        LogicalPlan::Join { left, right, .. }
        | LogicalPlan::SemiJoin { left, right, .. }
        | LogicalPlan::AntiJoin { left, right, .. }
        | LogicalPlan::Intersect { left, right }
        | LogicalPlan::Except { left, right } => key_owner_unfiltered_card(col_name, left, stats)
            .or_else(|| key_owner_unfiltered_card(col_name, right, stats)),
        _ => None,
    }
}

/// Walk past `Filter` / `Projection` / `Sort` / `Limit` / `Distinct`
/// / `Explain` / `Aggregate` wrappers to the underlying `TableScan`'s
/// declared `row_count`. For composite plans (Join / Union / etc.)
/// returns the same value as the regular `estimated_cardinality`.
///
/// Used by [`best_effort_join_ndv`] to compute a join key's expected
/// distinct-count from the scan's row count when column-level NDV stats
/// are unavailable. The recursive `min` of two child scans on a `Join`
/// matches the intuition that an equi-key on a chain of joins is
/// distinct-count-bounded by whichever underlying table is smallest.
fn unfiltered_scan_card(plan: &LogicalPlan, stats: &CatalogStats) -> f64 {
    match plan {
        LogicalPlan::TableScan { table, .. } => stats
            .get(table)
            .and_then(|s| s.row_count)
            .unwrap_or(DEFAULT_TABLE_SIZE) as f64,
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Projection { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Distinct { input }
        | LogicalPlan::Explain { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::PartialAggregate { input, .. }
        | LogicalPlan::FinalAggregate { input, .. }
        | LogicalPlan::Window { input, .. }
        | LogicalPlan::AssignUniqueId { input, .. } => unfiltered_scan_card(input, stats),
        LogicalPlan::Join { left, right, .. }
        | LogicalPlan::SemiJoin { left, right, .. }
        | LogicalPlan::AntiJoin { left, right, .. }
        | LogicalPlan::Intersect { left, right }
        | LogicalPlan::Except { left, right } => {
            unfiltered_scan_card(left, stats).min(unfiltered_scan_card(right, stats))
        }
        _ => estimated_cardinality(plan, stats),
    }
}

fn lookup_column_ndv(name: &str, plan: &LogicalPlan, stats: &CatalogStats) -> Option<u64> {
    walk_scans(plan, &mut |scan_table| {
        stats
            .get(scan_table)
            .and_then(|t| t.columns.get(name))
            .and_then(|c| c.ndv)
    })
}

fn walk_scans<T, F>(plan: &LogicalPlan, callback: &mut F) -> Option<T>
where
    F: FnMut(&TableReference) -> Option<T>,
{
    match plan {
        LogicalPlan::TableScan { table, .. } => callback(table),
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Projection { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Distinct { input }
        | LogicalPlan::Explain { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::PartialAggregate { input, .. }
        | LogicalPlan::FinalAggregate { input, .. }
        | LogicalPlan::Window { input, .. } => walk_scans(input, callback),
        LogicalPlan::Join { left, right, .. }
        | LogicalPlan::SemiJoin { left, right, .. }
        | LogicalPlan::AntiJoin { left, right, .. }
        | LogicalPlan::Intersect { left, right }
        | LogicalPlan::Except { left, right } => {
            walk_scans(left, callback).or_else(|| walk_scans(right, callback))
        }
        LogicalPlan::UnionAll { inputs } => {
            for input in inputs {
                if let Some(v) = walk_scans(input, callback) {
                    return Some(v);
                }
            }
            None
        }
        LogicalPlan::ScalarSubquery { subplan } => walk_scans(subplan, callback),
        _ => None,
    }
}

fn visit_columns<F>(expr: &PlanExpr, callback: &mut F)
where
    F: FnMut(&str),
{
    match expr {
        PlanExpr::Column { name, .. } => callback(name),
        PlanExpr::BinaryOp { left, right, .. } => {
            visit_columns(left, callback);
            visit_columns(right, callback);
        }
        PlanExpr::UnaryOp { expr, .. }
        | PlanExpr::IsNull { expr, .. }
        | PlanExpr::IsNotNull { expr, .. }
        | PlanExpr::Cast { expr, .. } => visit_columns(expr, callback),
        PlanExpr::Between {
            expr, low, high, ..
        } => {
            visit_columns(expr, callback);
            visit_columns(low, callback);
            visit_columns(high, callback);
        }
        PlanExpr::InList { expr, list, .. } => {
            visit_columns(expr, callback);
            for item in list {
                visit_columns(item, callback);
            }
        }
        PlanExpr::Function { args, .. } => {
            for a in args {
                visit_columns(a, callback);
            }
        }
        PlanExpr::CaseExpr {
            operand,
            when_clauses,
            else_result,
            ..
        } => {
            if let Some(op) = operand {
                visit_columns(op, callback);
            }
            for (w, t) in when_clauses {
                visit_columns(w, callback);
                visit_columns(t, callback);
            }
            if let Some(e) = else_result {
                visit_columns(e, callback);
            }
        }
        // Literals, parameters, wildcards, and scalar subqueries don't
        // contribute column NDV information.
        _ => {}
    }
}

fn group_by_ndv_product(
    group_by: &[PlanExpr],
    input: &LogicalPlan,
    stats: &CatalogStats,
    _schema: &[arneb_common::types::ColumnInfo],
) -> f64 {
    let mut product = 1.0_f64;
    for expr in group_by {
        let mut col_ndv: Option<u64> = None;
        visit_columns(expr, &mut |name| {
            if col_ndv.is_some() {
                return;
            }
            col_ndv = lookup_column_ndv(name, input, stats);
        });
        let ndv = col_ndv.map(|v| v as f64).unwrap_or_else(|| {
            // Conservative default: roughly `sqrt(child_size)` so a
            // grouped aggregate doesn't get estimated as "no rows" when
            // stats are missing.
            estimated_cardinality(input, stats).sqrt().max(1.0)
        });
        product = product.saturating_mul_f64(ndv);
    }
    product
}

/// Saturating-multiply for f64 in `[0, f64::MAX]`.
trait SaturatingMulF64 {
    fn saturating_mul_f64(self, other: f64) -> f64;
}
impl SaturatingMulF64 for f64 {
    fn saturating_mul_f64(self, other: f64) -> f64 {
        let prod = self * other;
        if prod.is_finite() {
            prod
        } else {
            f64::MAX
        }
    }
}

/// Returns a borrow of the named column's `ColumnStatistics` from `stats`
/// for the given `table`, or `None`.
pub fn column_stats<'a>(
    stats: &'a CatalogStats,
    table: &TableReference,
    column: &str,
) -> Option<&'a ColumnStatistics> {
    stats.get(table).and_then(|t| t.columns.get(column))
}

fn clamp(value: f64) -> Cost {
    if !value.is_finite() || value < 1.0 {
        1.0
    } else {
        value
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arneb_common::types::{ColumnInfo, DataType, ScalarValue};
    use arneb_sql_parser::ast::{BinaryOp as AstBinaryOp, JoinType};

    fn col(name: &str) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn make_scan(table_name: &str, cols: Vec<&str>) -> LogicalPlan {
        LogicalPlan::TableScan {
            table: TableReference::table(table_name),
            schema: cols.iter().map(|c| col(c)).collect(),
            alias: None,
            properties: HashMap::new(),
            dynamic_filters_consumed: Vec::new(),
        }
    }

    fn make_stats(row_count: u64) -> TableStatistics {
        TableStatistics {
            row_count: Some(row_count),
            ..TableStatistics::default()
        }
    }

    fn make_stats_with_col(row_count: u64, col_name: &str, ndv: u64) -> TableStatistics {
        let mut columns = HashMap::new();
        columns.insert(
            col_name.to_string(),
            ColumnStatistics {
                ndv: Some(ndv),
                ..ColumnStatistics::default()
            },
        );
        TableStatistics {
            row_count: Some(row_count),
            size_bytes: None,
            columns,
        }
    }

    fn col_expr(idx: usize, name: &str) -> PlanExpr {
        PlanExpr::Column {
            index: idx,
            name: name.to_string(),
            span: None,
        }
    }

    fn lit(value: ScalarValue) -> PlanExpr {
        PlanExpr::Literal { value, span: None }
    }

    fn eq(left: PlanExpr, right: PlanExpr) -> PlanExpr {
        PlanExpr::BinaryOp {
            left: Box::new(left),
            op: AstBinaryOp::Eq,
            right: Box::new(right),
            span: None,
        }
    }

    // -- TableScan --

    #[test]
    fn tablescan_with_row_count_uses_stats() {
        let mut catalog_stats = CatalogStats::new();
        catalog_stats.insert(TableReference::table("lineitem"), make_stats(6_000_000));
        let plan = make_scan("lineitem", vec!["x"]);
        let cost = estimated_cardinality(&plan, &catalog_stats);
        assert_eq!(cost, 6_000_000.0);
    }

    #[test]
    fn tablescan_without_row_count_uses_default() {
        let catalog_stats = CatalogStats::new();
        let plan = make_scan("orders", vec!["x"]);
        let cost = estimated_cardinality(&plan, &catalog_stats);
        assert_eq!(cost, DEFAULT_TABLE_SIZE as f64);
    }

    // -- Filter --

    #[test]
    fn filter_without_column_ndv_uses_default_eq_selectivity() {
        // No `ndv` recorded for column "x" → selectivity estimator falls
        // back to `DEFAULT_EQ_SELECTIVITY` (0.1).
        let mut catalog_stats = CatalogStats::new();
        catalog_stats.insert(TableReference::table("orders"), make_stats(1_500_000));
        let plan = LogicalPlan::Filter {
            input: Box::new(make_scan("orders", vec!["x"])),
            predicate: eq(col_expr(0, "x"), lit(ScalarValue::Int64(1))),
        };
        let cost = estimated_cardinality(&plan, &catalog_stats);
        assert_eq!(
            cost,
            1_500_000.0 * crate::selectivity::DEFAULT_EQ_SELECTIVITY
        );
    }

    #[test]
    fn filter_with_column_ndv_uses_one_over_ndv() {
        // With ndv=25, equality selectivity is 1/25 = 0.04.
        let mut catalog_stats = CatalogStats::new();
        catalog_stats.insert(
            TableReference::table("orders"),
            make_stats_with_col(1_500_000, "x", 25),
        );
        let plan = LogicalPlan::Filter {
            input: Box::new(make_scan("orders", vec!["x"])),
            predicate: eq(col_expr(0, "x"), lit(ScalarValue::Int64(1))),
        };
        let cost = estimated_cardinality(&plan, &catalog_stats);
        assert!((cost - 1_500_000.0 / 25.0).abs() < 1e-6);
    }

    // -- Projection --

    #[test]
    fn projection_preserves_row_count() {
        let mut catalog_stats = CatalogStats::new();
        catalog_stats.insert(TableReference::table("orders"), make_stats(1_500_000));
        let scan = make_scan("orders", vec!["x"]);
        let plan = LogicalPlan::Projection {
            input: Box::new(scan),
            exprs: vec![col_expr(0, "x")],
            schema: vec![col("x")],
        };
        let cost = estimated_cardinality(&plan, &catalog_stats);
        assert_eq!(cost, 1_500_000.0);
    }

    // -- Limit --

    #[test]
    fn limit_caps_at_n() {
        let mut catalog_stats = CatalogStats::new();
        catalog_stats.insert(TableReference::table("orders"), make_stats(1_500_000));
        let plan = LogicalPlan::Limit {
            input: Box::new(make_scan("orders", vec!["x"])),
            limit: Some(100),
            offset: None,
        };
        let cost = estimated_cardinality(&plan, &catalog_stats);
        assert_eq!(cost, 100.0);
    }

    #[test]
    fn limit_below_child_returns_n() {
        let mut catalog_stats = CatalogStats::new();
        catalog_stats.insert(TableReference::table("nation"), make_stats(25));
        let plan = LogicalPlan::Limit {
            input: Box::new(make_scan("nation", vec!["x"])),
            limit: Some(100),
            offset: None,
        };
        let cost = estimated_cardinality(&plan, &catalog_stats);
        assert_eq!(cost, 25.0);
    }

    #[test]
    fn limit_none_is_passthrough() {
        let mut catalog_stats = CatalogStats::new();
        catalog_stats.insert(TableReference::table("orders"), make_stats(1_500_000));
        let plan = LogicalPlan::Limit {
            input: Box::new(make_scan("orders", vec!["x"])),
            limit: None,
            offset: None,
        };
        let cost = estimated_cardinality(&plan, &catalog_stats);
        assert_eq!(cost, 1_500_000.0);
    }

    // -- Sort --

    #[test]
    fn sort_is_passthrough() {
        let mut catalog_stats = CatalogStats::new();
        catalog_stats.insert(TableReference::table("orders"), make_stats(1_500_000));
        let plan = LogicalPlan::Sort {
            input: Box::new(make_scan("orders", vec!["x"])),
            order_by: vec![],
        };
        let cost = estimated_cardinality(&plan, &catalog_stats);
        assert_eq!(cost, 1_500_000.0);
    }

    // -- InnerJoin --

    #[test]
    fn inner_join_uses_ndv_formula() {
        let mut catalog_stats = CatalogStats::new();
        catalog_stats.insert(
            TableReference::table("lineitem"),
            make_stats_with_col(6_000_000, "l_orderkey", 1_500_000),
        );
        catalog_stats.insert(
            TableReference::table("orders"),
            make_stats_with_col(1_500_000, "o_orderkey", 1_500_000),
        );
        let plan = LogicalPlan::Join {
            left: Box::new(make_scan("lineitem", vec!["l_orderkey"])),
            right: Box::new(make_scan("orders", vec!["o_orderkey"])),
            join_type: JoinType::Inner,
            condition: JoinCondition::On(eq(col_expr(0, "l_orderkey"), col_expr(0, "o_orderkey"))),
            dynamic_filter_ids: Vec::new(),
        };
        let cost = estimated_cardinality(&plan, &catalog_stats);
        // (6M * 1.5M) / max(1.5M, 1.5M) = 6M
        assert_eq!(cost, 6_000_000.0);
    }

    #[test]
    fn chained_join_ndv_fallback_uses_key_owner_not_subtree_min() {
        // Regression for the q08 join-order bug: with NO column NDV stats
        // (the HMS reality), the join-key NDV fallback must size each key
        // by the table that OWNS it, not by the smallest scan anywhere in
        // the subtree. Chain mirrors q08's `lineitem ⋈ part ⋈ orders`.
        let mut s = CatalogStats::new();
        s.insert(TableReference::table("lineitem"), make_stats(6_000_000));
        s.insert(TableReference::table("part"), make_stats(200_000));
        s.insert(TableReference::table("orders"), make_stats(1_500_000));

        // lineitem ⋈ part ON l_partkey = p_partkey  -> ~6M (FK side)
        let li_part = LogicalPlan::Join {
            left: Box::new(make_scan("lineitem", vec!["l_orderkey", "l_partkey"])),
            right: Box::new(make_scan("part", vec!["p_partkey"])),
            join_type: JoinType::Inner,
            condition: JoinCondition::On(eq(col_expr(1, "l_partkey"), col_expr(2, "p_partkey"))),
            dynamic_filter_ids: Vec::new(),
        };
        assert_eq!(estimated_cardinality(&li_part, &s), 6_000_000.0);

        // (lineitem ⋈ part) ⋈ orders ON l_orderkey = o_orderkey.
        // orderkey NDV must resolve to its owners min(lineitem 6M,
        // orders 1.5M) = 1.5M, giving (6M*1.5M)/1.5M = 6M — NOT the
        // subtree-min proxy (part's 200k) which inflates to 45M and
        // makes the cost model defer the selective `part`.
        let li_part_orders = LogicalPlan::Join {
            left: Box::new(li_part),
            right: Box::new(make_scan("orders", vec!["o_orderkey"])),
            join_type: JoinType::Inner,
            condition: JoinCondition::On(eq(col_expr(0, "l_orderkey"), col_expr(3, "o_orderkey"))),
            dynamic_filter_ids: Vec::new(),
        };
        assert_eq!(estimated_cardinality(&li_part_orders, &s), 6_000_000.0);
    }

    #[test]
    fn cross_join_is_full_product() {
        let mut catalog_stats = CatalogStats::new();
        catalog_stats.insert(TableReference::table("a"), make_stats(100));
        catalog_stats.insert(TableReference::table("b"), make_stats(50));
        let plan = LogicalPlan::Join {
            left: Box::new(make_scan("a", vec!["x"])),
            right: Box::new(make_scan("b", vec!["x"])),
            join_type: JoinType::Cross,
            condition: JoinCondition::None,
            dynamic_filter_ids: Vec::new(),
        };
        let cost = estimated_cardinality(&plan, &catalog_stats);
        assert_eq!(cost, 100.0 * 50.0);
    }

    #[test]
    fn left_join_is_at_least_left_size() {
        let mut catalog_stats = CatalogStats::new();
        catalog_stats.insert(
            TableReference::table("a"),
            make_stats_with_col(1000, "k", 1000),
        );
        catalog_stats.insert(TableReference::table("b"), make_stats_with_col(50, "k", 50));
        let plan = LogicalPlan::Join {
            left: Box::new(make_scan("a", vec!["k"])),
            right: Box::new(make_scan("b", vec!["k"])),
            join_type: JoinType::Left,
            condition: JoinCondition::On(eq(col_expr(0, "k"), col_expr(0, "k"))),
            dynamic_filter_ids: Vec::new(),
        };
        let cost = estimated_cardinality(&plan, &catalog_stats);
        // inner = (1000 * 50) / 1000 = 50; left = max(1000, 50) = 1000
        assert_eq!(cost, 1000.0);
    }

    #[test]
    fn full_join_is_sum_of_sides_and_inner() {
        let mut catalog_stats = CatalogStats::new();
        catalog_stats.insert(
            TableReference::table("a"),
            make_stats_with_col(100, "k", 100),
        );
        catalog_stats.insert(TableReference::table("b"), make_stats_with_col(50, "k", 50));
        let plan = LogicalPlan::Join {
            left: Box::new(make_scan("a", vec!["k"])),
            right: Box::new(make_scan("b", vec!["k"])),
            join_type: JoinType::Full,
            condition: JoinCondition::On(eq(col_expr(0, "k"), col_expr(0, "k"))),
            dynamic_filter_ids: Vec::new(),
        };
        let cost = estimated_cardinality(&plan, &catalog_stats);
        // inner = (100*50)/100 = 50; full = 100 + 50 + 50 = 200
        assert_eq!(cost, 200.0);
    }

    // -- Aggregate --

    #[test]
    fn global_aggregate_is_one_row() {
        let mut catalog_stats = CatalogStats::new();
        catalog_stats.insert(TableReference::table("lineitem"), make_stats(6_000_000));
        let plan = LogicalPlan::Aggregate {
            input: Box::new(make_scan("lineitem", vec!["x"])),
            group_by: vec![],
            aggr_exprs: vec![],
            schema: vec![col("count")],
        };
        let cost = estimated_cardinality(&plan, &catalog_stats);
        assert_eq!(cost, 1.0);
    }

    #[test]
    fn grouped_aggregate_uses_ndv_product() {
        let mut catalog_stats = CatalogStats::new();
        let mut stats = make_stats(10_000_000);
        stats.columns.insert(
            "col_a".to_string(),
            ColumnStatistics {
                ndv: Some(25),
                ..ColumnStatistics::default()
            },
        );
        stats.columns.insert(
            "col_b".to_string(),
            ColumnStatistics {
                ndv: Some(100),
                ..ColumnStatistics::default()
            },
        );
        catalog_stats.insert(TableReference::table("t"), stats);
        let plan = LogicalPlan::Aggregate {
            input: Box::new(make_scan("t", vec!["col_a", "col_b"])),
            group_by: vec![col_expr(0, "col_a"), col_expr(1, "col_b")],
            aggr_exprs: vec![],
            schema: vec![col("col_a"), col("col_b")],
        };
        let cost = estimated_cardinality(&plan, &catalog_stats);
        // min(10M, 25 * 100) = 2500
        assert_eq!(cost, 2500.0);
    }

    // -- Distinct --

    #[test]
    fn distinct_is_passthrough_default() {
        let mut catalog_stats = CatalogStats::new();
        catalog_stats.insert(TableReference::table("orders"), make_stats(1_500_000));
        let plan = LogicalPlan::Distinct {
            input: Box::new(make_scan("orders", vec!["x"])),
        };
        let cost = estimated_cardinality(&plan, &catalog_stats);
        assert_eq!(cost, 1_500_000.0);
    }

    // -- UnionAll / Intersect / Except --

    #[test]
    fn union_all_sums_branches() {
        let mut catalog_stats = CatalogStats::new();
        catalog_stats.insert(TableReference::table("a"), make_stats(100));
        catalog_stats.insert(TableReference::table("b"), make_stats(50));
        let plan = LogicalPlan::UnionAll {
            inputs: vec![make_scan("a", vec!["x"]), make_scan("b", vec!["x"])],
        };
        let cost = estimated_cardinality(&plan, &catalog_stats);
        assert_eq!(cost, 150.0);
    }

    #[test]
    fn intersect_takes_min() {
        let mut catalog_stats = CatalogStats::new();
        catalog_stats.insert(TableReference::table("a"), make_stats(100));
        catalog_stats.insert(TableReference::table("b"), make_stats(50));
        let plan = LogicalPlan::Intersect {
            left: Box::new(make_scan("a", vec!["x"])),
            right: Box::new(make_scan("b", vec!["x"])),
        };
        let cost = estimated_cardinality(&plan, &catalog_stats);
        assert_eq!(cost, 50.0);
    }

    #[test]
    fn except_returns_left_size() {
        let mut catalog_stats = CatalogStats::new();
        catalog_stats.insert(TableReference::table("a"), make_stats(100));
        catalog_stats.insert(TableReference::table("b"), make_stats(50));
        let plan = LogicalPlan::Except {
            left: Box::new(make_scan("a", vec!["x"])),
            right: Box::new(make_scan("b", vec!["x"])),
        };
        let cost = estimated_cardinality(&plan, &catalog_stats);
        assert_eq!(cost, 100.0);
    }

    // -- Semi/Anti join --

    #[test]
    fn semi_join_is_half_of_left() {
        let mut catalog_stats = CatalogStats::new();
        catalog_stats.insert(TableReference::table("a"), make_stats(1000));
        catalog_stats.insert(TableReference::table("b"), make_stats(50));
        let plan = LogicalPlan::SemiJoin {
            left: Box::new(make_scan("a", vec!["k"])),
            right: Box::new(make_scan("b", vec!["k"])),
            left_key: col_expr(0, "k"),
            right_key: col_expr(0, "k"),
            residual: None,
            dynamic_filter_ids: Vec::new(),
        };
        let cost = estimated_cardinality(&plan, &catalog_stats);
        assert_eq!(cost, 500.0);
    }

    // -- Scalar subquery --

    #[test]
    fn scalar_subquery_caps_at_one() {
        let mut catalog_stats = CatalogStats::new();
        catalog_stats.insert(TableReference::table("orders"), make_stats(1_500_000));
        let plan = LogicalPlan::ScalarSubquery {
            subplan: Box::new(make_scan("orders", vec!["x"])),
        };
        let cost = estimated_cardinality(&plan, &catalog_stats);
        assert_eq!(cost, 1.0);
    }

    // -- CatalogStats API --

    #[test]
    fn catalog_stats_empty_lookup_returns_none() {
        let stats = CatalogStats::new();
        assert!(stats.get(&TableReference::table("missing")).is_none());
        assert!(stats.is_empty());
        assert_eq!(stats.len(), 0);
    }

    #[test]
    fn catalog_stats_insert_and_get() {
        let mut stats = CatalogStats::new();
        stats.insert(TableReference::table("t"), make_stats(123));
        assert_eq!(stats.len(), 1);
        let s = stats.get(&TableReference::table("t")).unwrap();
        assert_eq!(s.row_count, Some(123));
    }

    // -- Robustness: always finite and >= 1 --

    #[test]
    fn cost_is_finite_for_empty_stats() {
        let catalog_stats = CatalogStats::new();
        // Build a moderately complex plan with no stats at all.
        let scan_a = make_scan("a", vec!["k"]);
        let scan_b = make_scan("b", vec!["k"]);
        let join = LogicalPlan::Join {
            left: Box::new(scan_a),
            right: Box::new(scan_b),
            join_type: JoinType::Inner,
            condition: JoinCondition::On(eq(col_expr(0, "k"), col_expr(0, "k"))),
            dynamic_filter_ids: Vec::new(),
        };
        let filter = LogicalPlan::Filter {
            input: Box::new(join),
            predicate: eq(col_expr(0, "k"), lit(ScalarValue::Int64(0))),
        };
        let aggregate = LogicalPlan::Aggregate {
            input: Box::new(filter),
            group_by: vec![col_expr(0, "k")],
            aggr_exprs: vec![],
            schema: vec![col("k")],
        };
        let cost = estimated_cardinality(&aggregate, &catalog_stats);
        assert!(cost.is_finite());
        assert!(cost >= 1.0);
    }

    #[test]
    fn cost_clamps_negative_to_one() {
        // Sanity: even if a future variant accidentally produces a
        // negative intermediate, the public function clamps.
        assert_eq!(clamp(-100.0), 1.0);
        assert_eq!(clamp(f64::NAN), 1.0);
        assert_eq!(clamp(f64::INFINITY), 1.0);
        assert_eq!(clamp(0.5), 1.0);
        assert_eq!(clamp(42.0), 42.0);
    }
}
