//! Plan fragmentation for distributed execution.
//!
//! Splits an optimized [`LogicalPlan`] into distributable fragments separated
//! by exchange boundaries. Each fragment can execute independently on a worker.

use std::fmt;
use std::sync::OnceLock;

use arneb_common::identifiers::StageId;
use arneb_common::types::{ColumnInfo, DataType};

use crate::analyzer::assign_dynamic_filter_ids::assign_for_join;
use crate::dynamic_filter::DynamicFilterIdAllocator;
use crate::plan::{DynamicFilterProducer, JoinCondition, LogicalPlan, PlanExpr};
use arneb_sql_parser::ast::BinaryOp;

// A.4 (2026-05-20): the property-derivation visitor lives in
// `crate::properties` and is used inside the `Join` arm below to skip
// redundant repartitions when a child fragment is already partitioned
// on the join key (possibly via column equivalence).

/// W3-Hash.4: Walk a JOIN's ON expression to find equi-key pairs that
/// split cleanly across the two inputs. Returns `(left_keys, right_keys)`
/// as 0-based column indices in each input's schema. The right indices
/// are returned RELATIVE to the right schema (after subtracting
/// `left_col_count`).
///
/// Returns `None` when the condition is `JoinCondition::None`, when no
/// pure column-to-column equality predicates appear, or when the
/// expression mixes residuals in a way we don't recognize. The fragmenter
/// uses this strictly as a hint: missing equi-keys just means "don't
/// partition this join".
pub(crate) fn extract_partitioning_equi_keys(
    condition: &JoinCondition,
    left_col_count: usize,
) -> Option<(Vec<usize>, Vec<usize>)> {
    let JoinCondition::On(expr) = condition else {
        return None;
    };
    let mut left_keys = Vec::new();
    let mut right_keys = Vec::new();
    collect_equi_pairs(expr, left_col_count, &mut left_keys, &mut right_keys);
    if left_keys.is_empty() {
        None
    } else {
        Some((left_keys, right_keys))
    }
}

fn collect_equi_pairs(
    expr: &PlanExpr,
    left_col_count: usize,
    left_keys: &mut Vec<usize>,
    right_keys: &mut Vec<usize>,
) {
    match expr {
        PlanExpr::BinaryOp {
            left,
            op: BinaryOp::And,
            right,
            ..
        } => {
            collect_equi_pairs(left, left_col_count, left_keys, right_keys);
            collect_equi_pairs(right, left_col_count, left_keys, right_keys);
        }
        PlanExpr::BinaryOp {
            left,
            op: BinaryOp::Eq,
            right,
            ..
        } => {
            if let (PlanExpr::Column { index: l, .. }, PlanExpr::Column { index: r, .. }) =
                (left.as_ref(), right.as_ref())
            {
                if *l < left_col_count && *r >= left_col_count {
                    left_keys.push(*l);
                    right_keys.push(*r - left_col_count);
                } else if *r < left_col_count && *l >= left_col_count {
                    left_keys.push(*r);
                    right_keys.push(*l - left_col_count);
                }
            }
        }
        _ => {}
    }
}

fn extract_pure_equi_join_keys(
    condition: &JoinCondition,
    left_col_count: usize,
) -> Option<(Vec<usize>, Vec<usize>)> {
    let JoinCondition::On(expr) = condition else {
        return None;
    };
    let mut left_keys = Vec::new();
    let mut right_keys = Vec::new();
    if collect_pure_equi_pairs(expr, left_col_count, &mut left_keys, &mut right_keys)
        && !left_keys.is_empty()
    {
        Some((left_keys, right_keys))
    } else {
        None
    }
}

fn collect_pure_equi_pairs(
    expr: &PlanExpr,
    left_col_count: usize,
    left_keys: &mut Vec<usize>,
    right_keys: &mut Vec<usize>,
) -> bool {
    match expr {
        PlanExpr::BinaryOp {
            left,
            op: BinaryOp::And,
            right,
            ..
        } => {
            collect_pure_equi_pairs(left, left_col_count, left_keys, right_keys)
                && collect_pure_equi_pairs(right, left_col_count, left_keys, right_keys)
        }
        PlanExpr::BinaryOp {
            left,
            op: BinaryOp::Eq,
            right,
            ..
        } => {
            if let (PlanExpr::Column { index: l, .. }, PlanExpr::Column { index: r, .. }) =
                (left.as_ref(), right.as_ref())
            {
                if *l < left_col_count && *r >= left_col_count {
                    left_keys.push(*l);
                    right_keys.push(*r - left_col_count);
                    return true;
                }
                if *r < left_col_count && *l >= left_col_count {
                    left_keys.push(*r);
                    right_keys.push(*l - left_col_count);
                    return true;
                }
            }
            false
        }
        _ => false,
    }
}

fn is_fold_safe_cast_projection(exprs: &[PlanExpr], schema: &[ColumnInfo]) -> bool {
    exprs.len() == schema.len()
        && exprs.iter().enumerate().all(|(i, expr)| match expr {
            PlanExpr::Column { index, .. } => *index == i,
            PlanExpr::Cast { expr, .. } => {
                matches!(expr.as_ref(), PlanExpr::Column { index, .. } if *index == i)
            }
            _ => false,
        })
}

fn simple_column_projection_indices(exprs: &[PlanExpr]) -> Option<Vec<usize>> {
    exprs
        .iter()
        .map(|expr| match expr {
            PlanExpr::Column { index, .. } => Some(*index),
            _ => None,
        })
        .collect()
}

fn remap_fragment_partitioning_for_projection(
    partitioning: &PartitioningScheme,
    projection: &[usize],
) -> Option<PartitioningScheme> {
    match partitioning {
        PartitioningScheme::Hash {
            columns,
            partition_count,
        } => {
            let mut remapped = Vec::with_capacity(columns.len());
            for column in columns {
                remapped.push(
                    projection
                        .iter()
                        .position(|projected| projected == column)?,
                );
            }
            Some(PartitioningScheme::Hash {
                columns: remapped,
                partition_count: *partition_count,
            })
        }
        PartitioningScheme::RoundRobin => Some(PartitioningScheme::RoundRobin),
        PartitioningScheme::Single => Some(PartitioningScheme::Single),
        PartitioningScheme::Broadcast => Some(PartitioningScheme::Broadcast),
    }
}

#[cfg(test)]
static MINIMAL_JOIN_CARRY_FRAGMENT_FOLD_TEST_OVERRIDE: OnceLock<std::sync::Mutex<Option<bool>>> =
    OnceLock::new();
#[cfg(test)]
static MINIMAL_JOIN_CARRY_FRAGMENT_FOLD_TEST_LOCK: OnceLock<std::sync::Mutex<()>> = OnceLock::new();

#[cfg(test)]
struct MinimalJoinCarryFragmentFoldOverride {
    _guard: std::sync::MutexGuard<'static, ()>,
}

#[cfg(test)]
impl Drop for MinimalJoinCarryFragmentFoldOverride {
    fn drop(&mut self) {
        *MINIMAL_JOIN_CARRY_FRAGMENT_FOLD_TEST_OVERRIDE
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("MINIMAL_JOIN_CARRY_FRAGMENT_FOLD_TEST_OVERRIDE mutex poisoned") = None;
    }
}

#[cfg(test)]
fn set_minimal_join_carry_fragment_fold_for_test(
    enabled: bool,
) -> MinimalJoinCarryFragmentFoldOverride {
    let guard = MINIMAL_JOIN_CARRY_FRAGMENT_FOLD_TEST_LOCK
        .get_or_init(|| std::sync::Mutex::new(()))
        .lock()
        .expect("MINIMAL_JOIN_CARRY_FRAGMENT_FOLD_TEST_LOCK mutex poisoned");
    *MINIMAL_JOIN_CARRY_FRAGMENT_FOLD_TEST_OVERRIDE
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("MINIMAL_JOIN_CARRY_FRAGMENT_FOLD_TEST_OVERRIDE mutex poisoned") = Some(enabled);
    MinimalJoinCarryFragmentFoldOverride { _guard: guard }
}

fn minimal_join_carry_fragment_fold_enabled() -> bool {
    #[cfg(test)]
    if let Some(override_value) = MINIMAL_JOIN_CARRY_FRAGMENT_FOLD_TEST_OVERRIDE
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("MINIMAL_JOIN_CARRY_FRAGMENT_FOLD_TEST_OVERRIDE mutex poisoned")
        .as_ref()
    {
        return *override_value;
    }

    std::env::var("ARNEB_MINIMAL_JOIN_CARRY")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true") || v.eq_ignore_ascii_case("on"))
        .unwrap_or(false)
}

fn broadcast_build_side_swap_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();

    *ENABLED.get_or_init(|| {
        let enabled = std::env::var("ARNEB_BROADCAST_BUILD_SIDE_SWAP")
            .map(|v| v != "0" && !v.is_empty())
            .unwrap_or(true);
        tracing::info!(
            target: "arneb::config",
            broadcast_build_side_swap = enabled,
            "ARNEB_BROADCAST_BUILD_SIDE_SWAP effective value (default on; =0 to disable)"
        );
        enabled
    })
}

#[cfg(test)]
static PARTITIONED_BUILD_SIDE_SWAP_TEST_OVERRIDE: OnceLock<std::sync::Mutex<Option<(bool, f64)>>> =
    OnceLock::new();
#[cfg(test)]
static PARTITIONED_BUILD_SIDE_SWAP_TEST_LOCK: OnceLock<std::sync::Mutex<()>> = OnceLock::new();

#[cfg(test)]
struct PartitionedBuildSideSwapOverride {
    _guard: std::sync::MutexGuard<'static, ()>,
}

#[cfg(test)]
impl Drop for PartitionedBuildSideSwapOverride {
    fn drop(&mut self) {
        *PARTITIONED_BUILD_SIDE_SWAP_TEST_OVERRIDE
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("PARTITIONED_BUILD_SIDE_SWAP_TEST_OVERRIDE mutex poisoned") = None;
    }
}

#[cfg(test)]
fn set_partitioned_build_side_swap_for_test(
    enabled: bool,
    factor: f64,
) -> PartitionedBuildSideSwapOverride {
    let guard = PARTITIONED_BUILD_SIDE_SWAP_TEST_LOCK
        .get_or_init(|| std::sync::Mutex::new(()))
        .lock()
        .expect("PARTITIONED_BUILD_SIDE_SWAP_TEST_LOCK mutex poisoned");
    *PARTITIONED_BUILD_SIDE_SWAP_TEST_OVERRIDE
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("PARTITIONED_BUILD_SIDE_SWAP_TEST_OVERRIDE mutex poisoned") =
        Some((enabled, factor));
    PartitionedBuildSideSwapOverride { _guard: guard }
}

fn partitioned_build_side_swap_enabled() -> bool {
    #[cfg(test)]
    if let Some((enabled, _)) = PARTITIONED_BUILD_SIDE_SWAP_TEST_OVERRIDE
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("PARTITIONED_BUILD_SIDE_SWAP_TEST_OVERRIDE mutex poisoned")
        .as_ref()
    {
        return *enabled;
    }

    static ENABLED: OnceLock<bool> = OnceLock::new();

    *ENABLED.get_or_init(|| {
        let enabled = std::env::var("ARNEB_PARTITIONED_BUILD_SIDE_SWAP")
            .map(|v| v != "0" && !v.is_empty())
            .unwrap_or(false);
        tracing::info!(
            target: "arneb::config",
            partitioned_build_side_swap = enabled,
            "ARNEB_PARTITIONED_BUILD_SIDE_SWAP effective value (default off; set non-zero to enable)"
        );
        enabled
    })
}

fn partitioned_build_side_swap_factor() -> f64 {
    #[cfg(test)]
    if let Some((_, factor)) = PARTITIONED_BUILD_SIDE_SWAP_TEST_OVERRIDE
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("PARTITIONED_BUILD_SIDE_SWAP_TEST_OVERRIDE mutex poisoned")
        .as_ref()
    {
        return *factor;
    }

    static FACTOR: OnceLock<f64> = OnceLock::new();

    *FACTOR.get_or_init(|| {
        let factor = std::env::var("ARNEB_PARTITIONED_BUILD_SIDE_SWAP_FACTOR")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v >= 1.0)
            .unwrap_or(100.0);
        tracing::info!(
            target: "arneb::config",
            partitioned_build_side_swap_factor = factor,
            "ARNEB_PARTITIONED_BUILD_SIDE_SWAP_FACTOR effective value (default 100)"
        );
        factor
    })
}

fn all_scans_have_row_count(plan: &LogicalPlan, stats: &crate::cost::CatalogStats) -> bool {
    match plan {
        LogicalPlan::TableScan { table, .. } => {
            stats.get(table).and_then(|s| s.row_count).is_some()
        }
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Projection { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Explain { input, .. }
        | LogicalPlan::Distinct { input }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::PartialAggregate { input, .. }
        | LogicalPlan::FinalAggregate { input, .. }
        | LogicalPlan::Window { input, .. }
        | LogicalPlan::AssignUniqueId { input, .. }
        | LogicalPlan::ScalarSubquery { subplan: input } => all_scans_have_row_count(input, stats),
        LogicalPlan::Join { left, right, .. }
        | LogicalPlan::SemiJoin { left, right, .. }
        | LogicalPlan::AntiJoin { left, right, .. }
        | LogicalPlan::Intersect { left, right }
        | LogicalPlan::Except { left, right } => {
            all_scans_have_row_count(left, stats) && all_scans_have_row_count(right, stats)
        }
        LogicalPlan::UnionAll { inputs } => inputs
            .iter()
            .all(|input| all_scans_have_row_count(input, stats)),
        LogicalPlan::CreateTableAsSelect { source, .. }
        | LogicalPlan::InsertInto { source, .. }
        | LogicalPlan::CreateView { plan: source, .. } => all_scans_have_row_count(source, stats),
        LogicalPlan::ExchangeNode { .. } => false,
        LogicalPlan::OneRow
        | LogicalPlan::CreateTable { .. }
        | LogicalPlan::DropTable { .. }
        | LogicalPlan::DeleteFrom { .. }
        | LogicalPlan::DropView { .. } => true,
    }
}

fn partitioned_build_side_swap_estimates(
    left: &LogicalPlan,
    right: &LogicalPlan,
    stats: &crate::cost::CatalogStats,
    factor: f64,
) -> Option<(f64, f64)> {
    if !all_scans_have_row_count(left, stats) || !all_scans_have_row_count(right, stats) {
        return None;
    }
    let left_rows = crate::cost::estimated_cardinality(left, stats);
    let right_rows = crate::cost::estimated_cardinality(right, stats);
    if left_rows.is_finite()
        && right_rows.is_finite()
        && left_rows > 0.0
        && right_rows > 0.0
        && right_rows >= left_rows * factor
    {
        Some((left_rows, right_rows))
    } else {
        None
    }
}

#[cfg(test)]
static PARALLEL_FINAL_AGG_TEST_OVERRIDE: OnceLock<std::sync::Mutex<Option<bool>>> = OnceLock::new();
#[cfg(test)]
static PARALLEL_FINAL_AGG_TEST_LOCK: OnceLock<std::sync::Mutex<()>> = OnceLock::new();
#[cfg(test)]
static PARTIAL_AGG_OVER_JOIN_TEST_OVERRIDE: OnceLock<std::sync::Mutex<Option<bool>>> =
    OnceLock::new();
#[cfg(test)]
static PARTIAL_AGG_OVER_JOIN_TEST_LOCK: OnceLock<std::sync::Mutex<()>> = OnceLock::new();

#[cfg(test)]
struct ParallelFinalAggOverride {
    _guard: std::sync::MutexGuard<'static, ()>,
}

#[cfg(test)]
impl Drop for ParallelFinalAggOverride {
    fn drop(&mut self) {
        *PARALLEL_FINAL_AGG_TEST_OVERRIDE
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("PARALLEL_FINAL_AGG_TEST_OVERRIDE mutex poisoned") = None;
    }
}

#[cfg(test)]
fn set_parallel_final_agg_for_test(enabled: bool) -> ParallelFinalAggOverride {
    let guard = PARALLEL_FINAL_AGG_TEST_LOCK
        .get_or_init(|| std::sync::Mutex::new(()))
        .lock()
        .expect("PARALLEL_FINAL_AGG_TEST_LOCK mutex poisoned");
    *PARALLEL_FINAL_AGG_TEST_OVERRIDE
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("PARALLEL_FINAL_AGG_TEST_OVERRIDE mutex poisoned") = Some(enabled);
    ParallelFinalAggOverride { _guard: guard }
}

fn parallel_final_agg_enabled() -> bool {
    #[cfg(test)]
    if let Some(override_value) = PARALLEL_FINAL_AGG_TEST_OVERRIDE
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("PARALLEL_FINAL_AGG_TEST_OVERRIDE mutex poisoned")
        .as_ref()
    {
        return *override_value;
    }

    static ENABLED: OnceLock<bool> = OnceLock::new();

    *ENABLED.get_or_init(|| {
        let enabled = std::env::var("ARNEB_PARALLEL_FINAL_AGG")
            .map(|v| v != "0" && !v.is_empty())
            .unwrap_or(false);
        tracing::info!(
            target: "arneb::config",
            parallel_final_agg = enabled,
            "ARNEB_PARALLEL_FINAL_AGG effective value (default off; =1 to enable hash-partitioned final aggregation)"
        );
        enabled
    })
}

#[cfg(test)]
struct PartialAggOverJoinOverride {
    _guard: std::sync::MutexGuard<'static, ()>,
}

#[cfg(test)]
impl Drop for PartialAggOverJoinOverride {
    fn drop(&mut self) {
        *PARTIAL_AGG_OVER_JOIN_TEST_OVERRIDE
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("PARTIAL_AGG_OVER_JOIN_TEST_OVERRIDE mutex poisoned") = None;
    }
}

#[cfg(test)]
fn set_partial_agg_over_join_for_test(enabled: bool) -> PartialAggOverJoinOverride {
    let guard = PARTIAL_AGG_OVER_JOIN_TEST_LOCK
        .get_or_init(|| std::sync::Mutex::new(()))
        .lock()
        .expect("PARTIAL_AGG_OVER_JOIN_TEST_LOCK mutex poisoned");
    *PARTIAL_AGG_OVER_JOIN_TEST_OVERRIDE
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("PARTIAL_AGG_OVER_JOIN_TEST_OVERRIDE mutex poisoned") = Some(enabled);
    PartialAggOverJoinOverride { _guard: guard }
}

fn partial_agg_over_join_enabled() -> bool {
    #[cfg(test)]
    if let Some(override_value) = PARTIAL_AGG_OVER_JOIN_TEST_OVERRIDE
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("PARTIAL_AGG_OVER_JOIN_TEST_OVERRIDE mutex poisoned")
        .as_ref()
    {
        return *override_value;
    }

    static ENABLED: OnceLock<bool> = OnceLock::new();

    *ENABLED.get_or_init(|| {
        let enabled = std::env::var("ARNEB_PARTIAL_AGG_OVER_JOIN")
            .map(|v| v != "0" && !v.is_empty())
            .unwrap_or(false);
        tracing::info!(
            target: "arneb::config",
            partial_agg_over_join = enabled,
            "ARNEB_PARTIAL_AGG_OVER_JOIN effective value (default off; =1 to fuse partial aggregation into join-output fragments)"
        );
        enabled
    })
}

#[cfg(test)]
static PARTITIONED_SEMI_JOIN_TEST_OVERRIDE: OnceLock<std::sync::Mutex<Option<bool>>> =
    OnceLock::new();
#[cfg(test)]
static PARTITIONED_SEMI_JOIN_TEST_LOCK: OnceLock<std::sync::Mutex<()>> = OnceLock::new();

#[cfg(test)]
struct PartitionedSemiJoinOverride {
    _guard: std::sync::MutexGuard<'static, ()>,
}

#[cfg(test)]
impl Drop for PartitionedSemiJoinOverride {
    fn drop(&mut self) {
        *PARTITIONED_SEMI_JOIN_TEST_OVERRIDE
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("PARTITIONED_SEMI_JOIN_TEST_OVERRIDE mutex poisoned") = None;
    }
}

#[cfg(test)]
fn set_partitioned_semi_join_for_test(enabled: bool) -> PartitionedSemiJoinOverride {
    let guard = PARTITIONED_SEMI_JOIN_TEST_LOCK
        .get_or_init(|| std::sync::Mutex::new(()))
        .lock()
        .expect("PARTITIONED_SEMI_JOIN_TEST_LOCK mutex poisoned");
    *PARTITIONED_SEMI_JOIN_TEST_OVERRIDE
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("PARTITIONED_SEMI_JOIN_TEST_OVERRIDE mutex poisoned") = Some(enabled);
    PartitionedSemiJoinOverride { _guard: guard }
}

fn partitioned_semi_join_enabled() -> bool {
    #[cfg(test)]
    if let Some(override_value) = PARTITIONED_SEMI_JOIN_TEST_OVERRIDE
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("PARTITIONED_SEMI_JOIN_TEST_OVERRIDE mutex poisoned")
        .as_ref()
    {
        return *override_value;
    }

    static ENABLED: OnceLock<bool> = OnceLock::new();

    *ENABLED.get_or_init(|| {
        let enabled = std::env::var("ARNEB_PARTITIONED_SEMI_JOIN")
            .map(|v| v != "0" && !v.is_empty())
            .unwrap_or(false);
        tracing::info!(
            target: "arneb::config",
            partitioned_semi_join = enabled,
            "ARNEB_PARTITIONED_SEMI_JOIN effective value (default off; =1 to enable hash-partitioned semi/anti joins)"
        );
        enabled
    })
}

#[cfg(test)]
static SWAP_DF_REGEN_TEST_OVERRIDE: OnceLock<std::sync::Mutex<Option<bool>>> = OnceLock::new();
#[cfg(test)]
static SWAP_DF_REGEN_TEST_LOCK: OnceLock<std::sync::Mutex<()>> = OnceLock::new();

#[cfg(test)]
struct SwapDfRegenOverride {
    _guard: std::sync::MutexGuard<'static, ()>,
}

#[cfg(test)]
impl Drop for SwapDfRegenOverride {
    fn drop(&mut self) {
        *SWAP_DF_REGEN_TEST_OVERRIDE
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("SWAP_DF_REGEN_TEST_OVERRIDE mutex poisoned") = None;
    }
}

#[cfg(test)]
fn set_swap_df_regen_for_test(enabled: bool) -> SwapDfRegenOverride {
    let guard = SWAP_DF_REGEN_TEST_LOCK
        .get_or_init(|| std::sync::Mutex::new(()))
        .lock()
        .expect("SWAP_DF_REGEN_TEST_LOCK mutex poisoned");
    *SWAP_DF_REGEN_TEST_OVERRIDE
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("SWAP_DF_REGEN_TEST_OVERRIDE mutex poisoned") = Some(enabled);
    SwapDfRegenOverride { _guard: guard }
}

fn swap_df_regen_enabled() -> bool {
    #[cfg(test)]
    if let Some(override_value) = SWAP_DF_REGEN_TEST_OVERRIDE
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("SWAP_DF_REGEN_TEST_OVERRIDE mutex poisoned")
        .as_ref()
    {
        return *override_value;
    }

    static ENABLED: OnceLock<bool> = OnceLock::new();

    *ENABLED.get_or_init(|| {
        let enabled = std::env::var("ARNEB_SWAP_DF_REGEN").is_ok_and(|v| v == "1");
        tracing::info!(
            target: "arneb::config",
            ARNEB_SWAP_DF_REGEN = enabled,
            "broadcast build-side swap dynamic-filter regeneration configured"
        );
        enabled
    })
}

fn remap_swapped_join_column_index(
    old_left_width: usize,
    old_right_width: usize,
    x: usize,
) -> usize {
    if x < old_left_width {
        x + old_right_width
    } else {
        x - old_left_width
    }
}

fn remap_expr_for_swapped_join(
    expr: PlanExpr,
    old_left_width: usize,
    old_right_width: usize,
) -> PlanExpr {
    match expr {
        PlanExpr::Column { index, name, span } => PlanExpr::Column {
            index: remap_swapped_join_column_index(old_left_width, old_right_width, index),
            name,
            span,
        },
        PlanExpr::Literal { value, span } => PlanExpr::Literal { value, span },
        PlanExpr::BinaryOp {
            left,
            op,
            right,
            span,
        } => PlanExpr::BinaryOp {
            left: Box::new(remap_expr_for_swapped_join(
                *left,
                old_left_width,
                old_right_width,
            )),
            op,
            right: Box::new(remap_expr_for_swapped_join(
                *right,
                old_left_width,
                old_right_width,
            )),
            span,
        },
        PlanExpr::UnaryOp { op, expr, span } => PlanExpr::UnaryOp {
            op,
            expr: Box::new(remap_expr_for_swapped_join(
                *expr,
                old_left_width,
                old_right_width,
            )),
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
                .map(|arg| remap_expr_for_swapped_join(arg, old_left_width, old_right_width))
                .collect(),
            distinct,
            span,
        },
        PlanExpr::IsNull { expr, span } => PlanExpr::IsNull {
            expr: Box::new(remap_expr_for_swapped_join(
                *expr,
                old_left_width,
                old_right_width,
            )),
            span,
        },
        PlanExpr::IsNotNull { expr, span } => PlanExpr::IsNotNull {
            expr: Box::new(remap_expr_for_swapped_join(
                *expr,
                old_left_width,
                old_right_width,
            )),
            span,
        },
        PlanExpr::Between {
            expr,
            negated,
            low,
            high,
            span,
        } => PlanExpr::Between {
            expr: Box::new(remap_expr_for_swapped_join(
                *expr,
                old_left_width,
                old_right_width,
            )),
            negated,
            low: Box::new(remap_expr_for_swapped_join(
                *low,
                old_left_width,
                old_right_width,
            )),
            high: Box::new(remap_expr_for_swapped_join(
                *high,
                old_left_width,
                old_right_width,
            )),
            span,
        },
        PlanExpr::InList {
            expr,
            list,
            negated,
            span,
        } => PlanExpr::InList {
            expr: Box::new(remap_expr_for_swapped_join(
                *expr,
                old_left_width,
                old_right_width,
            )),
            list: list
                .into_iter()
                .map(|item| remap_expr_for_swapped_join(item, old_left_width, old_right_width))
                .collect(),
            negated,
            span,
        },
        PlanExpr::Cast {
            expr,
            data_type,
            span,
        } => PlanExpr::Cast {
            expr: Box::new(remap_expr_for_swapped_join(
                *expr,
                old_left_width,
                old_right_width,
            )),
            data_type,
            span,
        },
        PlanExpr::Wildcard => PlanExpr::Wildcard,
        PlanExpr::ScalarSubquery { subplan, span } => PlanExpr::ScalarSubquery { subplan, span },
        PlanExpr::CaseExpr {
            operand,
            when_clauses,
            else_result,
            span,
        } => PlanExpr::CaseExpr {
            operand: operand.map(|expr| {
                Box::new(remap_expr_for_swapped_join(
                    *expr,
                    old_left_width,
                    old_right_width,
                ))
            }),
            when_clauses: when_clauses
                .into_iter()
                .map(|(when, then)| {
                    (
                        remap_expr_for_swapped_join(when, old_left_width, old_right_width),
                        remap_expr_for_swapped_join(then, old_left_width, old_right_width),
                    )
                })
                .collect(),
            else_result: else_result.map(|expr| {
                Box::new(remap_expr_for_swapped_join(
                    *expr,
                    old_left_width,
                    old_right_width,
                ))
            }),
            span,
        },
        PlanExpr::Parameter {
            index,
            type_hint,
            span,
        } => PlanExpr::Parameter {
            index,
            type_hint,
            span,
        },
    }
}

fn remap_join_condition_for_swapped_join(
    condition: JoinCondition,
    old_left_width: usize,
    old_right_width: usize,
) -> JoinCondition {
    match condition {
        JoinCondition::On(expr) => JoinCondition::On(remap_expr_for_swapped_join(
            expr,
            old_left_width,
            old_right_width,
        )),
        JoinCondition::None => JoinCondition::None,
    }
}

#[derive(Clone)]
struct SwappedJoinOutputRestore {
    old_left_width: usize,
    old_right_width: usize,
    original_schema: Vec<ColumnInfo>,
}

fn restore_swapped_join_output(
    join_plan: LogicalPlan,
    restore: Option<&SwappedJoinOutputRestore>,
) -> LogicalPlan {
    let Some(restore) = restore else {
        return join_plan;
    };

    let mut exprs = Vec::with_capacity(restore.original_schema.len());
    for old_left_idx in 0..restore.old_left_width {
        let index = restore.old_right_width + old_left_idx;
        exprs.push(PlanExpr::Column {
            index,
            name: restore.original_schema[old_left_idx].name.clone(),
            span: None,
        });
    }
    for old_right_idx in 0..restore.old_right_width {
        let schema_idx = restore.old_left_width + old_right_idx;
        exprs.push(PlanExpr::Column {
            index: old_right_idx,
            name: restore.original_schema[schema_idx].name.clone(),
            span: None,
        });
    }

    LogicalPlan::Projection {
        input: Box::new(join_plan),
        exprs,
        schema: restore.original_schema.clone(),
    }
}

fn remove_dynamic_filter_consumers_for_join(
    plan: LogicalPlan,
    producers: &[DynamicFilterProducer],
) -> LogicalPlan {
    if producers.is_empty() {
        return plan;
    }
    use LogicalPlan as L;
    match plan {
        L::TableScan {
            table,
            schema,
            alias,
            properties,
            mut dynamic_filters_consumed,
        } => {
            dynamic_filters_consumed
                .retain(|consumer| !producers.iter().any(|producer| producer.id == consumer.id));
            L::TableScan {
                table,
                schema,
                alias,
                properties,
                dynamic_filters_consumed,
            }
        }
        L::Projection {
            input,
            exprs,
            schema,
        } => L::Projection {
            input: Box::new(remove_dynamic_filter_consumers_for_join(*input, producers)),
            exprs,
            schema,
        },
        L::Filter { input, predicate } => L::Filter {
            input: Box::new(remove_dynamic_filter_consumers_for_join(*input, producers)),
            predicate,
        },
        L::Join {
            left,
            right,
            join_type,
            condition,
            dynamic_filter_ids,
        } => L::Join {
            left: Box::new(remove_dynamic_filter_consumers_for_join(*left, producers)),
            right: Box::new(remove_dynamic_filter_consumers_for_join(*right, producers)),
            join_type,
            condition,
            dynamic_filter_ids,
        },
        L::Aggregate {
            input,
            group_by,
            aggr_exprs,
            schema,
        } => L::Aggregate {
            input: Box::new(remove_dynamic_filter_consumers_for_join(*input, producers)),
            group_by,
            aggr_exprs,
            schema,
        },
        L::Sort { input, order_by } => L::Sort {
            input: Box::new(remove_dynamic_filter_consumers_for_join(*input, producers)),
            order_by,
        },
        L::Limit {
            input,
            limit,
            offset,
        } => L::Limit {
            input: Box::new(remove_dynamic_filter_consumers_for_join(*input, producers)),
            limit,
            offset,
        },
        L::Explain { input, analyze } => L::Explain {
            input: Box::new(remove_dynamic_filter_consumers_for_join(*input, producers)),
            analyze,
        },
        L::PartialAggregate {
            input,
            group_by,
            aggr_exprs,
            schema,
        } => L::PartialAggregate {
            input: Box::new(remove_dynamic_filter_consumers_for_join(*input, producers)),
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
            input: Box::new(remove_dynamic_filter_consumers_for_join(*input, producers)),
            group_by,
            aggr_exprs,
            schema,
        },
        L::SemiJoin {
            left,
            right,
            left_key,
            right_key,
            residual,
            dynamic_filter_ids,
        } => L::SemiJoin {
            left: Box::new(remove_dynamic_filter_consumers_for_join(*left, producers)),
            right: Box::new(remove_dynamic_filter_consumers_for_join(*right, producers)),
            left_key,
            right_key,
            residual,
            dynamic_filter_ids,
        },
        L::AntiJoin {
            left,
            right,
            left_key,
            right_key,
            residual,
        } => L::AntiJoin {
            left: Box::new(remove_dynamic_filter_consumers_for_join(*left, producers)),
            right: Box::new(remove_dynamic_filter_consumers_for_join(*right, producers)),
            left_key,
            right_key,
            residual,
        },
        L::ScalarSubquery { subplan } => L::ScalarSubquery {
            subplan: Box::new(remove_dynamic_filter_consumers_for_join(
                *subplan, producers,
            )),
        },
        L::UnionAll { inputs } => L::UnionAll {
            inputs: inputs
                .into_iter()
                .map(|input| remove_dynamic_filter_consumers_for_join(input, producers))
                .collect(),
        },
        L::Distinct { input } => L::Distinct {
            input: Box::new(remove_dynamic_filter_consumers_for_join(*input, producers)),
        },
        L::Intersect { left, right } => L::Intersect {
            left: Box::new(remove_dynamic_filter_consumers_for_join(*left, producers)),
            right: Box::new(remove_dynamic_filter_consumers_for_join(*right, producers)),
        },
        L::Except { left, right } => L::Except {
            left: Box::new(remove_dynamic_filter_consumers_for_join(*left, producers)),
            right: Box::new(remove_dynamic_filter_consumers_for_join(*right, producers)),
        },
        L::CreateTable { name, schema } => L::CreateTable { name, schema },
        L::DropTable { name, if_exists } => L::DropTable { name, if_exists },
        L::CreateTableAsSelect { name, source } => L::CreateTableAsSelect {
            name,
            source: Box::new(remove_dynamic_filter_consumers_for_join(*source, producers)),
        },
        L::InsertInto { table, source } => L::InsertInto {
            table,
            source: Box::new(remove_dynamic_filter_consumers_for_join(*source, producers)),
        },
        L::DeleteFrom { table, predicate } => L::DeleteFrom { table, predicate },
        L::CreateView { name, sql, plan } => L::CreateView {
            name,
            sql,
            plan: Box::new(remove_dynamic_filter_consumers_for_join(*plan, producers)),
        },
        L::DropView { name, if_exists } => L::DropView { name, if_exists },
        L::Window { input, functions } => L::Window {
            input: Box::new(remove_dynamic_filter_consumers_for_join(*input, producers)),
            functions,
        },
        L::AssignUniqueId { input, id_column } => L::AssignUniqueId {
            input: Box::new(remove_dynamic_filter_consumers_for_join(*input, producers)),
            id_column,
        },
        L::ExchangeNode { stage_id, schema } => L::ExchangeNode { stage_id, schema },
        L::OneRow => L::OneRow,
    }
}

fn remove_dynamic_filter_consumers_from_fragment(
    fragment: &mut PlanFragment,
    producers: &[DynamicFilterProducer],
) {
    if producers.is_empty() {
        return;
    }
    let root = std::mem::replace(&mut fragment.root, LogicalPlan::OneRow);
    fragment.root = remove_dynamic_filter_consumers_for_join(root, producers);
    for source in &mut fragment.source_fragments {
        remove_dynamic_filter_consumers_from_fragment(source, producers);
    }
}

fn simple_column_index(expr: &PlanExpr) -> Option<usize> {
    match expr {
        PlanExpr::Column { index, .. } => Some(*index),
        _ => None,
    }
}

fn peel_identity_projections_to_aggregate(
    plan: &LogicalPlan,
) -> Option<(&LogicalPlan, &[PlanExpr])> {
    match plan {
        LogicalPlan::Projection { input, exprs, .. }
            if projection_is_identity(exprs, input.schema().len()) =>
        {
            peel_identity_projections_to_aggregate(input)
        }
        LogicalPlan::Aggregate { group_by, .. }
        | LogicalPlan::PartialAggregate { group_by, .. }
        | LogicalPlan::FinalAggregate { group_by, .. } => Some((plan, group_by)),
        _ => None,
    }
}

fn projection_is_identity(exprs: &[PlanExpr], input_width: usize) -> bool {
    exprs.len() == input_width
        && exprs.iter().enumerate().all(
            |(expected, expr)| matches!(expr, PlanExpr::Column { index, .. } if *index == expected),
        )
}

// ===========================================================================
// PartitioningScheme
// ===========================================================================

/// How data is redistributed between fragments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitioningScheme {
    /// All data goes to a single node (gather).
    Single,
    /// Data is hash-partitioned by the given column indices into
    /// `partition_count` output streams.
    Hash {
        columns: Vec<usize>,
        partition_count: usize,
    },
    /// Data is distributed evenly across nodes.
    RoundRobin,
    /// Data is replicated to all nodes.
    Broadcast,
}

impl PartitioningScheme {
    /// Number of output partitions this scheme produces. Used by the coord
    /// to decide how many parallel tasks to launch for the consumer stage.
    pub fn partition_count(&self) -> usize {
        match self {
            Self::Single | Self::RoundRobin | Self::Broadcast => 1,
            Self::Hash {
                partition_count, ..
            } => *partition_count,
        }
    }

    /// Returns true if this partitioning's columns form a (set-wise) subset
    /// of `required`. Equivalent to: "if my data is hashed on my columns,
    /// are rows that share the same `required` values guaranteed to colocate?"
    ///
    /// Subset (not equality) is the right rule: `Hash([a])` colocates rows
    /// matching on `(a, b)` as well, because matching on `(a, b)` implies
    /// matching on `a`. Used by the property-derivation pass (A.3) to decide
    /// whether a `RepartitionExec` is redundant.
    ///
    /// Vendor-port of `Partitioning::is_partitioned_on` semantics from
    /// DataFusion (Apache-2.0) — see attribution on [`Distribution`].
    pub fn is_partitioned_on(&self, required: &[usize]) -> bool {
        match self {
            Self::Hash { columns, .. } => columns.iter().all(|c| required.contains(c)),
            _ => false,
        }
    }

    /// Returns true if this partitioning satisfies the given `required`
    /// input distribution. Used by the property-derivation pass to decide
    /// whether an upstream `RepartitionExec` is needed.
    ///
    /// Semantics tuned to arneb's distributed model (where a downstream
    /// `HashPartitioned` consumer is scheduled as N parallel tasks):
    /// - `Unspecified` is satisfied by anything.
    /// - `SinglePartition` requires exactly one non-replicated stream
    ///   (`Single`, or `Hash{..,n=1}`).
    /// - `HashPartitioned(cols)` requires a multi-partition `Hash` whose
    ///   columns are a subset of `cols` ([`Self::is_partitioned_on`]).
    ///   Single-partition producers cannot fan out to N hash-partitioned
    ///   consumer tasks without an intermediate repartition step, so they
    ///   do *not* satisfy.
    pub fn satisfy(&self, required: &Distribution) -> bool {
        match required {
            Distribution::Unspecified => true,
            Distribution::SinglePartition => {
                matches!(self, Self::Single)
                    || matches!(
                        self,
                        Self::Hash {
                            partition_count: 1,
                            ..
                        }
                    )
            }
            Distribution::HashPartitioned(cols) => match self {
                Self::Hash {
                    partition_count, ..
                } if *partition_count > 1 => self.is_partitioned_on(cols),
                _ => false,
            },
        }
    }
}

impl fmt::Display for PartitioningScheme {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Single => write!(f, "SINGLE"),
            Self::Hash {
                columns,
                partition_count,
            } => write!(f, "HASH({columns:?}, n={partition_count})"),
            Self::RoundRobin => write!(f, "ROUND_ROBIN"),
            Self::Broadcast => write!(f, "BROADCAST"),
        }
    }
}

// ===========================================================================
// Distribution
// ===========================================================================
//
// Vendor-port of `datafusion-physical-expr::Distribution`, adapted to
// arneb's column-index representation. Apache-2.0 attribution:
//   https://github.com/apache/datafusion/blob/main/datafusion/physical-expr/src/partitioning.rs
//
// Why vendor instead of `cargo add datafusion-physical-expr`:
//   - DataFusion's `EnforceDistribution` rule requires their `ExecutionPlan`
//     trait, which arneb does not implement.
//   - DF 53.x pins arrow 56; arneb is on arrow 54.
//   - The DF trait surface has broken 7+ times in 18 months; a thin local
//     copy isolates arneb from upstream churn.
//
// Differences from DF:
//   - `Vec<usize>` column indices instead of `Vec<Arc<dyn PhysicalExpr>>`;
//     matches arneb's existing `PartitioningScheme::Hash` shape.

/// Distribution requirements that a plan node imposes on its input(s).
///
/// Produced by `ExecutionPlan::required_input_distribution()` (analogue
/// to come in A.3); consumed by the property-derivation pass to decide
/// whether to insert a `RepartitionExec` between operators.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Distribution {
    /// No specific distribution required — any partitioning works.
    Unspecified,
    /// All input data must arrive on a single (non-replicated) partition.
    SinglePartition,
    /// Rows that share the same value on the given column indices must be
    /// colocated on the same partition. Indices are 0-based into the input
    /// schema.
    HashPartitioned(Vec<usize>),
}

impl fmt::Display for Distribution {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unspecified => write!(f, "UNSPECIFIED"),
            Self::SinglePartition => write!(f, "SINGLE"),
            Self::HashPartitioned(cols) => write!(f, "HASH({cols:?})"),
        }
    }
}

// ===========================================================================
// FragmentType
// ===========================================================================

/// Classification of a plan fragment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FragmentType {
    /// Reads from a data source (leaf fragment).
    Source,
    /// Runs as a single instance (e.g., final aggregation, coordinator output).
    Fixed,
    /// Distributed by hash partitioning.
    HashPartitioned,
    /// Distributed round-robin.
    RoundRobin,
}

impl fmt::Display for FragmentType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Source => write!(f, "SOURCE"),
            Self::Fixed => write!(f, "FIXED"),
            Self::HashPartitioned => write!(f, "HASH_PARTITIONED"),
            Self::RoundRobin => write!(f, "ROUND_ROBIN"),
        }
    }
}

// ===========================================================================
// PlanFragment
// ===========================================================================

/// A distributable unit of a query plan.
///
/// Each fragment contains a sub-tree of the logical plan that can execute
/// on a single node. Exchange boundaries separate fragments.
#[derive(Debug, Clone)]
pub struct PlanFragment {
    /// Unique stage identifier.
    pub id: StageId,
    /// Classification of this fragment.
    pub fragment_type: FragmentType,
    /// The root logical plan node for this fragment.
    pub root: LogicalPlan,
    /// How this fragment's output is partitioned.
    pub output_partitioning: PartitioningScheme,
    /// Child fragments that feed into this fragment via exchanges.
    pub source_fragments: Vec<PlanFragment>,
}

impl fmt::Display for PlanFragment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Fragment[id={}, type={}, output={}, sources={}]",
            self.id,
            self.fragment_type,
            self.output_partitioning,
            self.source_fragments.len()
        )
    }
}

// ===========================================================================
// QueryStage
// ===========================================================================

/// Scheduling metadata for a fragment.
#[derive(Debug, Clone)]
pub struct QueryStage {
    /// The fragment to execute.
    pub fragment: PlanFragment,
    /// Desired parallelism (number of tasks).
    pub parallelism: usize,
}

// ===========================================================================
// PlanFragmenter
// ===========================================================================

/// Splits an optimized logical plan into a tree of fragments.
///
/// Rules:
/// - Each `TableScan` becomes a SOURCE fragment.
/// - Each side of a `Join` becomes a separate fragment (exchange boundary).
/// - `Aggregate` is split into PartialAggregate + Exchange + FinalAggregate.
/// - The root fragment is FIXED (coordinator output).
///
/// A2.2 (2026-05-28): when `broadcast_max_build_bytes` is `Some(n)` AND
/// `stats` is `Some(_)`, the Join arm checks whether the right (build)
/// child's estimated output size fits within `n` bytes. If yes, the
/// right child fragment's `output_partitioning` is set to
/// `PartitioningScheme::Broadcast` instead of `Hash`, and the join
/// remains a parallel HashPartitioned fragment whose tasks each
/// subscribe to the full right via a `BroadcastOutputBuffer`.
/// Default target rows per hash partition (`dist-adaptive-partition`). 4M is
/// large enough that SF1-scale joins still resolve to the 2-partition floor
/// (no behaviour change at small scale) while SF30-class intermediates
/// (~90M rows) fan out to ~23 partitions. Overridable via
/// `ARNEB_HASH_PARTITION_TARGET_ROWS`.
pub const DEFAULT_HASH_PARTITION_TARGET_ROWS: u64 = 4_000_000;

/// Default cap on the adaptive hash-partition count
/// (`ARNEB_MAX_HASH_PARTITIONS`). Bounds per-query fan-out so a bad estimate
/// cannot create thousands of tiny partitions.
///
/// PARKED at 2 (= effectively N=2 everywhere, the historical fixed fan-out)
/// until `dist-mxn-nested-joins` makes N>2 correct for nested joins. Without
/// that fix, fanning out past 2 drops rows from nested multi-way joins
/// (q09/q05 undercount, q07/q08 "partition already consumed"). Note: a high
/// `ARNEB_HASH_PARTITION_TARGET_ROWS` alone does NOT keep N=2 — nested-join
/// cardinality estimates explode — so the cap is the safe parking knob. Flip
/// this to a real cap (e.g. 64) once nested-join N>2 is correct.
pub const DEFAULT_MAX_HASH_PARTITIONS: usize = 2;

pub struct PlanFragmenter {
    next_stage_id: u32,
    swap_df_allocator: DynamicFilterIdAllocator,
    broadcast_max_build_bytes: Option<usize>,
    stats: Option<std::sync::Arc<crate::cost::CatalogStats>>,
    /// Live cluster worker count, used by the adaptive partition rule.
    /// Default 1 → the rule floors at 2 (old fixed behaviour) unless stats
    /// raise it. Set by the coordinator from the node registry.
    worker_count: usize,
    /// Target rows per hash partition (`dist-adaptive-partition`).
    hash_partition_target_rows: u64,
    /// Cap on the adaptive hash-partition count.
    max_hash_partitions: usize,
    /// Internal, per-recursion override used when a SemiJoin/AntiJoin build is
    /// a high-cardinality aggregate and `ARNEB_PARTITIONED_SEMI_JOIN` is on.
    force_parallel_final_agg: bool,
}

impl PlanFragmenter {
    /// Creates a new fragmenter with broadcast disabled (default).
    pub fn new() -> Self {
        Self {
            next_stage_id: 0,
            swap_df_allocator: DynamicFilterIdAllocator::new_starting_at(1_000_000),
            broadcast_max_build_bytes: None,
            stats: None,
            worker_count: 1,
            hash_partition_target_rows: DEFAULT_HASH_PARTITION_TARGET_ROWS,
            max_hash_partitions: DEFAULT_MAX_HASH_PARTITIONS,
            force_parallel_final_agg: false,
        }
    }

    /// `dist-adaptive-partition`: set the live cluster worker count, a floor
    /// for the adaptive hash-partition fan-out (every worker gets work).
    pub fn with_worker_count(mut self, workers: usize) -> Self {
        self.worker_count = workers;
        self
    }

    /// `dist-adaptive-partition`: set the partition-count policy knobs
    /// (target rows-per-partition + a hard cap). Resolved by the server from
    /// `ARNEB_HASH_PARTITION_TARGET_ROWS` / `ARNEB_MAX_HASH_PARTITIONS`.
    pub fn with_partition_policy(mut self, target_rows: u64, max_partitions: usize) -> Self {
        self.hash_partition_target_rows = target_rows;
        self.max_hash_partitions = max_partitions;
        self
    }

    /// A2.2 (2026-05-28): set the broadcast-eligibility cap. `Some(n)`
    /// enables the Join arm to mark builds smaller than `n` bytes for
    /// `PartitioningScheme::Broadcast`. `None` (default) preserves the
    /// pre-A2.2 W3-Hash α-model behaviour.
    pub fn with_broadcast_threshold(mut self, bytes: Option<usize>) -> Self {
        self.broadcast_max_build_bytes = bytes;
        self
    }

    /// A2.2: attach the per-query `CatalogStats` snapshot used by the
    /// broadcast decision's `estimated_bytes` call. Without stats the
    /// broadcast check degrades to default row-count estimates from
    /// `cost::DEFAULT_TABLE_SIZE`, which is usually too coarse — most
    /// real builds either look way too big (skipped) or too small
    /// (false-positive broadcast); the wrapper passes `None` from the
    /// fragmenter in unit tests where stats are unavailable.
    pub fn with_stats(mut self, stats: Option<std::sync::Arc<crate::cost::CatalogStats>>) -> Self {
        self.stats = stats;
        self
    }

    /// A2.2: returns true when `(threshold, stats)` are both configured
    /// AND the plan's estimated output bytes fit within the cap.
    /// The Join arm calls this on the right (build) subtree before
    /// deciding to mark it broadcast.
    ///
    /// N3 fix (2026-05-28): the caller MUST pass the *original* (un-split)
    /// right subtree. After `self.split(*right)` runs, the right plan
    /// becomes a bare `ExchangeNode` placeholder for which
    /// `estimated_cardinality` returns `DEFAULT_TABLE_SIZE` (10 000),
    /// causing nearly every right side to look broadcast-eligible
    /// regardless of actual size.
    fn is_broadcast_eligible(&self, build_plan: &LogicalPlan) -> bool {
        let Some(threshold) = self.broadcast_max_build_bytes else {
            return false;
        };
        let Some(stats) = self.stats.as_deref() else {
            return false;
        };
        crate::cost::estimated_bytes(build_plan, stats) <= threshold
    }

    fn high_cardinality_aggregate_build(
        &self,
        build_plan: &LogicalPlan,
        build_key_index: usize,
    ) -> bool {
        let Some(stats) = self.stats.as_deref() else {
            return false;
        };
        let Some((aggregate_plan, group_by)) = peel_identity_projections_to_aggregate(build_plan)
        else {
            return false;
        };
        if build_key_index >= group_by.len()
            || simple_column_index(&group_by[build_key_index]).is_none()
        {
            return false;
        }

        let estimated_groups = crate::cost::estimated_cardinality(aggregate_plan, stats);
        estimated_groups > self.hash_partition_target_rows as f64
    }

    fn next_id(&mut self) -> StageId {
        let id = StageId(self.next_stage_id);
        self.next_stage_id += 1;
        id
    }

    /// Fragments the given logical plan into a tree of [`PlanFragment`]s.
    pub fn fragment(&mut self, plan: LogicalPlan) -> PlanFragment {
        // Pre-fragmentation: decompose AVG into SUM/COUNT so AVG-bearing
        // aggregates become decomposable and split into Partial+Final
        // (workers emit O(groups) partial rows, not O(input) raw rows).
        let plan = rewrite_avg_for_split(plan);
        let (root_plan, source_fragments) = self.split(plan);
        let mut root = PlanFragment {
            id: self.next_id(),
            fragment_type: FragmentType::Fixed,
            root: root_plan,
            output_partitioning: PartitioningScheme::Single,
            source_fragments,
        };
        // dist-mxn-nested-joins: make every connected hash-exchange chain share
        // one `partition_count` so the M==N invariant holds at every boundary.
        // No-op at the historical fixed N=2 (already uniform).
        normalize_chain_partition_counts(&mut root);
        root
    }

    /// Recursively split the plan. Returns (local plan, child fragments).
    fn split(&mut self, plan: LogicalPlan) -> (LogicalPlan, Vec<PlanFragment>) {
        self.split_with_parent_aggregate_projection(plan, None)
    }

    /// Recursively split the plan. `parent_aggregate_projection` is populated
    /// only when this node is an Aggregate directly under a Projection, so the
    /// PFA split can preserve projection-required aggregate output ordering.
    fn split_with_parent_aggregate_projection(
        &mut self,
        plan: LogicalPlan,
        parent_aggregate_projection: Option<&[PlanExpr]>,
    ) -> (LogicalPlan, Vec<PlanFragment>) {
        match plan {
            LogicalPlan::TableScan {
                table,
                schema,
                alias,
                properties,
                dynamic_filters_consumed,
            } => {
                // TableScan becomes a SOURCE fragment. Preserve any
                // cross-fragment dynamic-filter consumer annotations
                // assigned by `AssignDynamicFilterIds`; the worker
                // that runs this fragment needs to know which DFs to
                // await before scan rows are read.
                let scan_plan = LogicalPlan::TableScan {
                    table: table.clone(),
                    schema: schema.clone(),
                    alias: alias.clone(),
                    properties: properties.clone(),
                    dynamic_filters_consumed: dynamic_filters_consumed.clone(),
                };
                let fragment = PlanFragment {
                    id: self.next_id(),
                    fragment_type: FragmentType::Source,
                    root: scan_plan,
                    output_partitioning: PartitioningScheme::RoundRobin,
                    source_fragments: vec![],
                };
                // Replace with an ExchangeNode placeholder in the parent.
                let exchange = LogicalPlan::ExchangeNode {
                    stage_id: fragment.id,
                    schema: schema.clone(),
                };
                (exchange, vec![fragment])
            }

            LogicalPlan::Filter { input, predicate } => {
                let (input_plan, mut frags) = self.split(*input);
                // W1 MVP push-down: when the filter sits directly above
                // a `TableScan` source fragment, fold it into the worker
                // task so the worker emits already-filtered rows. Without
                // this every worker ships the full table to the coord,
                // which then OOMs on multi-join queries (Q05/Q07/Q09/…)
                // because the coord buffers the full lineitem/orders/etc.
                // The exchange schema is preserved (Filter is a row-mask,
                // not a projection), so the upstream `ExchangeNode` and
                // any column-index baked into the coord plan stay valid.
                if let LogicalPlan::ExchangeNode { stage_id, .. } = &input_plan {
                    if frags.len() == 1 && frags[0].id == *stage_id {
                        let mut scan_frag = frags.pop().unwrap();
                        scan_frag.root = LogicalPlan::Filter {
                            input: Box::new(scan_frag.root),
                            predicate,
                        };
                        return (input_plan, vec![scan_frag]);
                    }
                }
                if let LogicalPlan::Projection {
                    input: proj_input,
                    exprs,
                    schema,
                } = &input_plan
                {
                    if let LogicalPlan::ExchangeNode { stage_id, .. } = proj_input.as_ref() {
                        if frags.len() == 1 && frags[0].id == *stage_id {
                            let partitioning = minimal_join_carry_fragment_fold_enabled()
                                .then(|| simple_column_projection_indices(exprs))
                                .flatten()
                                .and_then(|indices| {
                                    remap_fragment_partitioning_for_projection(
                                        &frags[0].output_partitioning,
                                        &indices,
                                    )
                                });
                            if is_fold_safe_cast_projection(exprs, schema) || partitioning.is_some()
                            {
                                let mut scan_frag = frags.pop().unwrap();
                                scan_frag.root = LogicalPlan::Filter {
                                    input: Box::new(LogicalPlan::Projection {
                                        input: Box::new(scan_frag.root),
                                        exprs: exprs.clone(),
                                        schema: schema.clone(),
                                    }),
                                    predicate,
                                };
                                if let Some(partitioning) = partitioning {
                                    scan_frag.output_partitioning = partitioning;
                                }
                                let new_exchange = LogicalPlan::ExchangeNode {
                                    stage_id: *stage_id,
                                    schema: schema.clone(),
                                };
                                return (new_exchange, vec![scan_frag]);
                            }
                        }
                    }
                }
                (
                    LogicalPlan::Filter {
                        input: Box::new(input_plan),
                        predicate,
                    },
                    frags,
                )
            }

            LogicalPlan::Projection {
                input,
                exprs,
                schema,
            } => {
                let parent_aggregate_projection =
                    matches!(input.as_ref(), LogicalPlan::Aggregate { .. })
                        .then_some(exprs.as_slice());
                let (input_plan, mut frags) = self
                    .split_with_parent_aggregate_projection(*input, parent_aggregate_projection);
                if parent_aggregate_projection.is_none()
                    && minimal_join_carry_fragment_fold_enabled()
                {
                    if let LogicalPlan::ExchangeNode { stage_id, .. } = &input_plan {
                        if frags.len() == 1 && frags[0].id == *stage_id {
                            if let Some(indices) = simple_column_projection_indices(&exprs) {
                                if let Some(partitioning) =
                                    remap_fragment_partitioning_for_projection(
                                        &frags[0].output_partitioning,
                                        &indices,
                                    )
                                {
                                    let mut fragment = frags.pop().unwrap();
                                    fragment.root = LogicalPlan::Projection {
                                        input: Box::new(fragment.root),
                                        exprs,
                                        schema: schema.clone(),
                                    };
                                    fragment.output_partitioning = partitioning;
                                    let exchange = LogicalPlan::ExchangeNode {
                                        stage_id: *stage_id,
                                        schema,
                                    };
                                    return (exchange, vec![fragment]);
                                }
                            }
                        }
                    }
                }
                (
                    LogicalPlan::Projection {
                        input: Box::new(input_plan),
                        exprs,
                        schema,
                    },
                    frags,
                )
            }

            LogicalPlan::Sort { input, order_by } => {
                let (input_plan, frags) = self.split(*input);
                (
                    LogicalPlan::Sort {
                        input: Box::new(input_plan),
                        order_by,
                    },
                    frags,
                )
            }

            LogicalPlan::Limit {
                input,
                limit,
                offset,
            } => {
                let (input_plan, frags) = self.split(*input);
                (
                    LogicalPlan::Limit {
                        input: Box::new(input_plan),
                        limit,
                        offset,
                    },
                    frags,
                )
            }

            LogicalPlan::Join {
                left,
                right,
                join_type,
                mut condition,
                mut dynamic_filter_ids,
            } => {
                let mut left_input = *left;
                let mut right_input = *right;
                let mut restore_swapped_output = None;

                // N3 fix (2026-05-28): capture the UN-SPLIT right plan
                // for the broadcast eligibility check. After
                // `self.split(*right)` runs, `right_plan` becomes a
                // bare `ExchangeNode` placeholder pointing at the
                // newly created child fragment, and
                // `estimated_cardinality(ExchangeNode)` always returns
                // `DEFAULT_TABLE_SIZE` (10000) — so `is_broadcast_eligible`
                // was passing for nearly every right side regardless
                // of actual size, including 60 M-row filtered lineitem
                // in Q14/Q19. Cloning the boxed right is cheap (a
                // LogicalPlan tree is a handful of nodes).
                let mut original_right_for_broadcast = right_input.clone();
                // dist-adaptive-partition: same trap — the adaptive count must
                // estimate cardinality from the UN-SPLIT subtrees. After split,
                // `left_plan`/`right_plan` are bare `ExchangeNode` placeholders
                // whose `estimated_cardinality` is `DEFAULT_TABLE_SIZE` (10000),
                // which would pin the fan-out to the worker-count floor.
                let mut original_left_for_estimate = left_input.clone();

                if broadcast_build_side_swap_enabled()
                    && matches!(join_type, arneb_sql_parser::ast::JoinType::Inner)
                    && self.broadcast_max_build_bytes.is_some()
                    && self.stats.is_some()
                    && !self.is_broadcast_eligible(&original_right_for_broadcast)
                    && self.is_broadcast_eligible(&original_left_for_estimate)
                {
                    let old_left_width = original_left_for_estimate.schema().len();
                    let old_right_width = original_right_for_broadcast.schema().len();
                    if let Some((left_keys, right_keys)) =
                        extract_partitioning_equi_keys(&condition, old_left_width)
                    {
                        let original_schema = {
                            let mut schema = original_left_for_estimate.schema();
                            schema.extend(original_right_for_broadcast.schema());
                            schema
                        };
                        std::mem::swap(&mut left_input, &mut right_input);
                        std::mem::swap(
                            &mut original_left_for_estimate,
                            &mut original_right_for_broadcast,
                        );
                        left_input = remove_dynamic_filter_consumers_for_join(
                            left_input,
                            &dynamic_filter_ids,
                        );
                        right_input = remove_dynamic_filter_consumers_for_join(
                            right_input,
                            &dynamic_filter_ids,
                        );
                        condition = remap_join_condition_for_swapped_join(
                            condition,
                            old_left_width,
                            old_right_width,
                        );
                        let _swapped_left_keys = right_keys;
                        let _swapped_right_keys = left_keys;
                        dynamic_filter_ids.clear();
                        let regen_enabled = swap_df_regen_enabled();
                        eprintln!(
                            "[Q5_DF_REPRO] fragment_join_branch=swap df_regen={regen_enabled} old_left_width={old_left_width} old_right_width={old_right_width}"
                        );
                        if regen_enabled {
                            let regenerated_join = assign_for_join(
                                left_input,
                                right_input,
                                join_type,
                                condition,
                                &self.swap_df_allocator,
                            );
                            let LogicalPlan::Join {
                                left,
                                right,
                                condition: regenerated_condition,
                                dynamic_filter_ids: regenerated_dynamic_filter_ids,
                                ..
                            } = regenerated_join
                            else {
                                unreachable!("assign_for_join always returns a Join");
                            };
                            left_input = *left;
                            right_input = *right;
                            condition = regenerated_condition;
                            dynamic_filter_ids = regenerated_dynamic_filter_ids;
                        }
                        restore_swapped_output = Some(SwappedJoinOutputRestore {
                            old_left_width,
                            old_right_width,
                            original_schema,
                        });
                    }
                }

                if partitioned_build_side_swap_enabled()
                    && matches!(join_type, arneb_sql_parser::ast::JoinType::Inner)
                    && self.stats.is_some()
                {
                    let old_left_width = original_left_for_estimate.schema().len();
                    let old_right_width = original_right_for_broadcast.schema().len();
                    let pure_equi =
                        extract_pure_equi_join_keys(&condition, old_left_width).is_some();
                    let factor = partitioned_build_side_swap_factor();
                    let estimates = self.stats.as_deref().and_then(|stats| {
                        partitioned_build_side_swap_estimates(
                            &original_left_for_estimate,
                            &original_right_for_broadcast,
                            stats,
                            factor,
                        )
                    });
                    if pure_equi {
                        if let Some((left_rows, right_rows)) = estimates {
                            let original_schema = {
                                let mut schema = original_left_for_estimate.schema();
                                schema.extend(original_right_for_broadcast.schema());
                                schema
                            };
                            std::mem::swap(&mut left_input, &mut right_input);
                            std::mem::swap(
                                &mut original_left_for_estimate,
                                &mut original_right_for_broadcast,
                            );
                            left_input = remove_dynamic_filter_consumers_for_join(
                                left_input,
                                &dynamic_filter_ids,
                            );
                            right_input = remove_dynamic_filter_consumers_for_join(
                                right_input,
                                &dynamic_filter_ids,
                            );
                            condition = remap_join_condition_for_swapped_join(
                                condition,
                                old_left_width,
                                old_right_width,
                            );
                            dynamic_filter_ids.clear();
                            restore_swapped_output = Some(SwappedJoinOutputRestore {
                                old_left_width,
                                old_right_width,
                                original_schema,
                            });
                            tracing::info!(
                                target: "arneb::planner",
                                left_rows,
                                right_rows,
                                factor,
                                "partitioned build-side swap applied"
                            );
                        }
                    }
                }

                let (left_plan, mut left_frags) = self.split(left_input);
                let (right_plan, mut right_frags) = self.split(right_input);

                // Broadcast v2b (2026-06-03): INLINE a broadcast-build join
                // into the probe fragment when the probe is already a
                // partitioned (N-task) fragment. The probe keeps its
                // partitioning (a broadcast build needs no colocation), so
                // there is NO probe re-hash and NO new probe stage — a
                // left-deep chain of broadcast joins collapses into ONE
                // probe fragment with sibling Broadcast build sources.
                // Mirrors Trino's REPLICATE exchange not cutting the probe
                // fragment (`PlanFragmenter.visitExchange`), confirmed by
                // 2026-06-03 cross-engine research. This eliminates BOTH
                // the per-level probe re-shuffle (q09's ~50 GB) AND the
                // per-level re-partition+rebuild barrier (the
                // first_batch≈elapsed serialization), since the small
                // broadcast build has a fast barrier and the probe streams
                // straight through.
                //
                // Gate: only when the probe child is ALREADY HashPartitioned
                // (multi-task). For a bare-scan probe, inlining would
                // serialize the join to one task — those fall through to the
                // v2a path below (re-hash probe N-way, broadcast build, N
                // parallel join tasks), which keeps parallelism without the
                // q14-style serial-probe regression. INNER/LEFT only (the
                // probe is the left side; broadcast preserves its
                // partitioning for these — research-confirmed). Dormant
                // unless `broadcast_max_build_bytes` + `stats` are set.
                if matches!(join_type, arneb_sql_parser::ast::JoinType::Inner)
                    && left_frags.len() == 1
                    && right_frags.len() == 1
                    && matches!(
                        left_frags[0].fragment_type,
                        FragmentType::HashPartitioned | FragmentType::Source
                    )
                    && self.is_broadcast_eligible(&original_right_for_broadcast)
                {
                    let mut build_frag = right_frags.pop().unwrap();
                    build_frag.output_partitioning = PartitioningScheme::Broadcast;
                    // Push the join INTO the probe fragment. Its root
                    // becomes Join(probe_root, ExchangeNode(build)); the
                    // build joins as a sibling source. output_partitioning
                    // and fragment_type are UNCHANGED — broadcast preserves
                    // the probe's existing N-way partitioning, so the next
                    // join up still sees an N-partition exchange.
                    let probe_id = left_frags[0].id;
                    let scan_root = std::mem::replace(&mut left_frags[0].root, LogicalPlan::OneRow);
                    let mut probe_root =
                        replace_first_exchange_or_fallback(left_plan, probe_id, scan_root);
                    let mut build_plan = right_plan;
                    let regen_enabled = swap_df_regen_enabled();
                    eprintln!(
                        "[Q5_DF_REPRO] fragment_join_branch=v2b_inline df_regen={regen_enabled} probe_fragment={} build_fragment={}",
                        left_frags[0].id,
                        build_frag.id
                    );
                    if regen_enabled {
                        probe_root = remove_dynamic_filter_consumers_for_join(
                            probe_root,
                            &dynamic_filter_ids,
                        );
                        build_plan = remove_dynamic_filter_consumers_for_join(
                            build_plan,
                            &dynamic_filter_ids,
                        );
                        build_frag.root = remove_dynamic_filter_consumers_for_join(
                            build_frag.root,
                            &dynamic_filter_ids,
                        );
                        dynamic_filter_ids.clear();
                        let regenerated_join = assign_for_join(
                            probe_root,
                            build_plan,
                            join_type,
                            condition,
                            &self.swap_df_allocator,
                        );
                        let LogicalPlan::Join {
                            left,
                            right,
                            condition: regenerated_condition,
                            dynamic_filter_ids: regenerated_dynamic_filter_ids,
                            ..
                        } = regenerated_join
                        else {
                            unreachable!("assign_for_join always returns a Join");
                        };
                        probe_root = *left;
                        build_plan = *right;
                        condition = regenerated_condition;
                        dynamic_filter_ids = regenerated_dynamic_filter_ids;
                    }
                    let join_plan = LogicalPlan::Join {
                        left: Box::new(probe_root),
                        right: Box::new(build_plan),
                        join_type,
                        condition,
                        dynamic_filter_ids,
                    };
                    let join_plan =
                        restore_swapped_join_output(join_plan, restore_swapped_output.as_ref());
                    let output_schema = join_plan.schema();
                    left_frags[0].root = join_plan;
                    left_frags[0].source_fragments.push(build_frag);
                    let exchange = LogicalPlan::ExchangeNode {
                        stage_id: left_frags[0].id,
                        schema: output_schema,
                    };
                    return (exchange, left_frags);
                }

                // M×N step 4 (2026-05-20): drop the `fragment_type == Source`
                // requirement from the A.4-era gate. With M×N runtime wired
                // (commit aa05e1f), an upstream HashPartitioned child whose
                // output_partitioning we overwrite here will re-hash its
                // output on the new keys via worker-side RepartitionExec.
                //
                // Preconditions for safely setting children's output_partitioning:
                //   (i)  `frags.len() == 1` — avoid mis-partitioning Union etc.
                //        where `frags[0]` would silently skip siblings.
                //   (ii) `plan.schema().len() == frags[0].root.schema().len()`
                //        — column indices extracted from the JOIN condition
                //        reference `left_plan`/`right_plan` schemas. When a
                //        schema-changing wrapper (Aggregate, projection) sits
                //        between the join and its child fragment, those
                //        indices no longer correspond to columns in the
                //        child's schema. Setting `frags[0].output_partitioning`
                //        to those indices hashes rows on the WRONG columns
                //        and silently drops joined rows downstream. Q02
                //        (Aggregate(ExchangeNode) right child) is the
                //        canonical case.
                let left_col_count = left_plan.schema().len();
                // dist-adaptive-partition: choose the hash fan-out from the
                // cluster size and the rows flowing through this exchange
                // (the larger of the two children being repartitioned), instead
                // of the old fixed 2. Without stats the estimate is `None` and
                // the rule falls back to a worker-count-only count.
                let estimated_rows = self.stats.as_deref().map(|stats| {
                    let l = crate::cost::estimated_cardinality(&original_left_for_estimate, stats);
                    let r =
                        crate::cost::estimated_cardinality(&original_right_for_broadcast, stats);
                    l.max(r).ceil() as u64
                });
                let partition_count = choose_partition_count(
                    self.worker_count,
                    estimated_rows,
                    self.hash_partition_target_rows,
                    self.max_hash_partitions,
                );
                // dist-mxn-nested-joins T2 (2026-06-05): env-gated trace of the
                // per-join count choice + what each child's output count was
                // BEFORE this join overwrites it. Confirms the `o_i` flow:
                // this join writes its own `partition_count` onto its children,
                // while its own output is later overwritten by its parent — the
                // non-uniformity that breaks the M==N exchange invariant.
                if std::env::var("ARNEB_TRACE_FRAGMENTS")
                    .map(|v| v != "0" && !v.is_empty())
                    .unwrap_or(false)
                {
                    let lc = left_frags
                        .first()
                        .map(|f| (f.id.0, f.output_partitioning.partition_count()));
                    let rc = right_frags
                        .first()
                        .map(|f| (f.id.0, f.output_partitioning.partition_count()));
                    eprintln!(
                        "[FRAGTRACE] CHOOSE join o_i={partition_count} est_rows={estimated_rows:?} left_child(id,prev_n)={lc:?} right_child(id,prev_n)={rc:?}"
                    );
                }
                tracing::debug!(
                    worker_count = self.worker_count,
                    estimated_rows = ?estimated_rows,
                    target_rows = self.hash_partition_target_rows,
                    max_partitions = self.max_hash_partitions,
                    partition_count,
                    "dist-adaptive-partition: hash fan-out chosen"
                );
                let single_frags = left_frags.len() == 1 && right_frags.len() == 1;
                let schemas_align = single_frags
                    && left_plan.schema().len() == left_frags[0].root.schema().len()
                    && right_plan.schema().len() == right_frags[0].root.schema().len();
                // Fixed children can't honour a non-Single output_partitioning
                // override: the coord match `(Fixed, _) -> (1, 1)` sends 1 task
                // with 1 output partition, no RepartitionExec wrap. Setting
                // their output to non-empty Hash here creates a mismatch where
                // the consumer expects N partitions but upstream emits 1 →
                // silent data loss. The canonical case is Q02 after JoinReorder
                // inlines the subquery Aggregate mid-chain (one join becomes
                // Fixed due to !schemas_align, then the join above it tries to
                // partition that Fixed fragment).
                let partitionable_children = single_frags
                    && left_frags[0].fragment_type != FragmentType::Fixed
                    && right_frags[0].fragment_type != FragmentType::Fixed;
                let partitioning = if schemas_align && partitionable_children {
                    extract_partitioning_equi_keys(&condition, left_col_count)
                } else {
                    None
                };

                if let Some((left_keys, right_keys)) = partitioning {
                    // Broadcast v2 (2026-06-03): if the right (build)
                    // subtree is broadcast-eligible, keep the PROBE (left)
                    // hash-partitioned N-way and the join HashPartitioned
                    // (N parallel tasks) — ONLY the build switches to
                    // Broadcast (1 producer, replayed to every probe task
                    // via `BroadcastOutputBuffer.subscribe()`). Each probe
                    // task K pulls its colocated probe partition K and the
                    // FULL broadcast build, then probes locally.
                    //
                    // This replaces A2.3 v1, which collapsed the join to a
                    // Fixed single-task fragment with left→Single. v1 was
                    // (a) serial on the probe (the q14/q19 regression in
                    // the A2.4 measurement) and (b) CORRECTNESS-BROKEN when
                    // the left child was itself a multi-partition HASH
                    // fragment: forcing its output to Single made the lone
                    // consumer task pull only partition 0, silently
                    // dropping the other buckets (measured SF30 q09 =
                    // 0.496× = exactly half). Keeping the probe N-way both
                    // fixes correctness and preserves parallelism. Dormant
                    // when `broadcast_max_build_bytes` / `stats` are None.
                    if self.is_broadcast_eligible(&original_right_for_broadcast) {
                        eprintln!(
                            "[Q5_DF_REPRO] fragment_join_branch=v2a_broadcast df_regen=false left_child={} right_child={} partition_count={partition_count}",
                            left_frags[0].id,
                            right_frags[0].id
                        );
                        // Probe stays hashed on its equi-keys (N partitions,
                        // one per parallel join task). The right equi-keys
                        // aren't needed — a broadcast build needs no
                        // colocation since every task holds the whole build.
                        let _ = right_keys;
                        left_frags[0].output_partitioning = PartitioningScheme::Hash {
                            columns: left_keys,
                            partition_count,
                        };
                        right_frags[0].output_partitioning = PartitioningScheme::Broadcast;
                        left_frags.append(&mut right_frags);

                        let join_plan = LogicalPlan::Join {
                            left: Box::new(left_plan),
                            right: Box::new(right_plan),
                            join_type,
                            condition,
                            dynamic_filter_ids: dynamic_filter_ids.clone(),
                        };
                        let join_plan =
                            restore_swapped_join_output(join_plan, restore_swapped_output.as_ref());
                        let output_schema = join_plan.schema();

                        let join_fragment = PlanFragment {
                            id: self.next_id(),
                            fragment_type: FragmentType::HashPartitioned,
                            root: join_plan,
                            output_partitioning: PartitioningScheme::Hash {
                                columns: Vec::new(),
                                partition_count,
                            },
                            source_fragments: left_frags,
                        };

                        let exchange = LogicalPlan::ExchangeNode {
                            stage_id: join_fragment.id,
                            schema: output_schema,
                        };
                        return (exchange, vec![join_fragment]);
                    }

                    // Default path (A2.2 W3-Hash α/β model): both sides
                    // hash-partitioned on equi-keys, N parallel join
                    // tasks each pulling colocated partition K.
                    left_frags[0].output_partitioning = PartitioningScheme::Hash {
                        columns: left_keys,
                        partition_count,
                    };
                    right_frags[0].output_partitioning = PartitioningScheme::Hash {
                        columns: right_keys,
                        partition_count,
                    };
                    left_frags.append(&mut right_frags);

                    let join_plan = LogicalPlan::Join {
                        left: Box::new(left_plan),
                        right: Box::new(right_plan),
                        join_type,
                        condition,
                        dynamic_filter_ids: dynamic_filter_ids.clone(),
                    };
                    let join_plan =
                        restore_swapped_join_output(join_plan, restore_swapped_output.as_ref());
                    let output_schema = join_plan.schema();

                    let join_fragment = PlanFragment {
                        id: self.next_id(),
                        fragment_type: FragmentType::HashPartitioned,
                        root: join_plan,
                        output_partitioning: PartitioningScheme::Hash {
                            columns: Vec::new(),
                            partition_count,
                        },
                        source_fragments: left_frags,
                    };

                    let exchange = LogicalPlan::ExchangeNode {
                        stage_id: join_fragment.id,
                        schema: output_schema,
                    };
                    (exchange, vec![join_fragment])
                } else {
                    // Single-task join fragment, no hash repartitioning of
                    // children — matches W3-Join MVP behaviour.
                    left_frags.append(&mut right_frags);

                    let join_plan = LogicalPlan::Join {
                        left: Box::new(left_plan),
                        right: Box::new(right_plan),
                        join_type,
                        condition,
                        dynamic_filter_ids,
                    };
                    let join_plan =
                        restore_swapped_join_output(join_plan, restore_swapped_output.as_ref());
                    let output_schema = join_plan.schema();

                    let join_fragment = PlanFragment {
                        id: self.next_id(),
                        fragment_type: FragmentType::Fixed,
                        root: join_plan,
                        output_partitioning: PartitioningScheme::Single,
                        source_fragments: left_frags,
                    };

                    let exchange = LogicalPlan::ExchangeNode {
                        stage_id: join_fragment.id,
                        schema: output_schema,
                    };
                    (exchange, vec![join_fragment])
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
                let original_left_for_estimate = left.as_ref().clone();
                let original_right_for_estimate = right.as_ref().clone();
                let left_key_index = simple_column_index(&left_key);
                let right_key_index = simple_column_index(&right_key);
                let prefer_hash_aggregate_build = partitioned_semi_join_enabled()
                    && matches!((left_key_index, right_key_index), (Some(_), Some(_)))
                    && self.high_cardinality_aggregate_build(
                        &original_right_for_estimate,
                        right_key_index.expect("checked Some above"),
                    );
                let (left_plan, mut left_frags) = self.split(*left);
                let previous_force_parallel_final_agg = self.force_parallel_final_agg;
                self.force_parallel_final_agg =
                    previous_force_parallel_final_agg || prefer_hash_aggregate_build;
                let (right_plan, mut right_frags) = self.split(*right);
                self.force_parallel_final_agg = previous_force_parallel_final_agg;

                let single_frags = left_frags.len() == 1 && right_frags.len() == 1;
                let schemas_align = single_frags
                    && left_plan.schema().len() == left_frags[0].root.schema().len()
                    && right_plan.schema().len() == right_frags[0].root.schema().len();
                let partitionable_children = single_frags
                    && left_frags[0].fragment_type != FragmentType::Fixed
                    && right_frags[0].fragment_type != FragmentType::Fixed;
                let aggregate_build =
                    peel_identity_projections_to_aggregate(&original_right_for_estimate).is_some();

                let partitioning = if partitioned_semi_join_enabled()
                    && schemas_align
                    && partitionable_children
                    && (!aggregate_build || prefer_hash_aggregate_build)
                {
                    left_key_index.zip(right_key_index)
                } else {
                    None
                };

                if let Some((left_key_index, right_key_index)) = partitioning {
                    let estimated_rows = self.stats.as_deref().map(|stats| {
                        let l =
                            crate::cost::estimated_cardinality(&original_left_for_estimate, stats);
                        let r =
                            crate::cost::estimated_cardinality(&original_right_for_estimate, stats);
                        l.max(r).ceil() as u64
                    });
                    let partition_count = choose_partition_count(
                        self.worker_count,
                        estimated_rows,
                        self.hash_partition_target_rows,
                        self.max_hash_partitions,
                    );

                    left_frags[0].output_partitioning = PartitioningScheme::Hash {
                        columns: vec![left_key_index],
                        partition_count,
                    };
                    right_frags[0].output_partitioning = PartitioningScheme::Hash {
                        columns: vec![right_key_index],
                        partition_count,
                    };
                    for fragment in &mut left_frags {
                        remove_dynamic_filter_consumers_from_fragment(
                            fragment,
                            &dynamic_filter_ids,
                        );
                    }
                    for fragment in &mut right_frags {
                        remove_dynamic_filter_consumers_from_fragment(
                            fragment,
                            &dynamic_filter_ids,
                        );
                    }
                    left_frags.append(&mut right_frags);

                    // Partitioned semi joins are exact by key colocation, but
                    // this first gated path intentionally disables dynamic
                    // filters so no producer/consumer state crosses partition
                    // or fragment boundaries with unresolved ownership.
                    let join_plan = LogicalPlan::SemiJoin {
                        left: Box::new(remove_dynamic_filter_consumers_for_join(
                            left_plan,
                            &dynamic_filter_ids,
                        )),
                        right: Box::new(remove_dynamic_filter_consumers_for_join(
                            right_plan,
                            &dynamic_filter_ids,
                        )),
                        left_key,
                        right_key,
                        residual,
                        dynamic_filter_ids: Vec::new(),
                    };
                    let output_schema = join_plan.schema();

                    let join_fragment = PlanFragment {
                        id: self.next_id(),
                        fragment_type: FragmentType::HashPartitioned,
                        root: join_plan,
                        output_partitioning: PartitioningScheme::Hash {
                            columns: Vec::new(),
                            partition_count,
                        },
                        source_fragments: left_frags,
                    };

                    let exchange = LogicalPlan::ExchangeNode {
                        stage_id: join_fragment.id,
                        schema: output_schema,
                    };
                    return (exchange, vec![join_fragment]);
                }

                left_frags.append(&mut right_frags);

                let join_plan = LogicalPlan::SemiJoin {
                    left: Box::new(left_plan),
                    right: Box::new(right_plan),
                    left_key,
                    right_key,
                    residual,
                    dynamic_filter_ids,
                };
                let output_schema = join_plan.schema();

                let join_fragment = PlanFragment {
                    id: self.next_id(),
                    fragment_type: FragmentType::Fixed,
                    root: join_plan,
                    output_partitioning: PartitioningScheme::Single,
                    source_fragments: left_frags,
                };

                let exchange = LogicalPlan::ExchangeNode {
                    stage_id: join_fragment.id,
                    schema: output_schema,
                };
                (exchange, vec![join_fragment])
            }

            LogicalPlan::AntiJoin {
                left,
                right,
                left_key,
                right_key,
                residual,
            } => {
                let original_left_for_estimate = left.as_ref().clone();
                let original_right_for_estimate = right.as_ref().clone();
                let left_key_index = simple_column_index(&left_key);
                let right_key_index = simple_column_index(&right_key);
                let prefer_hash_aggregate_build = partitioned_semi_join_enabled()
                    && matches!((left_key_index, right_key_index), (Some(_), Some(_)))
                    && self.high_cardinality_aggregate_build(
                        &original_right_for_estimate,
                        right_key_index.expect("checked Some above"),
                    );
                let (left_plan, mut left_frags) = self.split(*left);
                let previous_force_parallel_final_agg = self.force_parallel_final_agg;
                self.force_parallel_final_agg =
                    previous_force_parallel_final_agg || prefer_hash_aggregate_build;
                let (right_plan, mut right_frags) = self.split(*right);
                self.force_parallel_final_agg = previous_force_parallel_final_agg;

                let single_frags = left_frags.len() == 1 && right_frags.len() == 1;
                let schemas_align = single_frags
                    && left_plan.schema().len() == left_frags[0].root.schema().len()
                    && right_plan.schema().len() == right_frags[0].root.schema().len();
                let partitionable_children = single_frags
                    && left_frags[0].fragment_type != FragmentType::Fixed
                    && right_frags[0].fragment_type != FragmentType::Fixed;
                let aggregate_build =
                    peel_identity_projections_to_aggregate(&original_right_for_estimate).is_some();

                let partitioning = if partitioned_semi_join_enabled()
                    && schemas_align
                    && partitionable_children
                    && (!aggregate_build || prefer_hash_aggregate_build)
                {
                    left_key_index.zip(right_key_index)
                } else {
                    None
                };

                if let Some((left_key_index, right_key_index)) = partitioning {
                    let estimated_rows = self.stats.as_deref().map(|stats| {
                        let l =
                            crate::cost::estimated_cardinality(&original_left_for_estimate, stats);
                        let r =
                            crate::cost::estimated_cardinality(&original_right_for_estimate, stats);
                        l.max(r).ceil() as u64
                    });
                    let partition_count = choose_partition_count(
                        self.worker_count,
                        estimated_rows,
                        self.hash_partition_target_rows,
                        self.max_hash_partitions,
                    );

                    left_frags[0].output_partitioning = PartitioningScheme::Hash {
                        columns: vec![left_key_index],
                        partition_count,
                    };
                    right_frags[0].output_partitioning = PartitioningScheme::Hash {
                        columns: vec![right_key_index],
                        partition_count,
                    };
                    left_frags.append(&mut right_frags);

                    let join_plan = LogicalPlan::AntiJoin {
                        left: Box::new(left_plan),
                        right: Box::new(right_plan),
                        left_key,
                        right_key,
                        residual,
                    };
                    let output_schema = join_plan.schema();

                    let join_fragment = PlanFragment {
                        id: self.next_id(),
                        fragment_type: FragmentType::HashPartitioned,
                        root: join_plan,
                        output_partitioning: PartitioningScheme::Hash {
                            columns: Vec::new(),
                            partition_count,
                        },
                        source_fragments: left_frags,
                    };

                    let exchange = LogicalPlan::ExchangeNode {
                        stage_id: join_fragment.id,
                        schema: output_schema,
                    };
                    return (exchange, vec![join_fragment]);
                }

                left_frags.append(&mut right_frags);

                let join_plan = LogicalPlan::AntiJoin {
                    left: Box::new(left_plan),
                    right: Box::new(right_plan),
                    left_key,
                    right_key,
                    residual,
                };
                let output_schema = join_plan.schema();

                let join_fragment = PlanFragment {
                    id: self.next_id(),
                    fragment_type: FragmentType::Fixed,
                    root: join_plan,
                    output_partitioning: PartitioningScheme::Single,
                    source_fragments: left_frags,
                };

                let exchange = LogicalPlan::ExchangeNode {
                    stage_id: join_fragment.id,
                    schema: output_schema,
                };
                (exchange, vec![join_fragment])
            }

            LogicalPlan::Aggregate {
                input,
                group_by,
                aggr_exprs,
                schema,
            } => {
                // Two-phase aggregation split (2026-05-26): when ALL
                // aggregate functions are decomposable (SUM, COUNT,
                // MIN, MAX) we split into PartialAggregate (worker) +
                // FinalAggregate (coord). The worker emits one row per
                // (partition × group) instead of every input row; the
                // coord combines partials.
                //
                // Cuts coord-side network bytes + HT memory by orders
                // of magnitude when GROUP BY cardinality is small
                // (Q01 4 groups, Q09 25 groups, Q14 1 group, etc).
                //
                // AVG / COUNT(DISTINCT) require multi-output partial
                // state (sum + count) or extra hashing and aren't
                // worth the per-query complexity vs. just running the
                // existing single-phase aggregate. Those fall back to
                // the old behavior — `is_decomposable_for_split`
                // returns false and we keep the original Aggregate.
                let input = *input;
                let estimated_rows = self
                    .stats
                    .as_deref()
                    .map(|stats| crate::cost::estimated_cardinality(&input, stats).ceil() as u64);
                let (input_plan, mut input_frags) = self.split(input);
                if !is_decomposable_for_split(&aggr_exprs) {
                    return (
                        LogicalPlan::Aggregate {
                            input: Box::new(input_plan),
                            group_by,
                            aggr_exprs,
                            schema,
                        },
                        input_frags,
                    );
                }

                // Partial output schema = group_cols (positions 0..G)
                // followed by aggregate output cols (positions G..G+A).
                // For SUM/COUNT/MIN/MAX the partial output type matches
                // the original aggregate's output type, so the partial
                // schema columns can reuse the final-aggregate's output
                // schema entries.
                let n_group = group_by.len();
                let partial_schema = schema.clone();

                // Final aggregate: group_by columns now reference
                // positions 0..G in the partial output. Aggregate exprs
                // are remapped: COUNT(*) / COUNT(col) -> SUM(partial_col)
                // because the partial step already counted rows; the
                // final step just sums the per-partition counts. SUM /
                // MIN / MAX keep their function names (idempotent).
                // Shared by both the fused (map-side) and separate-fragment
                // paths below.
                let final_group_by: Vec<PlanExpr> = group_by
                    .iter()
                    .enumerate()
                    .map(|(i, g)| PlanExpr::Column {
                        index: i,
                        name: column_name_for_expr(g, &partial_schema, i),
                        span: None,
                    })
                    .collect();
                let final_aggr_exprs: Vec<PlanExpr> = aggr_exprs
                    .iter()
                    .enumerate()
                    .map(|(i, a)| build_final_aggr_expr(a, n_group + i, &partial_schema))
                    .collect();

                // A1 map-side fuse (2026-06-10): when the aggregate's input
                // is a single scan SOURCE fragment, fold the PartialAggregate
                // INTO it so it aggregates the scan in-process and emits
                // partials (O(groups) rows), instead of a separate
                // `output=Single` fragment that gathers every input row
                // through a Flight exchange (q01's 177M-row / ~57 MB/s SF30
                // bottleneck). Mirrors the Filter push-down above: the
                // partial-agg's input subtree is inlined over the source's
                // own root, dropping the intervening ExchangeNode.
                let parallel_final = (parallel_final_agg_enabled()
                    || self.force_parallel_final_agg)
                    && n_group > 0
                    && parent_projection_preserves_natural_aggregate_order(
                        parent_aggregate_projection,
                        n_group,
                        aggr_exprs.len(),
                    );
                let partial_hash_keys: Vec<usize> = (0..n_group).collect();
                let partition_count = choose_partition_count(
                    self.worker_count,
                    estimated_rows,
                    self.hash_partition_target_rows,
                    self.max_hash_partitions,
                );
                let partial_output_partitioning = if parallel_final {
                    PartitioningScheme::Hash {
                        columns: partial_hash_keys.clone(),
                        partition_count,
                    }
                } else {
                    PartitioningScheme::Single
                };
                let final_aggregate = |partial_exchange: LogicalPlan| LogicalPlan::FinalAggregate {
                    input: Box::new(partial_exchange),
                    group_by: final_group_by.clone(),
                    aggr_exprs: final_aggr_exprs.clone(),
                    schema: schema.clone(),
                };
                let finish_aggregate_split = |fragmenter: &mut Self,
                                              partial_exchange: LogicalPlan,
                                              partial_fragments: Vec<PlanFragment>|
                 -> (LogicalPlan, Vec<PlanFragment>) {
                    let final_plan = final_aggregate(partial_exchange);
                    if !parallel_final {
                        return (final_plan, partial_fragments);
                    }

                    let final_schema = final_plan.schema();
                    let final_fragment = PlanFragment {
                        id: fragmenter.next_id(),
                        fragment_type: FragmentType::HashPartitioned,
                        root: final_plan,
                        // Empty hash columns means "N-way consumer,
                        // single output partition per task" in the
                        // existing distributed hash-join scheduler.
                        output_partitioning: PartitioningScheme::Hash {
                            columns: Vec::new(),
                            partition_count,
                        },
                        source_fragments: partial_fragments,
                    };
                    let exchange = LogicalPlan::ExchangeNode {
                        stage_id: final_fragment.id,
                        schema: final_schema,
                    };
                    (exchange, vec![final_fragment])
                };
                if input_frags.len() == 1 && input_frags[0].fragment_type == FragmentType::Source {
                    let src_id = input_frags[0].id;
                    let fused =
                        inline_source_exchange(input_plan.clone(), src_id, &input_frags[0].root);
                    if let Some(fused_input) = fused {
                        let mut src = input_frags.pop().unwrap();
                        src.root = LogicalPlan::PartialAggregate {
                            input: Box::new(fused_input),
                            group_by: group_by.clone(),
                            aggr_exprs: aggr_exprs.clone(),
                            schema: partial_schema.clone(),
                        };
                        src.output_partitioning = partial_output_partitioning.clone();
                        let partial_exchange = LogicalPlan::ExchangeNode {
                            stage_id: src.id,
                            schema: partial_schema,
                        };
                        return finish_aggregate_split(self, partial_exchange, vec![src]);
                    }
                }

                // q08 partial-over-join (default off): when a decomposable
                // grouped aggregate consumes the output of a single worker-side
                // join fragment, push the PartialAggregate into that fragment
                // so join batches are reduced before the fragment exchange.
                // The coordinator still runs the FinalAggregate and any parent
                // Projection computes final-only expressions such as num/den.
                if partial_agg_over_join_enabled()
                    && input_frags.len() == 1
                    && input_frags[0].fragment_type != FragmentType::Fixed
                    && matches!(input_frags[0].root, LogicalPlan::Join { .. })
                {
                    let join_id = input_frags[0].id;
                    if let Some(fused_input) =
                        inline_source_exchange(input_plan.clone(), join_id, &input_frags[0].root)
                    {
                        let mut join_fragment = input_frags.pop().unwrap();
                        join_fragment.root = LogicalPlan::PartialAggregate {
                            input: Box::new(fused_input),
                            group_by: group_by.clone(),
                            aggr_exprs: aggr_exprs.clone(),
                            schema: partial_schema.clone(),
                        };
                        join_fragment.output_partitioning = partial_output_partitioning.clone();
                        let partial_exchange = LogicalPlan::ExchangeNode {
                            stage_id: join_fragment.id,
                            schema: partial_schema,
                        };
                        return finish_aggregate_split(self, partial_exchange, vec![join_fragment]);
                    }
                }

                // Separate-fragment path: the input isn't a single scan
                // source (e.g. a post-join aggregate, whose input is already
                // reduced). Keep the partial in its own single-task fragment.
                let partial_aggregate = LogicalPlan::PartialAggregate {
                    input: Box::new(input_plan),
                    group_by: group_by.clone(),
                    aggr_exprs: aggr_exprs.clone(),
                    schema: partial_schema.clone(),
                };

                let partial_fragment = PlanFragment {
                    id: self.next_id(),
                    fragment_type: FragmentType::Fixed,
                    root: partial_aggregate,
                    output_partitioning: partial_output_partitioning,
                    source_fragments: input_frags,
                };

                let partial_exchange = LogicalPlan::ExchangeNode {
                    stage_id: partial_fragment.id,
                    schema: partial_schema,
                };

                finish_aggregate_split(self, partial_exchange, vec![partial_fragment])
            }

            LogicalPlan::Window { input, functions } => {
                let (input_plan, input_frags) = self.split(*input);
                let window_plan = LogicalPlan::Window {
                    input: Box::new(input_plan),
                    functions,
                };

                if window_plan_has_global_function(&window_plan) {
                    let output_schema = window_plan.schema();
                    let window_fragment = PlanFragment {
                        id: self.next_id(),
                        fragment_type: FragmentType::Fixed,
                        root: window_plan,
                        output_partitioning: PartitioningScheme::Single,
                        source_fragments: input_frags,
                    };
                    let exchange = LogicalPlan::ExchangeNode {
                        stage_id: window_fragment.id,
                        schema: output_schema,
                    };
                    return (exchange, vec![window_fragment]);
                }

                (window_plan, input_frags)
            }

            LogicalPlan::Explain { input, analyze } => {
                let (input_plan, frags) = self.split(*input);
                (
                    LogicalPlan::Explain {
                        input: Box::new(input_plan),
                        analyze,
                    },
                    frags,
                )
            }

            // Pass through nodes that don't need fragmentation.
            other => (other, vec![]),
        }
    }
}

fn window_plan_has_global_function(plan: &LogicalPlan) -> bool {
    let LogicalPlan::Window { functions, .. } = plan else {
        return false;
    };
    functions
        .iter()
        .any(|function| function.partition_by.is_empty())
}

impl Default for PlanFragmenter {
    fn default() -> Self {
        Self::new()
    }
}

/// Returns true when EVERY aggregate in `aggr_exprs` is decomposable
/// into partial+final phases that arneb's single-output-column
/// `HashAggregateExec` can handle. SUM / MIN / MAX are idempotent
/// (partial and final use the same function). COUNT / COUNT(*) need
/// the final to use SUM over partial counts. DISTINCT and AVG need
/// multi-column partial state and are skipped here — caller falls
/// back to single-phase Aggregate.
/// Choose the hash-repartition fan-out (number of consumer partitions) for a
/// distributed exchange. Adaptive on cluster size and the estimated rows
/// flowing through the exchange, bounded to `[2, max_partitions]`.
///
/// `candidate = max(worker_count, ceil(estimated_rows / target_rows_per_partition))`
/// then clamped. An unknown / zero estimate (or zero target) drops the
/// cardinality term and falls back to a worker-count-only count — still
/// deterministic. See the `adaptive-partition-count` spec
/// (`openspec/changes/dist-adaptive-partition/`).
///
/// Pure and deterministic: the same inputs always yield the same N, which the
/// cell-parity gate and reproducible benches rely on.
fn choose_partition_count(
    worker_count: usize,
    estimated_rows: Option<u64>,
    target_rows_per_partition: u64,
    max_partitions: usize,
) -> usize {
    let by_workers = worker_count.max(1);
    let by_rows = match estimated_rows {
        Some(rows) if rows > 0 && target_rows_per_partition > 0 => {
            rows.div_ceil(target_rows_per_partition) as usize
        }
        // Unknown / degenerate estimate → worker-count-only (deterministic).
        _ => 0,
    };
    // Floor of 2; cap at the configured maximum (which is itself floored at 2
    // so a misconfigured `max < 2` cannot invert the clamp bounds).
    by_workers.max(by_rows).clamp(2, max_partitions.max(2))
}

/// dist-mxn-nested-joins (2026-06-05): enforce a single `partition_count` across
/// each connected hash-exchange chain so the distributed exchange's M==N
/// invariant holds.
///
/// Why this is needed: `choose_partition_count` sizes each join independently
/// from its OWN local cardinality estimate, and the fragmenter propagates a
/// parent's count onto its immediate children's `output_partitioning` only one
/// level deep. So a join's OUTPUT count (overwritten by its parent) can diverge
/// from the INPUT count its own children received (set by itself). The
/// coordinator uses the single `output_partitioning.partition_count` field for
/// BOTH `task_count` (how many buckets a consumer reads, one per parallel task)
/// AND `output_partitions` (how many buckets a producer emits). A non-uniform
/// producer→consumer boundary therefore either DROPS the high buckets
/// (parent < child → silent undercount, e.g. q09's 2/13) or pulls OUT-OF-RANGE
/// buckets (parent > child → "partition k already consumed" hard error, e.g.
/// q07/q08). At the historical fixed N=2 every stage is 2, so M==N held trivially.
///
/// A "chain" is a maximal set of fragments with `Hash` output partitioning
/// connected by parent→child edges where BOTH endpoints are hash. `Broadcast`,
/// `Single`, `RoundRobin` and non-hash fragments are chain boundaries (a
/// broadcast build is subscribed in full, not pulled per-partition; a
/// single-stream producer is pulled at partition 0). Each chain is set to its
/// own MAX `partition_count` — sized to the largest intermediate, which
/// over-partitions the small joins harmlessly (empty buckets are free) while
/// never under-partitioning the big ones. Only `partition_count` is changed;
/// hash columns and the partitioning scheme are untouched. Idempotent: a chain
/// that is already uniform (e.g. the fixed-N=2 case) is unchanged.
pub fn normalize_chain_partition_counts(frag: &mut PlanFragment) {
    if is_hash_output(frag) {
        // `frag` is the top of a hash chain (its parent is non-hash or it is
        // the root). Size the whole connected hash component to its max.
        let max = chain_max_count(frag);
        apply_chain_count(frag, max);
    } else {
        // Non-hash boundary — look for chain tops among the children.
        for child in &mut frag.source_fragments {
            normalize_chain_partition_counts(child);
        }
    }
}

fn is_hash_output(frag: &PlanFragment) -> bool {
    matches!(frag.output_partitioning, PartitioningScheme::Hash { .. })
}

/// Max `partition_count` over `frag` and its transitively hash-connected
/// descendants.
fn chain_max_count(frag: &PlanFragment) -> usize {
    let mut m = frag.output_partitioning.partition_count();
    for child in &frag.source_fragments {
        if is_hash_output(child) {
            m = m.max(chain_max_count(child));
        }
    }
    m
}

/// Set `frag` and every transitively hash-connected descendant to `n`. Non-hash
/// children are chain boundaries: recurse into them so any independent chain
/// nested below (e.g. inside a broadcast build) is normalized on its own.
fn apply_chain_count(frag: &mut PlanFragment, n: usize) {
    if let PartitioningScheme::Hash {
        partition_count, ..
    } = &mut frag.output_partitioning
    {
        *partition_count = n;
    }
    for child in &mut frag.source_fragments {
        if is_hash_output(child) {
            apply_chain_count(child, n);
        } else {
            normalize_chain_partition_counts(child);
        }
    }
}

// ---------------------------------------------------------------------------
// AVG decomposition (pre-fragmentation)
// ---------------------------------------------------------------------------

/// Pre-fragmentation rewrite: decompose `AVG(x)` into `SUM(x) / COUNT(x)`
/// so an aggregate that contains AVG becomes fully decomposable and can be
/// split into PartialAggregate (worker) + FinalAggregate (coord). Without
/// this, the AVG forces the entire aggregate onto the coordinator, which
/// then receives every input row (TPC-H Q01: 177 M rows) instead of one
/// partial row per (partition × group).
///
/// Each qualifying `Aggregate` is replaced by:
///   `Projection( …, AVG = SUM(x) / COUNT(x), … )`
///     over `Aggregate( …, SUM(x), COUNT(x), … )`
/// The inner aggregate is then SUM/COUNT-only (decomposable); the AVG
/// division lives in the projection above the FinalAggregate. The
/// projection OUTPUTs the original aggregate's schema, so upstream column
/// indices, names, and types are unchanged.
///
/// Opportunistic and conservative: an aggregate that does not qualify is
/// returned unchanged (the existing single-phase behavior), so partial
/// tree coverage never affects correctness.
fn rewrite_avg_for_split(plan: LogicalPlan) -> LogicalPlan {
    // Bottom-up: rewrite children first, then this node.
    let plan = recurse_avg_children(plan);
    match plan {
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggr_exprs,
            schema,
        } => rewrite_avg_aggregate(*input, group_by, aggr_exprs, schema),
        other => other,
    }
}

/// Recurse `rewrite_avg_for_split` into every plan child, rebuilding the
/// node. Mirrors the structural walk in `assign_dynamic_filter_ids`.
fn recurse_avg_children(plan: LogicalPlan) -> LogicalPlan {
    use LogicalPlan as L;
    match plan {
        L::TableScan { .. } | L::ExchangeNode { .. } | L::OneRow => plan,
        L::Projection {
            input,
            exprs,
            schema,
        } => L::Projection {
            input: Box::new(rewrite_avg_for_split(*input)),
            exprs,
            schema,
        },
        L::Filter { input, predicate } => L::Filter {
            input: Box::new(rewrite_avg_for_split(*input)),
            predicate,
        },
        L::Sort { input, order_by } => L::Sort {
            input: Box::new(rewrite_avg_for_split(*input)),
            order_by,
        },
        L::Limit {
            input,
            limit,
            offset,
        } => L::Limit {
            input: Box::new(rewrite_avg_for_split(*input)),
            limit,
            offset,
        },
        L::Explain { input, analyze } => L::Explain {
            input: Box::new(rewrite_avg_for_split(*input)),
            analyze,
        },
        L::Distinct { input } => L::Distinct {
            input: Box::new(rewrite_avg_for_split(*input)),
        },
        L::Window { input, functions } => L::Window {
            input: Box::new(rewrite_avg_for_split(*input)),
            functions,
        },
        L::AssignUniqueId { input, id_column } => L::AssignUniqueId {
            input: Box::new(rewrite_avg_for_split(*input)),
            id_column,
        },
        L::Aggregate {
            input,
            group_by,
            aggr_exprs,
            schema,
        } => L::Aggregate {
            input: Box::new(rewrite_avg_for_split(*input)),
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
            input: Box::new(rewrite_avg_for_split(*input)),
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
            input: Box::new(rewrite_avg_for_split(*input)),
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
            left: Box::new(rewrite_avg_for_split(*left)),
            right: Box::new(rewrite_avg_for_split(*right)),
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
        } => L::SemiJoin {
            left: Box::new(rewrite_avg_for_split(*left)),
            right: Box::new(rewrite_avg_for_split(*right)),
            left_key,
            right_key,
            residual,
            dynamic_filter_ids,
        },
        L::AntiJoin {
            left,
            right,
            left_key,
            right_key,
            residual,
        } => L::AntiJoin {
            left: Box::new(rewrite_avg_for_split(*left)),
            right: Box::new(rewrite_avg_for_split(*right)),
            left_key,
            right_key,
            residual,
        },
        L::ScalarSubquery { subplan } => L::ScalarSubquery {
            subplan: Box::new(rewrite_avg_for_split(*subplan)),
        },
        L::UnionAll { inputs } => L::UnionAll {
            inputs: inputs.into_iter().map(rewrite_avg_for_split).collect(),
        },
        L::Intersect { left, right } => L::Intersect {
            left: Box::new(rewrite_avg_for_split(*left)),
            right: Box::new(rewrite_avg_for_split(*right)),
        },
        L::Except { left, right } => L::Except {
            left: Box::new(rewrite_avg_for_split(*left)),
            right: Box::new(rewrite_avg_for_split(*right)),
        },
        L::CreateTableAsSelect { name, source } => L::CreateTableAsSelect {
            name,
            source: Box::new(rewrite_avg_for_split(*source)),
        },
        L::InsertInto { table, source } => L::InsertInto {
            table,
            source: Box::new(rewrite_avg_for_split(*source)),
        },
        L::CreateView { name, sql, plan } => L::CreateView {
            name,
            sql,
            plan: Box::new(rewrite_avg_for_split(*plan)),
        },
        L::CreateTable { .. } | L::DropTable { .. } | L::DeleteFrom { .. } | L::DropView { .. } => {
            plan
        }
    }
}

/// If `expr` is a rewritable `AVG(x)` — non-distinct, single-argument —
/// return its argument `x`. Other aggregates (and AVG DISTINCT / multi-arg)
/// return `None`.
fn rewritable_avg_arg(expr: &PlanExpr) -> Option<&PlanExpr> {
    match expr {
        PlanExpr::Function {
            name,
            args,
            distinct,
            ..
        } if !*distinct && args.len() == 1 && name.eq_ignore_ascii_case("AVG") => Some(&args[0]),
        _ => None,
    }
}

/// Replace an `Aggregate` containing AVG with `Projection(SUM/COUNT division)`
/// over an `Aggregate(SUM, COUNT)` so the inner aggregate is decomposable.
/// Returns the original `Aggregate` unchanged when the rewrite does not
/// apply (no AVG, an AVG we can't rewrite, a non-Float64 AVG output type,
/// or some other non-decomposable aggregate that would keep the split
/// blocked anyway).
fn rewrite_avg_aggregate(
    input: LogicalPlan,
    group_by: Vec<PlanExpr>,
    aggr_exprs: Vec<PlanExpr>,
    schema: Vec<ColumnInfo>,
) -> LogicalPlan {
    let n_group = group_by.len();

    // Gate: at least one rewritable AVG, every AVG output typed Float64,
    // and every non-AVG aggregate already decomposable — otherwise the
    // split stays blocked, so rewriting would only churn the plan.
    let mut has_rewritable_avg = false;
    for (j, agg) in aggr_exprs.iter().enumerate() {
        match rewritable_avg_arg(agg) {
            Some(_) => {
                // AVG over an integer column is typed Int64 by the planner
                // (a quirk vs the always-Float64 accumulator); skip it so
                // the projection's Float64 division isn't truncated.
                if schema.get(n_group + j).map(|c| &c.data_type) != Some(&DataType::Float64) {
                    return rebuild_aggregate(input, group_by, aggr_exprs, schema);
                }
                has_rewritable_avg = true;
            }
            None => {
                if !is_decomposable_for_split(std::slice::from_ref(agg)) {
                    return rebuild_aggregate(input, group_by, aggr_exprs, schema);
                }
            }
        }
    }
    if !has_rewritable_avg {
        return rebuild_aggregate(input, group_by, aggr_exprs, schema);
    }

    // Build the inner aggregate (SUM/COUNT only) and the projection that
    // reconstructs the original output layout above it.
    let mut inner_aggr_exprs: Vec<PlanExpr> = Vec::with_capacity(aggr_exprs.len() + 1);
    let mut inner_agg_cols: Vec<ColumnInfo> = Vec::with_capacity(aggr_exprs.len() + 1);
    // Projection output: group passthrough first, then one expr per
    // original aggregate column.
    let mut proj_exprs: Vec<PlanExpr> = Vec::with_capacity(schema.len());
    for (i, c) in schema.iter().take(n_group).enumerate() {
        proj_exprs.push(PlanExpr::Column {
            index: i,
            name: c.name.clone(),
            span: None,
        });
    }

    for (j, agg) in aggr_exprs.into_iter().enumerate() {
        let orig_col = schema[n_group + j].clone();
        match rewritable_avg_arg(&agg) {
            Some(arg) => {
                // SUM(x) — same declared type as the AVG output (SUM and
                // AVG share the planner's type-inference arm) → Float64.
                let sum_idx = n_group + inner_aggr_exprs.len();
                inner_aggr_exprs.push(PlanExpr::Function {
                    name: "SUM".into(),
                    args: vec![arg.clone()],
                    distinct: false,
                    span: None,
                });
                inner_agg_cols.push(ColumnInfo {
                    name: format!("__avg_sum_{j}"),
                    data_type: orig_col.data_type.clone(),
                    nullable: true,
                });
                // COUNT(x) — non-null count of the same argument.
                let count_idx = n_group + inner_aggr_exprs.len();
                inner_aggr_exprs.push(PlanExpr::Function {
                    name: "COUNT".into(),
                    args: vec![arg.clone()],
                    distinct: false,
                    span: None,
                });
                inner_agg_cols.push(ColumnInfo {
                    name: format!("__avg_count_{j}"),
                    data_type: DataType::Int64,
                    nullable: false,
                });
                // AVG = SUM / COUNT. Both casts to Float64 are required —
                // `arithmetic_op` rejects mismatched operand types.
                proj_exprs.push(PlanExpr::BinaryOp {
                    left: Box::new(cast_f64(PlanExpr::Column {
                        index: sum_idx,
                        name: inner_agg_cols[sum_idx - n_group].name.clone(),
                        span: None,
                    })),
                    op: BinaryOp::Divide,
                    right: Box::new(cast_f64(PlanExpr::Column {
                        index: count_idx,
                        name: inner_agg_cols[count_idx - n_group].name.clone(),
                        span: None,
                    })),
                    span: None,
                });
            }
            None => {
                // Decomposable aggregate carried through unchanged.
                let idx = n_group + inner_aggr_exprs.len();
                inner_aggr_exprs.push(agg);
                inner_agg_cols.push(orig_col.clone());
                proj_exprs.push(PlanExpr::Column {
                    index: idx,
                    name: orig_col.name,
                    span: None,
                });
            }
        }
    }

    // Inner aggregate schema = group columns + the rewritten aggregate
    // columns. The original `schema` becomes the projection's output, so
    // upstream indices/names/types are preserved exactly.
    let mut inner_schema: Vec<ColumnInfo> = schema[..n_group].to_vec();
    inner_schema.extend(inner_agg_cols);

    let inner_aggregate = LogicalPlan::Aggregate {
        input: Box::new(input),
        group_by,
        aggr_exprs: inner_aggr_exprs,
        schema: inner_schema,
    };

    LogicalPlan::Projection {
        input: Box::new(inner_aggregate),
        exprs: proj_exprs,
        schema,
    }
}

/// Reassemble an unchanged `Aggregate` (the no-rewrite path).
fn rebuild_aggregate(
    input: LogicalPlan,
    group_by: Vec<PlanExpr>,
    aggr_exprs: Vec<PlanExpr>,
    schema: Vec<ColumnInfo>,
) -> LogicalPlan {
    LogicalPlan::Aggregate {
        input: Box::new(input),
        group_by,
        aggr_exprs,
        schema,
    }
}

/// Wrap `expr` in a `CAST(… AS DOUBLE)`.
fn cast_f64(expr: PlanExpr) -> PlanExpr {
    PlanExpr::Cast {
        expr: Box::new(expr),
        data_type: DataType::Float64,
        span: None,
    }
}

/// Inline a single SOURCE fragment's plan in place of the `ExchangeNode`
/// that references it, walking through the schema-preserving wrappers
/// (`Projection`, `Filter`) the fragmenter may leave above a source
/// exchange. Used by the A1 map-side fuse to drop the exchange between a
/// PartialAggregate and the scan it sits on. Returns `None` if the target
/// `ExchangeNode` isn't reachable through those wrappers — the caller then
/// keeps the separate-fragment path, so this is always safe.
fn inline_source_exchange(
    plan: LogicalPlan,
    target: StageId,
    replacement: &LogicalPlan,
) -> Option<LogicalPlan> {
    match plan {
        LogicalPlan::ExchangeNode { stage_id, .. } if stage_id == target => {
            Some(replacement.clone())
        }
        LogicalPlan::Projection {
            input,
            exprs,
            schema,
        } => inline_source_exchange(*input, target, replacement).map(|inner| {
            LogicalPlan::Projection {
                input: Box::new(inner),
                exprs,
                schema,
            }
        }),
        LogicalPlan::Filter { input, predicate } => {
            inline_source_exchange(*input, target, replacement).map(|inner| LogicalPlan::Filter {
                input: Box::new(inner),
                predicate,
            })
        }
        _ => None,
    }
}

fn replace_first_exchange_or_fallback(
    plan: LogicalPlan,
    target: StageId,
    replacement: LogicalPlan,
) -> LogicalPlan {
    let (rewritten, unused_replacement) = replace_first_exchange(plan, target, replacement);
    if let Some(replacement) = unused_replacement {
        replacement
    } else {
        rewritten
    }
}

fn replace_first_exchange(
    plan: LogicalPlan,
    target: StageId,
    replacement: LogicalPlan,
) -> (LogicalPlan, Option<LogicalPlan>) {
    match plan {
        LogicalPlan::ExchangeNode { stage_id, .. } if stage_id == target => (replacement, None),
        LogicalPlan::Projection {
            input,
            exprs,
            schema,
        } => {
            let (input, replacement) = replace_first_exchange(*input, target, replacement);
            (
                LogicalPlan::Projection {
                    input: Box::new(input),
                    exprs,
                    schema,
                },
                replacement,
            )
        }
        LogicalPlan::Filter { input, predicate } => {
            let (input, replacement) = replace_first_exchange(*input, target, replacement);
            (
                LogicalPlan::Filter {
                    input: Box::new(input),
                    predicate,
                },
                replacement,
            )
        }
        LogicalPlan::Sort { input, order_by } => {
            let (input, replacement) = replace_first_exchange(*input, target, replacement);
            (
                LogicalPlan::Sort {
                    input: Box::new(input),
                    order_by,
                },
                replacement,
            )
        }
        LogicalPlan::Limit {
            input,
            limit,
            offset,
        } => {
            let (input, replacement) = replace_first_exchange(*input, target, replacement);
            (
                LogicalPlan::Limit {
                    input: Box::new(input),
                    limit,
                    offset,
                },
                replacement,
            )
        }
        LogicalPlan::Distinct { input } => {
            let (input, replacement) = replace_first_exchange(*input, target, replacement);
            (
                LogicalPlan::Distinct {
                    input: Box::new(input),
                },
                replacement,
            )
        }
        LogicalPlan::Window { input, functions } => {
            let (input, replacement) = replace_first_exchange(*input, target, replacement);
            (
                LogicalPlan::Window {
                    input: Box::new(input),
                    functions,
                },
                replacement,
            )
        }
        LogicalPlan::AssignUniqueId { input, id_column } => {
            let (input, replacement) = replace_first_exchange(*input, target, replacement);
            (
                LogicalPlan::AssignUniqueId {
                    input: Box::new(input),
                    id_column,
                },
                replacement,
            )
        }
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggr_exprs,
            schema,
        } => {
            let (input, replacement) = replace_first_exchange(*input, target, replacement);
            (
                LogicalPlan::Aggregate {
                    input: Box::new(input),
                    group_by,
                    aggr_exprs,
                    schema,
                },
                replacement,
            )
        }
        LogicalPlan::PartialAggregate {
            input,
            group_by,
            aggr_exprs,
            schema,
        } => {
            let (input, replacement) = replace_first_exchange(*input, target, replacement);
            (
                LogicalPlan::PartialAggregate {
                    input: Box::new(input),
                    group_by,
                    aggr_exprs,
                    schema,
                },
                replacement,
            )
        }
        LogicalPlan::FinalAggregate {
            input,
            group_by,
            aggr_exprs,
            schema,
        } => {
            let (input, replacement) = replace_first_exchange(*input, target, replacement);
            (
                LogicalPlan::FinalAggregate {
                    input: Box::new(input),
                    group_by,
                    aggr_exprs,
                    schema,
                },
                replacement,
            )
        }
        LogicalPlan::Explain { input, analyze } => {
            let (input, replacement) = replace_first_exchange(*input, target, replacement);
            (
                LogicalPlan::Explain {
                    input: Box::new(input),
                    analyze,
                },
                replacement,
            )
        }
        other => (other, Some(replacement)),
    }
}

fn is_decomposable_for_split(aggr_exprs: &[PlanExpr]) -> bool {
    if aggr_exprs.is_empty() {
        return false;
    }
    aggr_exprs.iter().all(|e| match e {
        PlanExpr::Function { name, distinct, .. } => {
            if *distinct {
                return false;
            }
            matches!(
                name.to_uppercase().as_str(),
                "SUM" | "COUNT" | "MIN" | "MAX"
            )
        }
        _ => false,
    })
}

/// Build the FinalAggregate's aggr_expr referencing the partial output
/// at `partial_idx`. SUM / MIN / MAX keep their name (idempotent over
/// the same operation); COUNT becomes SUM (the partial counted rows;
/// the final sums the per-partition counts).
fn build_final_aggr_expr(
    original: &PlanExpr,
    partial_idx: usize,
    partial_schema: &[ColumnInfo],
) -> PlanExpr {
    let PlanExpr::Function { name, .. } = original else {
        return original.clone();
    };
    let final_name = match name.to_uppercase().as_str() {
        "COUNT" => "SUM".to_string(),
        _ => name.clone(),
    };
    let col_name = partial_schema
        .get(partial_idx)
        .map(|c| c.name.clone())
        .unwrap_or_default();
    PlanExpr::Function {
        name: final_name,
        args: vec![PlanExpr::Column {
            index: partial_idx,
            name: col_name,
            span: None,
        }],
        distinct: false,
        span: None,
    }
}

/// Best-effort column name for a group-by expression at `final_idx` in
/// the partial output. Used for display/serialization only; the index
/// is what matters at runtime.
fn column_name_for_expr(
    expr: &PlanExpr,
    partial_schema: &[ColumnInfo],
    final_idx: usize,
) -> String {
    if let PlanExpr::Column { name, .. } = expr {
        return name.clone();
    }
    partial_schema
        .get(final_idx)
        .map(|c| c.name.clone())
        .unwrap_or_default()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AggregateOutputColumn {
    Group(usize),
    Aggregate(usize),
}

fn aggregate_projection_output_order(
    exprs: &[PlanExpr],
    group_count: usize,
    aggr_count: usize,
) -> Option<Vec<AggregateOutputColumn>> {
    exprs
        .iter()
        .map(|expr| match expr {
            PlanExpr::Column { index, .. } if *index < group_count => {
                Some(AggregateOutputColumn::Group(*index))
            }
            PlanExpr::Column { index, .. } if *index < group_count + aggr_count => {
                Some(AggregateOutputColumn::Aggregate(*index - group_count))
            }
            _ => None,
        })
        .collect()
}

fn parent_projection_preserves_natural_aggregate_order(
    parent_projection: Option<&[PlanExpr]>,
    group_count: usize,
    aggr_count: usize,
) -> bool {
    let Some(exprs) = parent_projection else {
        return true;
    };
    let Some(output_order) = aggregate_projection_output_order(exprs, group_count, aggr_count)
    else {
        return true;
    };
    let natural_order: Vec<AggregateOutputColumn> = (0..group_count)
        .map(AggregateOutputColumn::Group)
        .chain((0..aggr_count).map(AggregateOutputColumn::Aggregate))
        .collect();
    output_order == natural_order
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::{PlanExpr, WindowFunctionDef};
    use arneb_common::types::{ColumnInfo, DataType, ScalarValue, TableReference};
    use arneb_sql_parser::ast;

    fn scan(name: &str) -> LogicalPlan {
        LogicalPlan::TableScan {
            table: TableReference::table(name),
            schema: vec![ColumnInfo {
                name: "id".into(),
                data_type: DataType::Int32,
                nullable: false,
            }],
            alias: None,
            properties: Default::default(),
            dynamic_filters_consumed: Vec::new(),
        }
    }

    fn scan_with_columns(name: &str, columns: &[&str]) -> LogicalPlan {
        LogicalPlan::TableScan {
            table: TableReference::table(name),
            schema: columns
                .iter()
                .map(|column| ColumnInfo {
                    name: (*column).into(),
                    data_type: DataType::Int32,
                    nullable: false,
                })
                .collect(),
            alias: None,
            properties: Default::default(),
            dynamic_filters_consumed: Vec::new(),
        }
    }

    fn qualified_scan(name: &str, columns: &[(&str, DataType)]) -> LogicalPlan {
        LogicalPlan::TableScan {
            table: TableReference::parse(name).expect("valid table reference"),
            schema: columns
                .iter()
                .map(|(column, data_type)| ColumnInfo {
                    name: (*column).into(),
                    data_type: data_type.clone(),
                    nullable: false,
                })
                .collect(),
            alias: None,
            properties: Default::default(),
            dynamic_filters_consumed: Vec::new(),
        }
    }

    fn count_table_scans_in_plan(plan: &LogicalPlan, table_name: &str) -> usize {
        match plan {
            LogicalPlan::TableScan { table, .. } => usize::from(table.table == table_name),
            LogicalPlan::Projection { input, .. }
            | LogicalPlan::Filter { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Explain { input, .. }
            | LogicalPlan::Distinct { input }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::PartialAggregate { input, .. }
            | LogicalPlan::FinalAggregate { input, .. }
            | LogicalPlan::Window { input, .. }
            | LogicalPlan::AssignUniqueId { input, .. } => {
                count_table_scans_in_plan(input, table_name)
            }
            LogicalPlan::Join { left, right, .. }
            | LogicalPlan::SemiJoin { left, right, .. }
            | LogicalPlan::AntiJoin { left, right, .. }
            | LogicalPlan::Intersect { left, right }
            | LogicalPlan::Except { left, right } => {
                count_table_scans_in_plan(left, table_name)
                    + count_table_scans_in_plan(right, table_name)
            }
            LogicalPlan::UnionAll { inputs } => inputs
                .iter()
                .map(|input| count_table_scans_in_plan(input, table_name))
                .sum(),
            LogicalPlan::ScalarSubquery { subplan } => {
                count_table_scans_in_plan(subplan, table_name)
            }
            LogicalPlan::CreateTableAsSelect { source, .. }
            | LogicalPlan::InsertInto { source, .. } => {
                count_table_scans_in_plan(source, table_name)
            }
            LogicalPlan::CreateView { plan, .. } => count_table_scans_in_plan(plan, table_name),
            LogicalPlan::ExchangeNode { .. }
            | LogicalPlan::OneRow
            | LogicalPlan::CreateTable { .. }
            | LogicalPlan::DropTable { .. }
            | LogicalPlan::DeleteFrom { .. }
            | LogicalPlan::DropView { .. } => 0,
        }
    }

    fn count_table_scans_in_fragments(fragment: &PlanFragment, table_name: &str) -> usize {
        count_table_scans_in_plan(&fragment.root, table_name)
            + fragment
                .source_fragments
                .iter()
                .map(|source| count_table_scans_in_fragments(source, table_name))
                .sum::<usize>()
    }

    fn find_source_scan_fragment<'a>(
        fragment: &'a PlanFragment,
        table_name: &str,
    ) -> Option<&'a PlanFragment> {
        if fragment.fragment_type == FragmentType::Source
            && count_table_scans_in_plan(&fragment.root, table_name) > 0
        {
            return Some(fragment);
        }
        fragment
            .source_fragments
            .iter()
            .find_map(|source| find_source_scan_fragment(source, table_name))
    }

    fn table_scan_reference<'a>(
        plan: &'a LogicalPlan,
        table_name: &str,
    ) -> Option<&'a TableReference> {
        match plan {
            LogicalPlan::TableScan { table, .. } if table.table == table_name => Some(table),
            LogicalPlan::Projection { input, .. }
            | LogicalPlan::Filter { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Explain { input, .. }
            | LogicalPlan::Distinct { input }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::PartialAggregate { input, .. }
            | LogicalPlan::FinalAggregate { input, .. }
            | LogicalPlan::Window { input, .. }
            | LogicalPlan::AssignUniqueId { input, .. } => table_scan_reference(input, table_name),
            LogicalPlan::Join { left, right, .. }
            | LogicalPlan::SemiJoin { left, right, .. }
            | LogicalPlan::AntiJoin { left, right, .. }
            | LogicalPlan::Intersect { left, right }
            | LogicalPlan::Except { left, right } => table_scan_reference(left, table_name)
                .or_else(|| table_scan_reference(right, table_name)),
            LogicalPlan::UnionAll { inputs } => inputs
                .iter()
                .find_map(|input| table_scan_reference(input, table_name)),
            LogicalPlan::ScalarSubquery { subplan } => table_scan_reference(subplan, table_name),
            LogicalPlan::CreateTableAsSelect { source, .. }
            | LogicalPlan::InsertInto { source, .. } => table_scan_reference(source, table_name),
            LogicalPlan::CreateView { plan, .. } => table_scan_reference(plan, table_name),
            _ => None,
        }
    }

    #[test]
    fn fragment_simple_scan() {
        let mut frag = PlanFragmenter::new();
        let result = frag.fragment(scan("t"));
        // Root is FIXED, with one SOURCE child.
        assert_eq!(result.fragment_type, FragmentType::Fixed);
        assert_eq!(result.source_fragments.len(), 1);
        assert_eq!(
            result.source_fragments[0].fragment_type,
            FragmentType::Source
        );
        assert!(matches!(result.root, LogicalPlan::ExchangeNode { .. }));
    }

    #[test]
    fn fragment_filter_scan_pushes_filter_to_worker() {
        // W1 MVP (2026-05-20): Filter sitting directly above TableScan is
        // pushed INTO the source fragment so workers emit already-filtered
        // rows. Coord sees just `ExchangeNode` at root level.
        let plan = LogicalPlan::Filter {
            input: Box::new(scan("t")),
            predicate: PlanExpr::Literal {
                value: ScalarValue::Boolean(true),
                span: None,
            },
        };
        let mut frag = PlanFragmenter::new();
        let result = frag.fragment(plan);
        assert_eq!(result.fragment_type, FragmentType::Fixed);
        assert_eq!(result.source_fragments.len(), 1);
        // Root collapsed to the ExchangeNode placeholder — Filter moved
        // into the worker fragment below.
        assert!(matches!(result.root, LogicalPlan::ExchangeNode { .. }));
        assert!(matches!(
            result.source_fragments[0].root,
            LogicalPlan::Filter { .. }
        ));
    }

    #[test]
    fn fragment_column_subset_projection_scan_stays_in_source_fragment() {
        let _fold = set_minimal_join_carry_fragment_fold_for_test(true);
        let plan = LogicalPlan::Projection {
            input: Box::new(scan_with_columns("t", &["id", "payload", "dead"])),
            exprs: vec![
                PlanExpr::Column {
                    index: 0,
                    name: "id".to_string(),
                    span: None,
                },
                PlanExpr::Column {
                    index: 1,
                    name: "payload".to_string(),
                    span: None,
                },
            ],
            schema: vec![
                ColumnInfo {
                    name: "id".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                },
                ColumnInfo {
                    name: "payload".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                },
            ],
        };

        let mut frag = PlanFragmenter::new();
        let result = frag.fragment(plan);

        assert_eq!(result.source_fragments.len(), 1);
        assert!(
            matches!(result.root, LogicalPlan::ExchangeNode { .. }),
            "projection should be folded below the exchange"
        );
        assert!(
            matches!(
                result.source_fragments[0].root,
                LogicalPlan::Projection { .. }
            ),
            "source fragment should own the column-subset projection"
        );
        assert_eq!(result.root.schema().len(), 2);
        assert_eq!(result.source_fragments[0].root.schema().len(), 2);
    }

    #[test]
    fn fragment_join_creates_dedicated_join_fragment() {
        // W3-Join (2026-05-20): a Join becomes its own worker-side
        // fragment, with the two scan-source fragments as children of
        // that join fragment. The ROOT fragment sees only an
        // `ExchangeNode` pointing to the join fragment.
        let plan = LogicalPlan::Join {
            left: Box::new(scan("left_table")),
            right: Box::new(scan("right_table")),
            join_type: ast::JoinType::Inner,
            condition: crate::plan::JoinCondition::None,
            dynamic_filter_ids: Vec::new(),
        };
        let mut frag = PlanFragmenter::new();
        let result = frag.fragment(plan);
        // Root has exactly one source: the join fragment.
        assert_eq!(result.source_fragments.len(), 1);
        assert!(matches!(result.root, LogicalPlan::ExchangeNode { .. }));
        // The join fragment's root is the Join node, and it has two
        // children (one per scan).
        assert!(matches!(
            result.source_fragments[0].root,
            LogicalPlan::Join { .. }
        ));
        assert_eq!(result.source_fragments[0].source_fragments.len(), 2);
    }

    #[test]
    fn fragment_global_window_over_grouped_scan_keeps_scan_as_single_source_fragment() {
        let lineitem = LogicalPlan::Filter {
            input: Box::new(qualified_scan(
                "datalake.tpch.lineitem",
                &[
                    ("l_suppkey", DataType::Int32),
                    ("l_extendedprice", DataType::Float64),
                    ("l_discount", DataType::Float64),
                ],
            )),
            predicate: PlanExpr::Literal {
                value: ScalarValue::Boolean(true),
                span: None,
            },
        };
        let revenue_schema = vec![
            ColumnInfo {
                name: "supplier_no".into(),
                data_type: DataType::Int32,
                nullable: false,
            },
            ColumnInfo {
                name: "total_revenue".into(),
                data_type: DataType::Float64,
                nullable: false,
            },
        ];
        let revenue = LogicalPlan::Aggregate {
            input: Box::new(lineitem),
            group_by: vec![column(0, "l_suppkey")],
            aggr_exprs: vec![PlanExpr::Function {
                name: "SUM".into(),
                args: vec![column(1, "l_extendedprice")],
                distinct: false,
                span: None,
            }],
            schema: revenue_schema,
        };
        let windowed_revenue = LogicalPlan::Window {
            input: Box::new(revenue),
            functions: vec![WindowFunctionDef {
                name: "MAX".into(),
                args: vec![column(1, "total_revenue")],
                partition_by: Vec::new(),
                order_by: Vec::new(),
                output_name: "__cte_max".into(),
            }],
        };
        let plan = LogicalPlan::Join {
            left: Box::new(scan_with_columns("supplier", &["s_suppkey"])),
            right: Box::new(windowed_revenue),
            join_type: ast::JoinType::Inner,
            condition: JoinCondition::On(PlanExpr::BinaryOp {
                left: Box::new(column(0, "s_suppkey")),
                op: ast::BinaryOp::Eq,
                right: Box::new(PlanExpr::Column {
                    index: 1,
                    name: "supplier_no".into(),
                    span: None,
                }),
                span: None,
            }),
            dynamic_filter_ids: Vec::new(),
        };

        let mut frag = PlanFragmenter::new();
        let result = frag.fragment(plan);

        assert_eq!(
            count_table_scans_in_fragments(&result, "lineitem"),
            1,
            "window-wrapped revenue CTE must not duplicate or orphan the lineitem scan"
        );
        let lineitem_source = find_source_scan_fragment(&result, "lineitem")
            .expect("lineitem scan should be owned by a Source fragment");
        let table = table_scan_reference(&lineitem_source.root, "lineitem")
            .expect("source fragment should contain the lineitem scan");
        assert_eq!(table.catalog.as_deref(), Some("datalake"));
        assert_eq!(table.schema.as_deref(), Some("tpch"));
        assert_eq!(table.table, "lineitem");
    }

    fn column(index: usize, name: &str) -> PlanExpr {
        PlanExpr::Column {
            index,
            name: name.into(),
            span: None,
        }
    }

    fn semi_join_plan() -> LogicalPlan {
        LogicalPlan::SemiJoin {
            left: Box::new(scan_with_columns("left_table", &["l_id", "l_payload"])),
            right: Box::new(scan_with_columns("right_table", &["r_payload", "r_id"])),
            left_key: column(0, "l_id"),
            right_key: column(1, "r_id"),
            residual: None,
            dynamic_filter_ids: Vec::new(),
        }
    }

    fn anti_join_plan() -> LogicalPlan {
        LogicalPlan::AntiJoin {
            left: Box::new(scan_with_columns("left_table", &["l_id", "l_payload"])),
            right: Box::new(scan_with_columns("right_table", &["r_payload", "r_id"])),
            left_key: column(0, "l_id"),
            right_key: column(1, "r_id"),
            residual: None,
        }
    }

    fn semi_join_with_aggregate_build() -> LogicalPlan {
        let aggregate_schema = vec![
            ColumnInfo {
                name: "r_id".into(),
                data_type: DataType::Int32,
                nullable: false,
            },
            ColumnInfo {
                name: "sum_payload".into(),
                data_type: DataType::Int32,
                nullable: false,
            },
        ];
        LogicalPlan::SemiJoin {
            left: Box::new(scan_with_columns("left_table", &["l_id", "l_payload"])),
            right: Box::new(LogicalPlan::Aggregate {
                input: Box::new(scan_with_columns("right_table", &["r_payload", "r_id"])),
                group_by: vec![column(1, "r_id")],
                aggr_exprs: vec![PlanExpr::Function {
                    name: "SUM".into(),
                    args: vec![column(0, "r_payload")],
                    distinct: false,
                    span: None,
                }],
                schema: aggregate_schema,
            }),
            left_key: column(0, "l_id"),
            right_key: column(0, "r_id"),
            residual: None,
            dynamic_filter_ids: Vec::new(),
        }
    }

    fn stats_for_aggregate_build(build_rows: u64, build_ndv: u64) -> crate::cost::CatalogStats {
        let mut columns = std::collections::HashMap::new();
        columns.insert(
            "r_id".to_string(),
            arneb_catalog::ColumnStatistics {
                ndv: Some(build_ndv),
                ..arneb_catalog::ColumnStatistics::default()
            },
        );

        let mut stats = crate::cost::CatalogStats::new();
        stats.insert(
            TableReference::table("left_table"),
            arneb_catalog::TableStatistics {
                row_count: Some(10_000_000),
                size_bytes: Some(80_000_000),
                columns: std::collections::HashMap::new(),
            },
        );
        stats.insert(
            TableReference::table("right_table"),
            arneb_catalog::TableStatistics {
                row_count: Some(build_rows),
                size_bytes: Some(build_rows * 8),
                columns,
            },
        );
        stats
    }

    fn assert_hash_partitioning(
        partitioning: &PartitioningScheme,
        expected_columns: &[usize],
    ) -> usize {
        let PartitioningScheme::Hash {
            columns,
            partition_count,
        } = partitioning
        else {
            panic!("expected Hash partitioning, got {partitioning:?}");
        };
        assert_eq!(columns, expected_columns);
        assert!(
            *partition_count >= 2,
            "partitioned semi/anti joins should use at least 2 partitions"
        );
        *partition_count
    }

    #[test]
    fn high_cardinality_aggregate_semi_build_prefers_hash_when_gate_enabled() {
        let _gate = set_partitioned_semi_join_for_test(true);
        let _pfa = set_parallel_final_agg_for_test(false);
        let mut frag = PlanFragmenter::new()
            .with_worker_count(4)
            .with_partition_policy(1_000_000, 64)
            .with_broadcast_threshold(Some(512 * 1024 * 1024))
            .with_stats(Some(std::sync::Arc::new(stats_for_aggregate_build(
                8_000_000, 5_000_000,
            ))));
        let result = frag.fragment(semi_join_with_aggregate_build());
        let join_frag = &result.source_fragments[0];

        assert_eq!(join_frag.fragment_type, FragmentType::HashPartitioned);
        assert!(matches!(join_frag.root, LogicalPlan::SemiJoin { .. }));
        assert_eq!(join_frag.source_fragments.len(), 2);
        let join_n = assert_hash_partitioning(&join_frag.output_partitioning, &[]);
        assert_hash_partitioning(&join_frag.source_fragments[0].output_partitioning, &[0]);

        let build_final = &join_frag.source_fragments[1];
        let build_n = assert_hash_partitioning(&build_final.output_partitioning, &[0]);
        assert_eq!(build_n, join_n);
        assert_eq!(build_final.fragment_type, FragmentType::HashPartitioned);
        assert!(matches!(
            build_final.root,
            LogicalPlan::FinalAggregate { .. }
        ));
        assert_eq!(build_final.source_fragments.len(), 1);
        let partial_n =
            assert_hash_partitioning(&build_final.source_fragments[0].output_partitioning, &[0]);
        assert_eq!(partial_n, join_n);
        assert!(matches!(
            build_final.source_fragments[0].root,
            LogicalPlan::PartialAggregate { .. }
        ));
    }

    #[test]
    fn small_aggregate_semi_build_does_not_trigger_hash_override() {
        let _gate = set_partitioned_semi_join_for_test(true);
        let _pfa = set_parallel_final_agg_for_test(false);
        let mut frag = PlanFragmenter::new()
            .with_worker_count(4)
            .with_partition_policy(1_000_000, 64)
            .with_broadcast_threshold(Some(512 * 1024 * 1024))
            .with_stats(Some(std::sync::Arc::new(stats_for_aggregate_build(
                10_000, 10,
            ))));
        let result = frag.fragment(semi_join_with_aggregate_build());
        let join_frag = &result.source_fragments[0];

        assert_eq!(join_frag.fragment_type, FragmentType::Fixed);
        assert_eq!(join_frag.output_partitioning, PartitioningScheme::Single);
        assert!(matches!(join_frag.root, LogicalPlan::SemiJoin { .. }));
        assert_eq!(join_frag.source_fragments.len(), 2);
        assert_eq!(
            join_frag.source_fragments[1].output_partitioning,
            PartitioningScheme::Single
        );
    }

    #[test]
    fn high_cardinality_aggregate_semi_build_gate_off_preserves_fallback_shape() {
        let _gate = set_partitioned_semi_join_for_test(false);
        let _pfa = set_parallel_final_agg_for_test(false);
        let mut frag = PlanFragmenter::new()
            .with_worker_count(4)
            .with_partition_policy(1_000_000, 64)
            .with_broadcast_threshold(Some(512 * 1024 * 1024))
            .with_stats(Some(std::sync::Arc::new(stats_for_aggregate_build(
                8_000_000, 5_000_000,
            ))));
        let result = frag.fragment(semi_join_with_aggregate_build());
        let join_frag = &result.source_fragments[0];

        assert_eq!(join_frag.fragment_type, FragmentType::Fixed);
        assert_eq!(join_frag.output_partitioning, PartitioningScheme::Single);
        assert!(matches!(join_frag.root, LogicalPlan::SemiJoin { .. }));
        assert_eq!(
            join_frag.source_fragments[1].output_partitioning,
            PartitioningScheme::Single
        );
    }

    #[test]
    fn partitioned_semi_join_hashes_both_children_when_gate_enabled() {
        let _gate = set_partitioned_semi_join_for_test(true);
        let mut frag = PlanFragmenter::new().with_worker_count(4);
        let result = frag.fragment(semi_join_plan());
        let join_frag = &result.source_fragments[0];

        assert_eq!(join_frag.fragment_type, FragmentType::HashPartitioned);
        assert!(matches!(join_frag.root, LogicalPlan::SemiJoin { .. }));
        assert_eq!(join_frag.source_fragments.len(), 2);

        let join_n = assert_hash_partitioning(&join_frag.output_partitioning, &[]);
        let left_n =
            assert_hash_partitioning(&join_frag.source_fragments[0].output_partitioning, &[0]);
        let right_n =
            assert_hash_partitioning(&join_frag.source_fragments[1].output_partitioning, &[1]);
        assert_eq!(left_n, join_n);
        assert_eq!(right_n, join_n);
    }

    #[test]
    fn partitioned_anti_join_hashes_both_children_when_gate_enabled() {
        let _gate = set_partitioned_semi_join_for_test(true);
        let mut frag = PlanFragmenter::new().with_worker_count(4);
        let result = frag.fragment(anti_join_plan());
        let join_frag = &result.source_fragments[0];

        assert_eq!(join_frag.fragment_type, FragmentType::HashPartitioned);
        assert!(matches!(join_frag.root, LogicalPlan::AntiJoin { .. }));
        assert_eq!(join_frag.source_fragments.len(), 2);

        let join_n = assert_hash_partitioning(&join_frag.output_partitioning, &[]);
        let left_n =
            assert_hash_partitioning(&join_frag.source_fragments[0].output_partitioning, &[0]);
        let right_n =
            assert_hash_partitioning(&join_frag.source_fragments[1].output_partitioning, &[1]);
        assert_eq!(left_n, join_n);
        assert_eq!(right_n, join_n);
    }

    #[test]
    fn semi_and_anti_join_stay_fixed_single_when_partition_gate_disabled() {
        let _gate = set_partitioned_semi_join_for_test(false);

        let mut semi_frag = PlanFragmenter::new();
        let semi_result = semi_frag.fragment(semi_join_plan());
        let semi_join_frag = &semi_result.source_fragments[0];
        assert_eq!(semi_join_frag.fragment_type, FragmentType::Fixed);
        assert_eq!(
            semi_join_frag.output_partitioning,
            PartitioningScheme::Single
        );
        assert!(matches!(semi_join_frag.root, LogicalPlan::SemiJoin { .. }));

        let mut anti_frag = PlanFragmenter::new();
        let anti_result = anti_frag.fragment(anti_join_plan());
        let anti_join_frag = &anti_result.source_fragments[0];
        assert_eq!(anti_join_frag.fragment_type, FragmentType::Fixed);
        assert_eq!(
            anti_join_frag.output_partitioning,
            PartitioningScheme::Single
        );
        assert!(matches!(anti_join_frag.root, LogicalPlan::AntiJoin { .. }));
    }

    #[test]
    fn partitioned_semi_join_falls_back_when_key_is_not_simple_column() {
        let _gate = set_partitioned_semi_join_for_test(true);
        let mut plan = semi_join_plan();
        if let LogicalPlan::SemiJoin { left_key, .. } = &mut plan {
            *left_key = PlanExpr::Literal {
                value: ScalarValue::Int32(1),
                span: None,
            };
        }

        let mut frag = PlanFragmenter::new().with_worker_count(4);
        let result = frag.fragment(plan);
        let join_frag = &result.source_fragments[0];

        assert_eq!(join_frag.fragment_type, FragmentType::Fixed);
        assert_eq!(join_frag.output_partitioning, PartitioningScheme::Single);
        assert!(matches!(join_frag.root, LogicalPlan::SemiJoin { .. }));
    }

    // dist-mxn-nested-joins (2026-06-05): build a synthetic fragment with a
    // hand-set output partitioning, for testing `normalize_chain_partition_counts`
    // directly without depending on cardinality estimation.
    fn hash_frag(
        id: u32,
        ftype: FragmentType,
        cols: Vec<usize>,
        n: usize,
        sources: Vec<PlanFragment>,
    ) -> PlanFragment {
        PlanFragment {
            id: StageId(id),
            fragment_type: ftype,
            root: LogicalPlan::OneRow,
            output_partitioning: PartitioningScheme::Hash {
                columns: cols,
                partition_count: n,
            },
            source_fragments: sources,
        }
    }

    fn count_of(f: &PlanFragment) -> usize {
        f.output_partitioning.partition_count()
    }

    #[test]
    fn normalize_makes_nested_join_chain_uniform_at_max() {
        // Mirrors the q09 boundary: a deep join (n=13) feeds a shallower join
        // (n=2). The coord reads `consumer.partition_count` buckets from a
        // producer that emitted `producer.partition_count` — so a non-uniform
        // chain drops buckets (parent<child) or pulls out-of-range (parent>child).
        // After normalization every hash fragment in the chain shares the max,
        // restoring the M==N exchange invariant.
        let src_a = hash_frag(0, FragmentType::Source, vec![0], 13, vec![]);
        let src_b = hash_frag(1, FragmentType::Source, vec![0], 13, vec![]);
        let join1 = hash_frag(
            2,
            FragmentType::HashPartitioned,
            vec![0],
            13,
            vec![src_a, src_b],
        );
        let src_c = hash_frag(3, FragmentType::Source, vec![0], 2, vec![]);
        // join2 reads n=2 but join1 produces n=13 → the bug.
        let join2 = hash_frag(
            4,
            FragmentType::HashPartitioned,
            vec![1],
            2,
            vec![join1, src_c],
        );
        let mut root = PlanFragment {
            id: StageId(5),
            fragment_type: FragmentType::Fixed,
            root: LogicalPlan::OneRow,
            output_partitioning: PartitioningScheme::Single,
            source_fragments: vec![join2],
        };

        normalize_chain_partition_counts(&mut root);

        // Root (non-hash) is untouched.
        assert!(matches!(
            root.output_partitioning,
            PartitioningScheme::Single
        ));
        let join2 = &root.source_fragments[0];
        assert_eq!(count_of(join2), 13, "join2 normalized to chain max");
        let join1 = &join2.source_fragments[0];
        let src_c = &join2.source_fragments[1];
        assert_eq!(count_of(join1), 13, "join1 stays at chain max");
        assert_eq!(count_of(src_c), 13, "src_c lifted to chain max");
        assert_eq!(count_of(&join1.source_fragments[0]), 13, "src_a at max");
        assert_eq!(count_of(&join1.source_fragments[1]), 13, "src_b at max");
        // Columns are NOT touched by the normalization (only counts).
        let PartitioningScheme::Hash { columns, .. } = &join2.output_partitioning else {
            panic!("join2 should stay Hash");
        };
        assert_eq!(columns, &vec![1], "join2 keys unchanged");
    }

    #[test]
    fn normalize_exempts_broadcast_build() {
        // A broadcast build is subscribed in full (not pulled per-partition), so
        // it must NOT be forced to the probe chain's count — it stays Broadcast.
        let probe = hash_frag(0, FragmentType::Source, vec![0], 8, vec![]);
        let mut build = hash_frag(1, FragmentType::Source, vec![0], 3, vec![]);
        build.output_partitioning = PartitioningScheme::Broadcast;
        let mut root = PlanFragment {
            id: StageId(2),
            fragment_type: FragmentType::HashPartitioned,
            root: LogicalPlan::OneRow,
            output_partitioning: PartitioningScheme::Hash {
                columns: vec![0],
                partition_count: 8,
            },
            source_fragments: vec![probe, build],
        };

        normalize_chain_partition_counts(&mut root);

        assert_eq!(count_of(&root), 8);
        assert_eq!(count_of(&root.source_fragments[0]), 8, "probe normalized");
        assert!(
            matches!(
                root.source_fragments[1].output_partitioning,
                PartitioningScheme::Broadcast
            ),
            "broadcast build stays Broadcast (not pulled into the chain)"
        );
    }

    #[test]
    fn normalize_is_idempotent_on_uniform_chain() {
        // The fixed-N=2 case: every fragment already shares the count → no change.
        let src_a = hash_frag(0, FragmentType::Source, vec![0], 2, vec![]);
        let src_b = hash_frag(1, FragmentType::Source, vec![0], 2, vec![]);
        let mut join = hash_frag(
            2,
            FragmentType::HashPartitioned,
            vec![0],
            2,
            vec![src_a, src_b],
        );

        normalize_chain_partition_counts(&mut join);

        assert_eq!(count_of(&join), 2);
        assert_eq!(count_of(&join.source_fragments[0]), 2);
        assert_eq!(count_of(&join.source_fragments[1]), 2);
    }

    #[test]
    fn normalize_treats_independent_chains_separately() {
        // Two hash chains under one non-hash (Single) root must NOT be merged:
        // each keeps its own max. The root is a chain boundary.
        let chain_x = hash_frag(
            0,
            FragmentType::HashPartitioned,
            vec![0],
            8,
            vec![hash_frag(1, FragmentType::Source, vec![0], 8, vec![])],
        );
        let chain_y = hash_frag(
            2,
            FragmentType::HashPartitioned,
            vec![0],
            3,
            vec![hash_frag(3, FragmentType::Source, vec![0], 3, vec![])],
        );
        let mut root = PlanFragment {
            id: StageId(4),
            fragment_type: FragmentType::Fixed,
            root: LogicalPlan::OneRow,
            output_partitioning: PartitioningScheme::Single,
            source_fragments: vec![chain_x, chain_y],
        };

        normalize_chain_partition_counts(&mut root);

        let x = &root.source_fragments[0];
        let y = &root.source_fragments[1];
        assert_eq!(count_of(x), 8, "chain X keeps its own max");
        assert_eq!(count_of(&x.source_fragments[0]), 8);
        assert_eq!(
            count_of(y),
            3,
            "chain Y keeps its own max — not merged with X"
        );
        assert_eq!(count_of(&y.source_fragments[0]), 3);
    }

    #[test]
    fn fragment_decomposable_aggregate_splits_into_partial_and_final() {
        // 2026-05-26: COUNT is decomposable (partial COUNT -> final SUM),
        // so the fragmenter splits into PartialAggregate (worker) +
        // FinalAggregate (coord) with a new fragment in between.
        let schema = vec![
            ColumnInfo {
                name: "key".into(),
                data_type: DataType::Int32,
                nullable: false,
            },
            ColumnInfo {
                name: "count".into(),
                data_type: DataType::Int64,
                nullable: false,
            },
        ];
        let plan = LogicalPlan::Aggregate {
            input: Box::new(scan_with_columns("t", &["key", "value"])),
            group_by: vec![PlanExpr::Column {
                index: 0,
                name: "key".into(),
                span: None,
            }],
            aggr_exprs: vec![PlanExpr::Function {
                name: "COUNT".into(),
                args: vec![],
                distinct: false,
                span: None,
            }],
            schema,
        };
        let mut frag = PlanFragmenter::new();
        let result = frag.fragment(plan);
        // Root is FinalAggregate wrapping an ExchangeNode placeholder.
        assert!(matches!(result.root, LogicalPlan::FinalAggregate { .. }));
        // One child: the partial-aggregate fragment.
        assert_eq!(result.source_fragments.len(), 1);
        // The partial fragment's root is PartialAggregate.
        assert!(matches!(
            result.source_fragments[0].root,
            LogicalPlan::PartialAggregate { .. }
        ));
        // Verify the COUNT was rewritten to SUM at the final step
        // (correctness gate — partial counted rows, final must sum).
        let LogicalPlan::FinalAggregate { aggr_exprs, .. } = &result.root else {
            unreachable!();
        };
        assert_eq!(aggr_exprs.len(), 1);
        let PlanExpr::Function { name, .. } = &aggr_exprs[0] else {
            panic!("expected final aggr to be a Function expr");
        };
        assert_eq!(name, "SUM", "COUNT should be rewritten to SUM in final");
    }

    fn aggregate_over_join_plan() -> LogicalPlan {
        let join = LogicalPlan::Join {
            left: Box::new(scan_with_columns("orders", &["o_orderkey", "o_year"])),
            right: Box::new(scan_with_columns("lineitem", &["l_orderkey", "volume"])),
            join_type: ast::JoinType::Inner,
            condition: JoinCondition::On(PlanExpr::BinaryOp {
                left: Box::new(column(0, "o_orderkey")),
                op: ast::BinaryOp::Eq,
                right: Box::new(column(2, "l_orderkey")),
                span: None,
            }),
            dynamic_filter_ids: Vec::new(),
        };
        LogicalPlan::Aggregate {
            input: Box::new(join),
            group_by: vec![column(1, "o_year")],
            aggr_exprs: vec![
                PlanExpr::Function {
                    name: "SUM".into(),
                    args: vec![column(3, "volume")],
                    distinct: false,
                    span: None,
                },
                PlanExpr::Function {
                    name: "SUM".into(),
                    args: vec![PlanExpr::CaseExpr {
                        operand: None,
                        when_clauses: vec![(
                            PlanExpr::Literal {
                                value: ScalarValue::Boolean(true),
                                span: None,
                            },
                            column(3, "volume"),
                        )],
                        else_result: Some(Box::new(PlanExpr::Literal {
                            value: ScalarValue::Int32(0),
                            span: None,
                        })),
                        span: None,
                    }],
                    distinct: false,
                    span: None,
                },
            ],
            schema: vec![
                ColumnInfo {
                    name: "o_year".into(),
                    data_type: DataType::Int32,
                    nullable: false,
                },
                ColumnInfo {
                    name: "sum_volume".into(),
                    data_type: DataType::Int32,
                    nullable: false,
                },
                ColumnInfo {
                    name: "sum_brazil_volume".into(),
                    data_type: DataType::Int32,
                    nullable: false,
                },
            ],
        }
    }

    #[test]
    fn partial_agg_over_join_gate_off_keeps_existing_fixed_partial_fragment() {
        let _override = set_partial_agg_over_join_for_test(false);
        let _pfa = set_parallel_final_agg_for_test(false);
        let mut frag = PlanFragmenter::new().with_worker_count(4);
        let result = frag.fragment(aggregate_over_join_plan());

        assert!(matches!(result.root, LogicalPlan::FinalAggregate { .. }));
        assert_eq!(result.source_fragments.len(), 1);
        let partial_frag = &result.source_fragments[0];
        assert_eq!(partial_frag.fragment_type, FragmentType::Fixed);
        assert!(matches!(
            partial_frag.root,
            LogicalPlan::PartialAggregate { .. }
        ));
        assert_eq!(partial_frag.source_fragments.len(), 1);
        assert!(matches!(
            partial_frag.source_fragments[0].root,
            LogicalPlan::Join { .. }
        ));
    }

    #[test]
    fn partial_agg_over_join_gate_on_fuses_partial_into_join_fragment() {
        let _override = set_partial_agg_over_join_for_test(true);
        let _pfa = set_parallel_final_agg_for_test(false);
        let mut frag = PlanFragmenter::new().with_worker_count(4);
        let result = frag.fragment(aggregate_over_join_plan());

        assert!(matches!(result.root, LogicalPlan::FinalAggregate { .. }));
        assert_eq!(result.source_fragments.len(), 1);
        let join_frag = &result.source_fragments[0];
        assert_eq!(join_frag.fragment_type, FragmentType::HashPartitioned);
        assert!(matches!(
            join_frag.output_partitioning,
            PartitioningScheme::Single
        ));
        let LogicalPlan::PartialAggregate {
            input, aggr_exprs, ..
        } = &join_frag.root
        else {
            panic!("expected PartialAggregate fused into join fragment");
        };
        assert_eq!(aggr_exprs.len(), 2);
        assert!(matches!(**input, LogicalPlan::Join { .. }));
    }

    #[test]
    fn parallel_final_agg_hash_partitions_grouped_partials_when_enabled() {
        let _override = set_parallel_final_agg_for_test(true);
        let schema = vec![
            ColumnInfo {
                name: "key".into(),
                data_type: DataType::Int32,
                nullable: false,
            },
            ColumnInfo {
                name: "sum_v".into(),
                data_type: DataType::Int64,
                nullable: false,
            },
        ];
        let plan = LogicalPlan::Aggregate {
            input: Box::new(scan_with_columns("t", &["key", "value"])),
            group_by: vec![PlanExpr::Column {
                index: 0,
                name: "key".into(),
                span: None,
            }],
            aggr_exprs: vec![PlanExpr::Function {
                name: "SUM".into(),
                args: vec![PlanExpr::Column {
                    index: 1,
                    name: "value".into(),
                    span: None,
                }],
                distinct: false,
                span: None,
            }],
            schema,
        };

        let mut frag = PlanFragmenter::new().with_worker_count(4);
        let result = frag.fragment(plan);

        assert!(matches!(result.root, LogicalPlan::ExchangeNode { .. }));
        assert_eq!(result.source_fragments.len(), 1);
        let final_frag = &result.source_fragments[0];
        assert_eq!(final_frag.fragment_type, FragmentType::HashPartitioned);
        assert!(matches!(
            final_frag.output_partitioning,
            PartitioningScheme::Hash {
                ref columns,
                partition_count
            } if columns.is_empty() && partition_count > 1
        ));
        assert!(matches!(
            final_frag.root,
            LogicalPlan::FinalAggregate { .. }
        ));
        assert_eq!(final_frag.source_fragments.len(), 1);
        assert!(matches!(
            final_frag.source_fragments[0].output_partitioning,
            PartitioningScheme::Hash {
                ref columns,
                partition_count
            } if columns == &vec![0] && partition_count > 1
        ));
    }

    #[test]
    fn test_pfa_skipped_for_interleaved_aggregate_output() {
        let _override = set_parallel_final_agg_for_test(true);
        let aggregate_schema = vec![
            ColumnInfo {
                name: "g0".into(),
                data_type: DataType::Int32,
                nullable: false,
            },
            ColumnInfo {
                name: "g1".into(),
                data_type: DataType::Int32,
                nullable: false,
            },
            ColumnInfo {
                name: "a0".into(),
                data_type: DataType::Int64,
                nullable: false,
            },
        ];
        let projection_schema = vec![
            aggregate_schema[0].clone(),
            aggregate_schema[2].clone(),
            aggregate_schema[1].clone(),
        ];
        let aggregate = LogicalPlan::Aggregate {
            input: Box::new(scan_with_columns("t", &["g0", "g1", "v"])),
            group_by: vec![
                PlanExpr::Column {
                    index: 0,
                    name: "g0".into(),
                    span: None,
                },
                PlanExpr::Column {
                    index: 1,
                    name: "g1".into(),
                    span: None,
                },
            ],
            aggr_exprs: vec![PlanExpr::Function {
                name: "SUM".into(),
                args: vec![PlanExpr::Column {
                    index: 2,
                    name: "v".into(),
                    span: None,
                }],
                distinct: false,
                span: None,
            }],
            schema: aggregate_schema,
        };
        let plan = LogicalPlan::Projection {
            input: Box::new(aggregate),
            exprs: vec![
                PlanExpr::Column {
                    index: 0,
                    name: "g0".into(),
                    span: None,
                },
                PlanExpr::Column {
                    index: 2,
                    name: "a0".into(),
                    span: None,
                },
                PlanExpr::Column {
                    index: 1,
                    name: "g1".into(),
                    span: None,
                },
            ],
            schema: projection_schema,
        };

        let mut frag = PlanFragmenter::new().with_worker_count(4);
        let result = frag.fragment(plan);

        let LogicalPlan::Projection { input, .. } = &result.root else {
            panic!("expected Projection at root, got {:?}", result.root);
        };
        assert!(
            matches!(**input, LogicalPlan::FinalAggregate { .. }),
            "interleaved output should keep FinalAggregate in the root plan, got {:?}",
            input
        );
        assert_eq!(result.source_fragments.len(), 1);
        assert!(matches!(
            result.source_fragments[0].output_partitioning,
            PartitioningScheme::Single
        ));
    }

    #[test]
    fn test_pfa_applied_for_natural_order_aggregate() {
        let _override = set_parallel_final_agg_for_test(true);
        let aggregate_schema = vec![
            ColumnInfo {
                name: "g0".into(),
                data_type: DataType::Int32,
                nullable: false,
            },
            ColumnInfo {
                name: "g1".into(),
                data_type: DataType::Int32,
                nullable: false,
            },
            ColumnInfo {
                name: "a0".into(),
                data_type: DataType::Int64,
                nullable: false,
            },
        ];
        let aggregate = LogicalPlan::Aggregate {
            input: Box::new(scan_with_columns("t", &["g0", "g1", "v"])),
            group_by: vec![
                PlanExpr::Column {
                    index: 0,
                    name: "g0".into(),
                    span: None,
                },
                PlanExpr::Column {
                    index: 1,
                    name: "g1".into(),
                    span: None,
                },
            ],
            aggr_exprs: vec![PlanExpr::Function {
                name: "SUM".into(),
                args: vec![PlanExpr::Column {
                    index: 2,
                    name: "v".into(),
                    span: None,
                }],
                distinct: false,
                span: None,
            }],
            schema: aggregate_schema.clone(),
        };
        let natural_projection = LogicalPlan::Projection {
            input: Box::new(aggregate.clone()),
            exprs: vec![
                PlanExpr::Column {
                    index: 0,
                    name: "g0".into(),
                    span: None,
                },
                PlanExpr::Column {
                    index: 1,
                    name: "g1".into(),
                    span: None,
                },
                PlanExpr::Column {
                    index: 2,
                    name: "a0".into(),
                    span: None,
                },
            ],
            schema: aggregate_schema,
        };

        let mut frag = PlanFragmenter::new().with_worker_count(4);
        let result = frag.fragment(natural_projection);
        let LogicalPlan::Projection { input, .. } = &result.root else {
            panic!("expected Projection at root, got {:?}", result.root);
        };
        assert!(
            matches!(**input, LogicalPlan::ExchangeNode { .. }),
            "natural projection should PFA-split behind an exchange, got {:?}",
            input
        );
        assert_eq!(result.source_fragments.len(), 1);
        assert_eq!(
            result.source_fragments[0].fragment_type,
            FragmentType::HashPartitioned
        );
        assert!(matches!(
            result.source_fragments[0].root,
            LogicalPlan::FinalAggregate { .. }
        ));

        let mut frag = PlanFragmenter::new().with_worker_count(4);
        let result = frag.fragment(aggregate);
        assert!(matches!(result.root, LogicalPlan::ExchangeNode { .. }));
        assert_eq!(result.source_fragments.len(), 1);
        assert_eq!(
            result.source_fragments[0].fragment_type,
            FragmentType::HashPartitioned
        );
        assert!(matches!(
            result.source_fragments[0].root,
            LogicalPlan::FinalAggregate { .. }
        ));
    }

    #[test]
    fn parallel_final_agg_default_off_keeps_single_gather() {
        let _override = set_parallel_final_agg_for_test(false);
        let schema = vec![
            ColumnInfo {
                name: "key".into(),
                data_type: DataType::Int32,
                nullable: false,
            },
            ColumnInfo {
                name: "count".into(),
                data_type: DataType::Int64,
                nullable: false,
            },
        ];
        let plan = LogicalPlan::Aggregate {
            input: Box::new(scan("t")),
            group_by: vec![PlanExpr::Column {
                index: 0,
                name: "key".into(),
                span: None,
            }],
            aggr_exprs: vec![PlanExpr::Function {
                name: "COUNT".into(),
                args: vec![],
                distinct: false,
                span: None,
            }],
            schema,
        };

        let mut frag = PlanFragmenter::new().with_worker_count(4);
        let result = frag.fragment(plan);

        assert!(matches!(result.root, LogicalPlan::FinalAggregate { .. }));
        assert_eq!(result.source_fragments.len(), 1);
        assert_eq!(
            result.source_fragments[0].output_partitioning,
            PartitioningScheme::Single
        );
    }

    #[test]
    fn parallel_final_agg_keeps_global_aggregate_single_even_when_enabled() {
        let _override = set_parallel_final_agg_for_test(true);
        let schema = vec![ColumnInfo {
            name: "count".into(),
            data_type: DataType::Int64,
            nullable: false,
        }];
        let plan = LogicalPlan::Aggregate {
            input: Box::new(scan("t")),
            group_by: vec![],
            aggr_exprs: vec![PlanExpr::Function {
                name: "COUNT".into(),
                args: vec![],
                distinct: false,
                span: None,
            }],
            schema,
        };

        let mut frag = PlanFragmenter::new().with_worker_count(4);
        let result = frag.fragment(plan);

        assert!(matches!(result.root, LogicalPlan::FinalAggregate { .. }));
        assert_eq!(result.source_fragments.len(), 1);
        assert_eq!(
            result.source_fragments[0].output_partitioning,
            PartitioningScheme::Single
        );
    }

    #[test]
    fn fragment_distinct_aggregate_stays_single_phase() {
        // COUNT(DISTINCT col) needs multi-output partial state; we don't
        // support that yet, so the fragmenter leaves it as single-phase
        // Aggregate at the coord. Falls back to the legacy behavior.
        let schema = vec![ColumnInfo {
            name: "count".into(),
            data_type: DataType::Int64,
            nullable: false,
        }];
        let plan = LogicalPlan::Aggregate {
            input: Box::new(scan("t")),
            group_by: vec![],
            aggr_exprs: vec![PlanExpr::Function {
                name: "COUNT".into(),
                args: vec![PlanExpr::Column {
                    index: 0,
                    name: "id".into(),
                    span: None,
                }],
                distinct: true,
                span: None,
            }],
            schema,
        };
        let mut frag = PlanFragmenter::new();
        let result = frag.fragment(plan);
        // Should remain single-phase Aggregate (not split).
        assert!(matches!(result.root, LogicalPlan::Aggregate { .. }));
    }

    /// A two-column scan (`key` Int32, `v` Float64) so an aggregate can
    /// AVG over a Float64 column — the q01 shape (decimals load as DOUBLE).
    fn scan_key_value(name: &str) -> LogicalPlan {
        LogicalPlan::TableScan {
            table: TableReference::table(name),
            schema: vec![
                ColumnInfo {
                    name: "key".into(),
                    data_type: DataType::Int32,
                    nullable: false,
                },
                ColumnInfo {
                    name: "v".into(),
                    data_type: DataType::Float64,
                    nullable: true,
                },
            ],
            alias: None,
            properties: Default::default(),
            dynamic_filters_consumed: Vec::new(),
        }
    }

    #[test]
    fn fragment_avg_aggregate_rewritten_to_sum_count_division_and_splits() {
        // 2026-06-10: AVG is decomposed into SUM/COUNT before
        // fragmentation, so an aggregate that was previously single-phase
        // (AVG blocks `is_decomposable_for_split`) now splits into
        // PartialAggregate (worker SUM+COUNT) + FinalAggregate (coord),
        // with AVG = SUM/COUNT computed in a Projection above the
        // FinalAggregate. Cuts coord-side rows from O(input) to O(groups).
        let schema = vec![
            ColumnInfo {
                name: "key".into(),
                data_type: DataType::Int32,
                nullable: false,
            },
            ColumnInfo {
                name: "avg_v".into(),
                data_type: DataType::Float64,
                nullable: true,
            },
        ];
        let plan = LogicalPlan::Aggregate {
            input: Box::new(scan_key_value("t")),
            group_by: vec![PlanExpr::Column {
                index: 0,
                name: "key".into(),
                span: None,
            }],
            aggr_exprs: vec![PlanExpr::Function {
                name: "AVG".into(),
                args: vec![PlanExpr::Column {
                    index: 1,
                    name: "v".into(),
                    span: None,
                }],
                distinct: false,
                span: None,
            }],
            schema,
        };
        let mut frag = PlanFragmenter::new();
        let result = frag.fragment(plan);

        // Root coord plan: Projection (the AVG division) over a
        // FinalAggregate. The projection preserves the ORIGINAL aggregate
        // output schema so upstream column indices are unchanged.
        let LogicalPlan::Projection {
            input: proj_input,
            exprs,
            schema: proj_schema,
        } = &result.root
        else {
            panic!("expected Projection at root, got {:?}", result.root);
        };
        assert_eq!(proj_schema.len(), 2, "preserves [key, avg_v] layout");
        assert_eq!(proj_schema[1].name, "avg_v");
        assert_eq!(proj_schema[1].data_type, DataType::Float64);
        // The avg output expression is a Divide (sum / count).
        assert!(
            matches!(
                exprs[1],
                PlanExpr::BinaryOp {
                    op: ast::BinaryOp::Divide,
                    ..
                }
            ),
            "avg column should be a division, got {:?}",
            exprs[1]
        );
        assert!(
            matches!(**proj_input, LogicalPlan::FinalAggregate { .. }),
            "projection should wrap a FinalAggregate"
        );

        // One child fragment: the PartialAggregate with SUM + COUNT and no
        // AVG (proving the split fired — pre-fix this stayed single-phase).
        assert_eq!(result.source_fragments.len(), 1);
        let LogicalPlan::PartialAggregate { aggr_exprs, .. } = &result.source_fragments[0].root
        else {
            panic!("expected PartialAggregate in child fragment");
        };
        assert_eq!(aggr_exprs.len(), 2, "AVG expands to SUM + COUNT");
        let names: Vec<String> = aggr_exprs
            .iter()
            .map(|e| match e {
                PlanExpr::Function { name, .. } => name.to_uppercase(),
                other => panic!("expected aggregate function, got {other:?}"),
            })
            .collect();
        assert_eq!(names, vec!["SUM".to_string(), "COUNT".to_string()]);
    }

    #[test]
    fn fragment_decomposable_aggregate_over_scan_fuses_partial_into_source() {
        // A1 map-side (2026-06-10): when a decomposable aggregate sits
        // directly over a scan SOURCE fragment, the PartialAggregate is
        // FUSED INTO that source fragment (aggregates the scan in-process,
        // emits partials) instead of a separate single-task `output=Single`
        // fragment that gathers every scan row through a Flight exchange.
        // Removes the O(input)-row scan→partial-agg gather (q01's 177M-row,
        // 57 MB/s bottleneck at SF30).
        let schema = vec![
            ColumnInfo {
                name: "key".into(),
                data_type: DataType::Int32,
                nullable: false,
            },
            ColumnInfo {
                name: "s".into(),
                data_type: DataType::Float64,
                nullable: true,
            },
        ];
        let plan = LogicalPlan::Aggregate {
            input: Box::new(scan_key_value("t")),
            group_by: vec![PlanExpr::Column {
                index: 0,
                name: "key".into(),
                span: None,
            }],
            aggr_exprs: vec![PlanExpr::Function {
                name: "SUM".into(),
                args: vec![PlanExpr::Column {
                    index: 1,
                    name: "v".into(),
                    span: None,
                }],
                distinct: false,
                span: None,
            }],
            schema,
        };
        let mut frag = PlanFragmenter::new();
        let result = frag.fragment(plan);

        // Root = FinalAggregate over the fused source's exchange.
        assert!(matches!(result.root, LogicalPlan::FinalAggregate { .. }));
        assert_eq!(result.source_fragments.len(), 1);
        let src = &result.source_fragments[0];
        assert_eq!(
            src.fragment_type,
            FragmentType::Source,
            "partial-agg fused into the scan SOURCE fragment"
        );
        // FUSION: no sub-fragment — the scan is consumed in-process, not
        // gathered from a separate fragment through an exchange.
        assert!(
            src.source_fragments.is_empty(),
            "scan fused in-process, no sub-fragment to gather; got {} children",
            src.source_fragments.len()
        );
        // Its root is a PartialAggregate reading the TableScan directly (no
        // ExchangeNode between the partial-agg and the scan).
        let LogicalPlan::PartialAggregate { input, .. } = &src.root else {
            panic!("source root should be PartialAggregate, got {:?}", src.root);
        };
        assert!(
            matches!(**input, LogicalPlan::TableScan { .. }),
            "PartialAggregate input should be the TableScan (fused), got {:?}",
            input
        );
    }

    #[test]
    fn fragment_avg_of_integer_stays_single_phase() {
        // AVG over an integer column is typed Int64 by the planner (a
        // quirk vs the Float64-producing accumulator); the rewrite gates
        // on a Float64 AVG output type and leaves this case single-phase
        // — no behavior change for the already-fragile int path.
        let schema = vec![ColumnInfo {
            name: "avg_id".into(),
            data_type: DataType::Int64,
            nullable: true,
        }];
        let plan = LogicalPlan::Aggregate {
            input: Box::new(scan("t")),
            group_by: vec![],
            aggr_exprs: vec![PlanExpr::Function {
                name: "AVG".into(),
                args: vec![PlanExpr::Column {
                    index: 0,
                    name: "id".into(),
                    span: None,
                }],
                distinct: false,
                span: None,
            }],
            schema,
        };
        let mut frag = PlanFragmenter::new();
        let result = frag.fragment(plan);
        assert!(matches!(result.root, LogicalPlan::Aggregate { .. }));
    }

    #[test]
    fn partitioning_scheme_display() {
        assert_eq!(PartitioningScheme::Single.to_string(), "SINGLE");
        assert_eq!(
            PartitioningScheme::Hash {
                columns: vec![0, 1],
                partition_count: 4,
            }
            .to_string(),
            "HASH([0, 1], n=4)"
        );
        assert_eq!(PartitioningScheme::RoundRobin.to_string(), "ROUND_ROBIN");
        assert_eq!(PartitioningScheme::Broadcast.to_string(), "BROADCAST");
    }

    #[test]
    fn distribution_display() {
        assert_eq!(Distribution::Unspecified.to_string(), "UNSPECIFIED");
        assert_eq!(Distribution::SinglePartition.to_string(), "SINGLE");
        assert_eq!(
            Distribution::HashPartitioned(vec![0, 1]).to_string(),
            "HASH([0, 1])"
        );
    }

    #[test]
    fn partitioning_is_partitioned_on_subset() {
        let p2 = PartitioningScheme::Hash {
            columns: vec![0, 1],
            partition_count: 4,
        };
        assert!(p2.is_partitioned_on(&[0, 1]), "exact match");
        assert!(p2.is_partitioned_on(&[1, 0]), "set semantics");
        assert!(
            !p2.is_partitioned_on(&[0]),
            "self {{0,1}} is not subset of {{0}}"
        );

        let p1 = PartitioningScheme::Hash {
            columns: vec![0],
            partition_count: 4,
        };
        assert!(
            p1.is_partitioned_on(&[0, 1]),
            "self {{0}} IS subset of {{0,1}}; Hash([0]) colocates rows that match on (0,1) too"
        );
    }

    #[test]
    fn partitioning_is_partitioned_on_non_hash() {
        assert!(!PartitioningScheme::Single.is_partitioned_on(&[0]));
        assert!(!PartitioningScheme::RoundRobin.is_partitioned_on(&[0]));
        assert!(!PartitioningScheme::Broadcast.is_partitioned_on(&[0]));
    }

    #[test]
    fn partitioning_satisfies_unspecified() {
        for p in [
            PartitioningScheme::Single,
            PartitioningScheme::Hash {
                columns: vec![0],
                partition_count: 4,
            },
            PartitioningScheme::RoundRobin,
            PartitioningScheme::Broadcast,
        ] {
            assert!(
                p.satisfy(&Distribution::Unspecified),
                "{p} should satisfy UNSPECIFIED"
            );
        }
    }

    #[test]
    fn partitioning_satisfies_single() {
        assert!(PartitioningScheme::Single.satisfy(&Distribution::SinglePartition));
        assert!(
            !PartitioningScheme::Hash {
                columns: vec![0],
                partition_count: 4,
            }
            .satisfy(&Distribution::SinglePartition),
            "Hash with N>1 doesn't gather"
        );
        assert!(
            PartitioningScheme::Hash {
                columns: vec![0],
                partition_count: 1,
            }
            .satisfy(&Distribution::SinglePartition),
            "Hash with N=1 is effectively single"
        );
        assert!(
            !PartitioningScheme::RoundRobin.satisfy(&Distribution::SinglePartition),
            "RoundRobin distributes, doesn't gather"
        );
        assert!(
            !PartitioningScheme::Broadcast.satisfy(&Distribution::SinglePartition),
            "Broadcast has N replicas, not a single partition"
        );
    }

    #[test]
    fn partitioning_satisfies_hash() {
        let p2 = PartitioningScheme::Hash {
            columns: vec![0, 1],
            partition_count: 4,
        };
        let p1 = PartitioningScheme::Hash {
            columns: vec![0],
            partition_count: 4,
        };

        assert!(p2.satisfy(&Distribution::HashPartitioned(vec![0, 1])));
        assert!(
            p1.satisfy(&Distribution::HashPartitioned(vec![0, 1])),
            "subset rule: self {{0}} ⊆ required {{0,1}}"
        );
        assert!(!p2.satisfy(&Distribution::HashPartitioned(vec![0])));

        assert!(
            !PartitioningScheme::Single.satisfy(&Distribution::HashPartitioned(vec![0])),
            "single partition can't fan out to N hash-partitioned consumers"
        );
        assert!(
            !PartitioningScheme::Hash {
                columns: vec![0],
                partition_count: 1,
            }
            .satisfy(&Distribution::HashPartitioned(vec![0])),
            "Hash with n=1 is effectively single"
        );
        assert!(!PartitioningScheme::RoundRobin.satisfy(&Distribution::HashPartitioned(vec![0])));
        assert!(
            !PartitioningScheme::Broadcast.satisfy(&Distribution::HashPartitioned(vec![0])),
            "broadcast has replicas everywhere; downstream would do duplicate work"
        );
    }

    #[test]
    fn fragment_type_display() {
        assert_eq!(FragmentType::Source.to_string(), "SOURCE");
        assert_eq!(FragmentType::Fixed.to_string(), "FIXED");
    }

    #[test]
    fn plan_fragment_display() {
        let frag = PlanFragment {
            id: StageId(0),
            fragment_type: FragmentType::Source,
            root: scan("t"),
            output_partitioning: PartitioningScheme::RoundRobin,
            source_fragments: vec![],
        };
        assert!(frag.to_string().contains("Fragment[id=0"));
    }

    // ------------------------------------------------------------------
    // A2.2 (2026-05-28): broadcast eligibility tests
    // ------------------------------------------------------------------

    fn join_with_equi_keys(left_name: &str, right_name: &str) -> LogicalPlan {
        // Equi-join `left.id = right.id`. Two single-column Int32 schemas
        // → left_col_count == 1, right column referenced as index 1.
        LogicalPlan::Join {
            left: Box::new(scan(left_name)),
            right: Box::new(scan(right_name)),
            join_type: ast::JoinType::Inner,
            condition: crate::plan::JoinCondition::On(PlanExpr::BinaryOp {
                left: Box::new(PlanExpr::Column {
                    index: 0,
                    name: "id".into(),
                    span: None,
                }),
                op: ast::BinaryOp::Eq,
                right: Box::new(PlanExpr::Column {
                    index: 1,
                    name: "id".into(),
                    span: None,
                }),
                span: None,
            }),
            dynamic_filter_ids: Vec::new(),
        }
    }

    fn stats_with_small_right() -> crate::cost::CatalogStats {
        let mut stats = crate::cost::CatalogStats::new();
        // Right table 25 rows (TPC-H nation cardinality). At 4 bytes/row
        // (Int32) → estimated_bytes = 100 bytes, well under any reasonable
        // threshold.
        stats.insert(
            TableReference::table("right_t"),
            arneb_catalog::TableStatistics {
                row_count: Some(25),
                size_bytes: Some(100),
                columns: std::collections::HashMap::new(),
            },
        );
        stats
    }

    fn stats_for_build_side_swap() -> crate::cost::CatalogStats {
        let mut stats = crate::cost::CatalogStats::new();
        stats.insert(
            TableReference::table("small_left"),
            arneb_catalog::TableStatistics {
                row_count: Some(25),
                size_bytes: Some(200),
                columns: std::collections::HashMap::new(),
            },
        );
        stats.insert(
            TableReference::table("big_right"),
            arneb_catalog::TableStatistics {
                row_count: Some(1_000_000),
                size_bytes: Some(16_000_000),
                columns: std::collections::HashMap::new(),
            },
        );
        stats
    }

    fn stats_for_nested_build_side_swap() -> crate::cost::CatalogStats {
        let mut stats = stats_for_build_side_swap();
        stats.insert(
            TableReference::table("fact_probe"),
            arneb_catalog::TableStatistics {
                row_count: Some(1_000_000),
                size_bytes: Some(16_000_000),
                columns: std::collections::HashMap::new(),
            },
        );
        stats.insert(
            TableReference::table("fact_side"),
            arneb_catalog::TableStatistics {
                row_count: Some(1_000_000),
                size_bytes: Some(16_000_000),
                columns: std::collections::HashMap::new(),
            },
        );
        stats
    }

    fn stats_for_inline_broadcast_df_regen() -> crate::cost::CatalogStats {
        let mut stats = crate::cost::CatalogStats::new();
        stats.insert(
            TableReference::table("dim_t"),
            arneb_catalog::TableStatistics {
                row_count: Some(25),
                size_bytes: Some(100),
                columns: std::collections::HashMap::new(),
            },
        );
        stats
    }

    fn nested_fact_probe_for_swap() -> LogicalPlan {
        LogicalPlan::Join {
            left: Box::new(scan_with_columns("fact_probe", &["f_id"])),
            right: Box::new(scan_with_columns("fact_side", &["s_id"])),
            join_type: ast::JoinType::Inner,
            condition: crate::plan::JoinCondition::None,
            dynamic_filter_ids: Vec::new(),
        }
    }

    fn join_that_swaps_small_left_to_build() -> LogicalPlan {
        let mut fact_probe = nested_fact_probe_for_swap();
        fact_probe = add_dynamic_filter_consumer_to_table(
            fact_probe,
            "fact_probe",
            arneb_common::DynamicFilterId(7),
        );
        LogicalPlan::Join {
            left: Box::new(scan_with_columns("small_left", &["d_id"])),
            right: Box::new(fact_probe),
            join_type: ast::JoinType::Inner,
            condition: crate::plan::JoinCondition::On(PlanExpr::BinaryOp {
                left: Box::new(PlanExpr::Column {
                    index: 0,
                    name: "d_id".into(),
                    span: None,
                }),
                op: ast::BinaryOp::Eq,
                right: Box::new(PlanExpr::Column {
                    index: 1,
                    name: "f_id".into(),
                    span: None,
                }),
                span: None,
            }),
            dynamic_filter_ids: vec![crate::plan::DynamicFilterProducer {
                id: arneb_common::DynamicFilterId(7),
                build_index: 0,
                probe_index: 0,
                column_name: "d_id".into(),
            }],
        }
    }

    fn inline_broadcast_join_with_mis_sided_df() -> LogicalPlan {
        LogicalPlan::Join {
            left: Box::new(scan_with_columns("fact_t", &["f_id"])),
            right: Box::new(add_dynamic_filter_consumer_to_table(
                scan_with_columns("dim_t", &["d_id"]),
                "dim_t",
                arneb_common::DynamicFilterId(7),
            )),
            join_type: ast::JoinType::Inner,
            condition: crate::plan::JoinCondition::On(PlanExpr::BinaryOp {
                left: Box::new(PlanExpr::Column {
                    index: 0,
                    name: "f_id".into(),
                    span: None,
                }),
                op: ast::BinaryOp::Eq,
                right: Box::new(PlanExpr::Column {
                    index: 1,
                    name: "d_id".into(),
                    span: None,
                }),
                span: None,
            }),
            dynamic_filter_ids: vec![crate::plan::DynamicFilterProducer {
                id: arneb_common::DynamicFilterId(7),
                build_index: 0,
                probe_index: 0,
                column_name: "d_id".into(),
            }],
        }
    }

    fn add_dynamic_filter_consumer_to_table(
        plan: LogicalPlan,
        table_name: &str,
        id: arneb_common::DynamicFilterId,
    ) -> LogicalPlan {
        match plan {
            LogicalPlan::TableScan {
                table,
                schema,
                alias,
                properties,
                mut dynamic_filters_consumed,
            } => {
                if table.table == table_name {
                    dynamic_filters_consumed.push(crate::plan::DynamicFilterConsumer {
                        id,
                        column_index: 0,
                        column_name: "old_mis_sided".into(),
                    });
                }
                LogicalPlan::TableScan {
                    table,
                    schema,
                    alias,
                    properties,
                    dynamic_filters_consumed,
                }
            }
            LogicalPlan::Join {
                left,
                right,
                join_type,
                condition,
                dynamic_filter_ids,
            } => LogicalPlan::Join {
                left: Box::new(add_dynamic_filter_consumer_to_table(*left, table_name, id)),
                right: Box::new(add_dynamic_filter_consumer_to_table(*right, table_name, id)),
                join_type,
                condition,
                dynamic_filter_ids,
            },
            other => other,
        }
    }

    fn collect_dynamic_filter_producers(
        plan: &LogicalPlan,
        out: &mut Vec<crate::plan::DynamicFilterProducer>,
    ) {
        match plan {
            LogicalPlan::Join {
                left,
                right,
                dynamic_filter_ids,
                ..
            } => {
                out.extend(dynamic_filter_ids.iter().cloned());
                collect_dynamic_filter_producers(left, out);
                collect_dynamic_filter_producers(right, out);
            }
            LogicalPlan::Projection { input, .. }
            | LogicalPlan::Filter { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => collect_dynamic_filter_producers(input, out),
            _ => {}
        }
    }

    fn collect_table_consumers(
        plan: &LogicalPlan,
        table_name: &str,
        out: &mut Vec<crate::plan::DynamicFilterConsumer>,
    ) {
        match plan {
            LogicalPlan::TableScan {
                table,
                dynamic_filters_consumed,
                ..
            } if table.table == table_name => {
                out.extend(dynamic_filters_consumed.iter().cloned());
            }
            LogicalPlan::Join { left, right, .. } => {
                collect_table_consumers(left, table_name, out);
                collect_table_consumers(right, table_name, out);
            }
            LogicalPlan::Projection { input, .. }
            | LogicalPlan::Filter { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => collect_table_consumers(input, table_name, out),
            _ => {}
        }
    }

    fn collect_fragment_dynamic_filter_producers(
        fragment: &PlanFragment,
    ) -> Vec<crate::plan::DynamicFilterProducer> {
        let mut out = Vec::new();
        collect_dynamic_filter_producers(&fragment.root, &mut out);
        for source in &fragment.source_fragments {
            out.extend(collect_fragment_dynamic_filter_producers(source));
        }
        out
    }

    fn collect_fragment_table_consumers(
        fragment: &PlanFragment,
        table_name: &str,
    ) -> Vec<crate::plan::DynamicFilterConsumer> {
        let mut out = Vec::new();
        collect_table_consumers(&fragment.root, table_name, &mut out);
        for source in &fragment.source_fragments {
            out.extend(collect_fragment_table_consumers(source, table_name));
        }
        out
    }

    #[test]
    fn inline_broadcast_join_regenerates_dynamic_filter_on_fact_probe_scan() {
        let _swap_df_regen = set_swap_df_regen_for_test(true);
        let _df_through_joins =
            crate::analyzer::assign_dynamic_filter_ids::set_df_through_joins_for_test(true);
        let plan = inline_broadcast_join_with_mis_sided_df();

        let mut frag = PlanFragmenter::new()
            .with_broadcast_threshold(Some(1024))
            .with_stats(Some(std::sync::Arc::new(
                stats_for_inline_broadcast_df_regen(),
            )));
        let result = frag.fragment(plan);

        assert_eq!(result.source_fragments.len(), 1);
        let probe_frag = &result.source_fragments[0];
        assert!(matches!(probe_frag.root, LogicalPlan::Join { .. }));
        assert_eq!(probe_frag.source_fragments.len(), 1);
        assert_eq!(
            probe_frag.source_fragments[0].output_partitioning,
            PartitioningScheme::Broadcast
        );

        let producers = collect_fragment_dynamic_filter_producers(&result);
        assert_eq!(producers.len(), 1, "expected regenerated producer only");
        assert!(producers[0].id.0 >= 1_000_000);
        assert_eq!(producers[0].build_index, 0);
        assert_eq!(producers[0].probe_index, 0);
        assert_eq!(producers[0].column_name, "d_id");

        let fact_consumers = collect_fragment_table_consumers(&result, "fact_t");
        assert_eq!(fact_consumers.len(), 1);
        assert_eq!(fact_consumers[0].id, producers[0].id);
        assert_eq!(fact_consumers[0].column_index, 0);

        let dim_consumers = collect_fragment_table_consumers(&result, "dim_t");
        assert!(
            dim_consumers.is_empty(),
            "old dim-side consumer should be stripped before regeneration"
        );
    }

    #[test]
    fn inline_broadcast_join_preserves_original_dynamic_filter_when_regen_disabled() {
        let _swap_df_regen = set_swap_df_regen_for_test(false);
        let _df_through_joins =
            crate::analyzer::assign_dynamic_filter_ids::set_df_through_joins_for_test(true);
        let plan = inline_broadcast_join_with_mis_sided_df();

        let mut frag = PlanFragmenter::new()
            .with_broadcast_threshold(Some(1024))
            .with_stats(Some(std::sync::Arc::new(
                stats_for_inline_broadcast_df_regen(),
            )));
        let result = frag.fragment(plan);

        let producers = collect_fragment_dynamic_filter_producers(&result);
        assert_eq!(producers.len(), 1);
        assert_eq!(producers[0].id, arneb_common::DynamicFilterId(7));
        assert_eq!(producers[0].build_index, 0);
        assert_eq!(producers[0].probe_index, 0);
        assert_eq!(producers[0].column_name, "d_id");

        assert!(
            collect_fragment_table_consumers(&result, "fact_t").is_empty(),
            "gate off should not move the consumer onto the fact probe"
        );

        let dim_consumers = collect_fragment_table_consumers(&result, "dim_t");
        assert_eq!(dim_consumers.len(), 1);
        assert_eq!(dim_consumers[0].id, arneb_common::DynamicFilterId(7));
        assert_eq!(dim_consumers[0].column_index, 0);
    }

    #[test]
    fn broadcast_when_build_under_threshold() {
        let left_probe = join_with_equi_keys("left_t", "big_t");
        let plan = LogicalPlan::Join {
            left: Box::new(left_probe),
            right: Box::new(scan("right_t")),
            join_type: ast::JoinType::Inner,
            condition: crate::plan::JoinCondition::On(PlanExpr::BinaryOp {
                left: Box::new(PlanExpr::Column {
                    index: 0,
                    name: "id".into(),
                    span: None,
                }),
                op: ast::BinaryOp::Eq,
                right: Box::new(PlanExpr::Column {
                    index: 2,
                    name: "id".into(),
                    span: None,
                }),
                span: None,
            }),
            dynamic_filter_ids: Vec::new(),
        };
        let mut stats = stats_with_small_right();
        stats.insert(
            TableReference::table("big_t"),
            arneb_catalog::TableStatistics {
                row_count: Some(1_000_000),
                size_bytes: Some(4_000_000),
                columns: std::collections::HashMap::new(),
            },
        );
        let mut frag = PlanFragmenter::new()
            .with_broadcast_threshold(Some(1024))
            .with_stats(Some(std::sync::Arc::new(stats)));
        let result = frag.fragment(plan);
        // Root → ExchangeNode → outer join fragment; the probe side is an
        // already HashPartitioned inner join, and the outer build is appended
        // as a Broadcast source.
        let join_frag = &result.source_fragments[0];
        assert_eq!(join_frag.source_fragments.len(), 3);
        let right_child = &join_frag.source_fragments[2];
        // A2.3: right is Broadcast, left collapses to Single (skips the
        // hash redistribution), join fragment becomes Fixed (1 task).
        assert!(
            matches!(
                right_child.output_partitioning,
                PartitioningScheme::Broadcast
            ),
            "right (build) child should be Broadcast, got {:?}",
            right_child.output_partitioning
        );
        // v2 (2026-06-03): the probe (left) child stays Hash-partitioned
        // N-way and the join fragment stays HashPartitioned (N tasks).
        // v1 collapsed left→Single + join→Fixed, which serialized the
        // probe AND silently dropped half the rows when the left child
        // was itself a multi-partition HASH fragment (SF30 q09 = 0.496×).
        // Only the BUILD (right) side is broadcast.
        assert_eq!(
            join_frag.fragment_type,
            FragmentType::HashPartitioned,
            "v2: broadcast join stays HashPartitioned (N parallel probe tasks)"
        );
        assert!(
            matches!(
                join_frag.output_partitioning,
                PartitioningScheme::Hash { .. }
            ),
            "v2: join fragment outputs Hash (N tasks), got {:?}",
            join_frag.output_partitioning
        );
    }

    #[test]
    fn broadcast_inlines_source_probe_without_hash_repartition() {
        let plan = join_with_equi_keys("left_t", "right_t");
        let stats = stats_with_small_right();
        let mut frag = PlanFragmenter::new()
            .with_broadcast_threshold(Some(100 * 1024 * 1024))
            .with_stats(Some(std::sync::Arc::new(stats)));
        let result = frag.fragment(plan);

        assert_eq!(result.source_fragments.len(), 1);
        let probe_frag = &result.source_fragments[0];
        assert_eq!(probe_frag.fragment_type, FragmentType::Source);
        assert!(
            matches!(probe_frag.root, LogicalPlan::Join { .. }),
            "Source probe fragment root should be the Join"
        );
        assert_eq!(probe_frag.source_fragments.len(), 1);
        assert!(
            matches!(
                probe_frag.source_fragments[0].output_partitioning,
                PartitioningScheme::Broadcast
            ),
            "build side should be Broadcast, got {:?}",
            probe_frag.source_fragments[0].output_partitioning
        );
        assert!(
            !matches!(
                probe_frag.output_partitioning,
                PartitioningScheme::Hash { .. }
            ),
            "Source probe should not be hash-repartitioned, got {:?}",
            probe_frag.output_partitioning
        );
        assert!(
            !result
                .source_fragments
                .iter()
                .any(|f| f.fragment_type == FragmentType::HashPartitioned),
            "Source probe should not become a separate HashPartitioned fragment"
        );
    }

    #[test]
    fn swapped_join_column_index_remap_moves_old_left_after_old_right() {
        assert_eq!(remap_swapped_join_column_index(2, 3, 0), 3);
        assert_eq!(remap_swapped_join_column_index(2, 3, 1), 4);
        assert_eq!(remap_swapped_join_column_index(2, 3, 2), 0);
        assert_eq!(remap_swapped_join_column_index(2, 3, 4), 2);
    }

    #[test]
    fn inner_join_swaps_left_broadcast_candidate_to_build_and_restores_output_order() {
        let _swap_df_regen = set_swap_df_regen_for_test(false);
        let left = scan_with_columns("small_left", &["l_id", "l_payload"]);
        let right = scan_with_columns("big_right", &["r_id"]);
        let original_schema = {
            let mut schema = left.schema();
            schema.extend(right.schema());
            schema
        };
        let plan = LogicalPlan::Join {
            left: Box::new(left),
            right: Box::new(right),
            join_type: ast::JoinType::Inner,
            condition: crate::plan::JoinCondition::On(PlanExpr::BinaryOp {
                left: Box::new(PlanExpr::Column {
                    index: 0,
                    name: "l_id".into(),
                    span: None,
                }),
                op: ast::BinaryOp::Eq,
                right: Box::new(PlanExpr::Column {
                    index: 2,
                    name: "r_id".into(),
                    span: None,
                }),
                span: None,
            }),
            dynamic_filter_ids: vec![crate::plan::DynamicFilterProducer {
                id: arneb_common::DynamicFilterId(7),
                build_index: 0,
                probe_index: 0,
                column_name: "l_id".into(),
            }],
        };

        let mut frag = PlanFragmenter::new()
            .with_broadcast_threshold(Some(1024))
            .with_stats(Some(std::sync::Arc::new(stats_for_build_side_swap())));
        let result = frag.fragment(plan);

        let probe_frag = &result.source_fragments[0];
        assert_eq!(probe_frag.fragment_type, FragmentType::Source);
        assert_eq!(probe_frag.root.schema(), original_schema);
        assert_eq!(
            probe_frag.source_fragments[0].output_partitioning,
            PartitioningScheme::Broadcast
        );
        assert!(matches!(
            probe_frag.source_fragments[0].root,
            LogicalPlan::TableScan { .. }
        ));

        let LogicalPlan::Projection {
            input,
            exprs,
            schema,
        } = &probe_frag.root
        else {
            panic!("swapped join root should be Projection restoring original output order");
        };
        assert_eq!(schema, &original_schema);
        let projected_indices: Vec<_> = exprs
            .iter()
            .map(|expr| match expr {
                PlanExpr::Column { index, .. } => *index,
                other => panic!("restore projection should contain only columns, got {other:?}"),
            })
            .collect();
        assert_eq!(projected_indices, vec![1, 2, 0]);

        let LogicalPlan::Join {
            left,
            right,
            condition,
            dynamic_filter_ids,
            ..
        } = input.as_ref()
        else {
            panic!("restore projection should wrap the swapped Join");
        };
        assert!(dynamic_filter_ids.is_empty());
        assert!(matches!(left.as_ref(), LogicalPlan::TableScan { .. }));
        assert!(matches!(right.as_ref(), LogicalPlan::ExchangeNode { .. }));
        let crate::plan::JoinCondition::On(PlanExpr::BinaryOp { left, right, .. }) = condition
        else {
            panic!("expected remapped equi-join condition");
        };
        assert!(matches!(left.as_ref(), PlanExpr::Column { index: 1, .. }));
        assert!(matches!(right.as_ref(), PlanExpr::Column { index: 0, .. }));
    }

    #[test]
    fn swapped_inner_join_regenerates_dynamic_filter_on_new_probe_fact_scan() {
        let _swap_df_regen = set_swap_df_regen_for_test(true);
        let _df_through_joins =
            crate::analyzer::assign_dynamic_filter_ids::set_df_through_joins_for_test(true);
        let plan = join_that_swaps_small_left_to_build();

        let mut frag = PlanFragmenter::new()
            .with_broadcast_threshold(Some(1024))
            .with_stats(Some(
                std::sync::Arc::new(stats_for_nested_build_side_swap()),
            ));
        let result = frag.fragment(plan);

        let producers = collect_fragment_dynamic_filter_producers(&result);
        assert_eq!(producers.len(), 1, "expected regenerated producer only");
        assert!(producers[0].id.0 >= 1_000_000);
        assert_eq!(producers[0].build_index, 0);
        assert_eq!(producers[0].probe_index, 0);
        assert_eq!(producers[0].column_name, "d_id");

        let fact_consumers = collect_fragment_table_consumers(&result, "fact_probe");
        assert_eq!(fact_consumers.len(), 1);
        assert_eq!(fact_consumers[0].id, producers[0].id);
        assert_eq!(fact_consumers[0].column_index, 0);

        let dim_consumers = collect_fragment_table_consumers(&result, "small_left");
        assert!(
            dim_consumers.is_empty(),
            "new build side should not keep the old probe-side consumer"
        );
    }

    #[test]
    fn swapped_inner_join_clears_dynamic_filter_when_regeneration_is_disabled() {
        let _swap_df_regen = set_swap_df_regen_for_test(false);
        let _df_through_joins =
            crate::analyzer::assign_dynamic_filter_ids::set_df_through_joins_for_test(true);
        let plan = join_that_swaps_small_left_to_build();

        let mut frag = PlanFragmenter::new()
            .with_broadcast_threshold(Some(1024))
            .with_stats(Some(
                std::sync::Arc::new(stats_for_nested_build_side_swap()),
            ));
        let result = frag.fragment(plan);

        assert!(
            collect_fragment_dynamic_filter_producers(&result).is_empty(),
            "swap should preserve current behavior when regeneration is disabled"
        );
        assert!(
            collect_fragment_table_consumers(&result, "fact_probe").is_empty(),
            "old mis-sided consumer should be stripped and not regenerated"
        );
    }

    #[test]
    fn non_inner_join_does_not_swap_left_broadcast_candidate() {
        let plan = LogicalPlan::Join {
            left: Box::new(scan_with_columns("small_left", &["l_id", "l_payload"])),
            right: Box::new(scan_with_columns("big_right", &["r_id"])),
            join_type: ast::JoinType::Left,
            condition: crate::plan::JoinCondition::On(PlanExpr::BinaryOp {
                left: Box::new(PlanExpr::Column {
                    index: 0,
                    name: "l_id".into(),
                    span: None,
                }),
                op: ast::BinaryOp::Eq,
                right: Box::new(PlanExpr::Column {
                    index: 2,
                    name: "r_id".into(),
                    span: None,
                }),
                span: None,
            }),
            dynamic_filter_ids: Vec::new(),
        };

        let mut frag = PlanFragmenter::new()
            .with_broadcast_threshold(Some(1024))
            .with_stats(Some(std::sync::Arc::new(stats_for_build_side_swap())));
        let result = frag.fragment(plan);
        let join_frag = &result.source_fragments[0];

        assert!(matches!(join_frag.root, LogicalPlan::Join { .. }));
        assert!(
            !join_frag
                .source_fragments
                .iter()
                .any(|fragment| fragment.output_partitioning == PartitioningScheme::Broadcast),
            "left/right eligibility shape should not broadcast after a non-inner swap"
        );
    }

    #[test]
    fn partitioned_build_side_swap_default_off_keeps_original_build() {
        let _swap = set_partitioned_build_side_swap_for_test(false, 100.0);
        let plan = LogicalPlan::Join {
            left: Box::new(scan_with_columns("small_left", &["l_id", "l_payload"])),
            right: Box::new(scan_with_columns("big_right", &["r_id"])),
            join_type: ast::JoinType::Inner,
            condition: crate::plan::JoinCondition::On(PlanExpr::BinaryOp {
                left: Box::new(PlanExpr::Column {
                    index: 0,
                    name: "l_id".into(),
                    span: None,
                }),
                op: ast::BinaryOp::Eq,
                right: Box::new(PlanExpr::Column {
                    index: 2,
                    name: "r_id".into(),
                    span: None,
                }),
                span: None,
            }),
            dynamic_filter_ids: Vec::new(),
        };

        let mut frag = PlanFragmenter::new()
            .with_stats(Some(std::sync::Arc::new(stats_for_build_side_swap())));
        let result = frag.fragment(plan);
        let join_frag = &result.source_fragments[0];

        let LogicalPlan::Join { left, right, .. } = &join_frag.root else {
            panic!("default-off partitioned swap should leave root as Join");
        };
        assert!(matches!(left.as_ref(), LogicalPlan::ExchangeNode { .. }));
        assert!(matches!(right.as_ref(), LogicalPlan::ExchangeNode { .. }));
        assert!(matches!(
            join_frag.source_fragments[0].root,
            LogicalPlan::TableScan { ref table, .. } if table.table == "small_left"
        ));
        assert!(matches!(
            join_frag.source_fragments[1].root,
            LogicalPlan::TableScan { ref table, .. } if table.table == "big_right"
        ));
    }

    #[test]
    fn partitioned_build_side_swap_fires_and_restores_output_order() {
        let _swap = set_partitioned_build_side_swap_for_test(true, 100.0);
        let left = scan_with_columns("small_left", &["l_id", "l_payload"]);
        let right = scan_with_columns("big_right", &["r_id"]);
        let original_schema = {
            let mut schema = left.schema();
            schema.extend(right.schema());
            schema
        };
        let plan = LogicalPlan::Join {
            left: Box::new(left),
            right: Box::new(right),
            join_type: ast::JoinType::Inner,
            condition: crate::plan::JoinCondition::On(PlanExpr::BinaryOp {
                left: Box::new(PlanExpr::Column {
                    index: 0,
                    name: "l_id".into(),
                    span: None,
                }),
                op: ast::BinaryOp::Eq,
                right: Box::new(PlanExpr::Column {
                    index: 2,
                    name: "r_id".into(),
                    span: None,
                }),
                span: None,
            }),
            dynamic_filter_ids: vec![crate::plan::DynamicFilterProducer {
                id: arneb_common::DynamicFilterId(7),
                build_index: 0,
                probe_index: 0,
                column_name: "l_id".into(),
            }],
        };

        let mut frag = PlanFragmenter::new()
            .with_stats(Some(std::sync::Arc::new(stats_for_build_side_swap())));
        let result = frag.fragment(plan);
        let join_frag = &result.source_fragments[0];

        assert_eq!(join_frag.root.schema(), original_schema);
        let LogicalPlan::Projection {
            input,
            exprs,
            schema,
        } = &join_frag.root
        else {
            panic!("partitioned swap should wrap Join in output restore Projection");
        };
        assert_eq!(schema, &original_schema);
        let projected_indices: Vec<_> = exprs
            .iter()
            .map(|expr| match expr {
                PlanExpr::Column { index, .. } => *index,
                other => panic!("restore projection should contain only columns, got {other:?}"),
            })
            .collect();
        assert_eq!(projected_indices, vec![1, 2, 0]);

        let LogicalPlan::Join {
            left,
            right,
            condition,
            dynamic_filter_ids,
            ..
        } = input.as_ref()
        else {
            panic!("restore projection should wrap the swapped Join");
        };
        assert!(dynamic_filter_ids.is_empty());
        assert!(matches!(left.as_ref(), LogicalPlan::ExchangeNode { .. }));
        assert!(matches!(right.as_ref(), LogicalPlan::ExchangeNode { .. }));
        assert!(matches!(
            join_frag.source_fragments[0].root,
            LogicalPlan::TableScan { ref table, .. } if table.table == "big_right"
        ));
        assert!(matches!(
            join_frag.source_fragments[1].root,
            LogicalPlan::TableScan { ref table, .. } if table.table == "small_left"
        ));
        let crate::plan::JoinCondition::On(PlanExpr::BinaryOp { left, right, .. }) = condition
        else {
            panic!("expected remapped equi-join condition");
        };
        assert!(matches!(left.as_ref(), PlanExpr::Column { index: 1, .. }));
        assert!(matches!(right.as_ref(), PlanExpr::Column { index: 0, .. }));
    }

    #[test]
    fn adaptive_partition_count_scales_with_cardinality() {
        // dist-adaptive-partition: a large child cardinality must fan the hash
        // exchange out well beyond the worker-count floor. The estimate MUST
        // come from the un-split child — if it read the post-split
        // `ExchangeNode` placeholder it would see `DEFAULT_TABLE_SIZE` (10000)
        // and pin the count at 2. This test guards exactly that regression.
        let plan = join_with_equi_keys("left_t", "right_t");
        let mut stats = crate::cost::CatalogStats::new();
        stats.insert(
            TableReference::table("left_t"),
            arneb_catalog::TableStatistics {
                row_count: Some(6_000_000),
                size_bytes: Some(24_000_000),
                columns: std::collections::HashMap::new(),
            },
        );
        let mut frag = PlanFragmenter::new()
            .with_stats(Some(std::sync::Arc::new(stats)))
            .with_worker_count(2)
            .with_partition_policy(500_000, 64);
        let result = frag.fragment(plan);
        let join_frag = &result.source_fragments[0];
        let n = join_frag.output_partitioning.partition_count();
        assert_eq!(
            n, 12,
            "6M rows / 500k target = 12 partitions (estimate from un-split child); got {n}"
        );
        // The repartitioned children carry the same N.
        for child in &join_frag.source_fragments {
            if let PartitioningScheme::Hash {
                partition_count, ..
            } = child.output_partitioning
            {
                assert_eq!(partition_count, 12, "child fan-out matches the join");
            }
        }
    }

    #[test]
    fn adaptive_partition_count_floors_at_two_without_stats() {
        // No stats → estimate is None → worker-count-only. With the default
        // single-worker fragmenter the count is the floor of 2 (the previous
        // fixed behaviour), so existing distributed plans are unaffected.
        let plan = join_with_equi_keys("left_t", "right_t");
        let mut frag = PlanFragmenter::new();
        let result = frag.fragment(plan);
        let join_frag = &result.source_fragments[0];
        assert_eq!(
            join_frag.output_partitioning.partition_count(),
            2,
            "no stats + 1 worker → floor of 2 (unchanged behaviour)"
        );
    }

    #[test]
    fn no_broadcast_when_threshold_none() {
        // Default fragmenter — no threshold, no stats. Should never
        // produce Broadcast regardless of build size.
        let plan = join_with_equi_keys("left_t", "right_t");
        let mut frag = PlanFragmenter::new();
        let result = frag.fragment(plan);
        let join_frag = &result.source_fragments[0];
        let right_child = &join_frag.source_fragments[1];
        assert!(
            matches!(
                right_child.output_partitioning,
                PartitioningScheme::Hash { .. }
            ),
            "default fragmenter must not produce Broadcast"
        );
    }

    #[test]
    fn no_broadcast_when_stats_missing() {
        // Threshold set but stats=None. Falls back to Hash because the
        // broadcast eligibility check requires a stats snapshot.
        let plan = join_with_equi_keys("left_t", "right_t");
        let mut frag = PlanFragmenter::new()
            .with_broadcast_threshold(Some(100 * 1024 * 1024))
            .with_stats(None);
        let result = frag.fragment(plan);
        let join_frag = &result.source_fragments[0];
        let right_child = &join_frag.source_fragments[1];
        assert!(matches!(
            right_child.output_partitioning,
            PartitioningScheme::Hash { .. }
        ));
    }

    #[test]
    fn no_broadcast_when_build_too_big() {
        // Right has explicit stats showing 1 M rows × 4 bytes = 4 MB.
        // Threshold 1 KB → too small, falls back to Hash partitioning.
        let plan = join_with_equi_keys("left_t", "right_t");
        let mut stats = crate::cost::CatalogStats::new();
        stats.insert(
            TableReference::table("right_t"),
            arneb_catalog::TableStatistics {
                row_count: Some(1_000_000),
                size_bytes: Some(4_000_000),
                columns: std::collections::HashMap::new(),
            },
        );
        let mut frag = PlanFragmenter::new()
            .with_broadcast_threshold(Some(1024))
            .with_stats(Some(std::sync::Arc::new(stats)));
        let result = frag.fragment(plan);
        let join_frag = &result.source_fragments[0];
        let right_child = &join_frag.source_fragments[1];
        assert!(
            matches!(
                right_child.output_partitioning,
                PartitioningScheme::Hash { .. }
            ),
            "build too big for threshold should stay Hash"
        );
    }

    // ---- choose_partition_count (dist-adaptive-partition) ----

    #[test]
    fn partition_count_floor_is_two() {
        // Single worker, tiny estimate, generous max → still at least 2.
        assert_eq!(choose_partition_count(1, Some(10), 1_000_000, 64), 2);
        // Zero workers (degenerate) also floors at 2.
        assert_eq!(choose_partition_count(0, None, 1_000_000, 64), 2);
    }

    #[test]
    fn partition_count_capped_at_max() {
        // Huge estimate would want many partitions; capped at max.
        assert_eq!(choose_partition_count(2, Some(1_000_000_000), 1, 16), 16);
        // Worker count above max is also capped.
        assert_eq!(choose_partition_count(100, None, 1_000_000, 8), 8);
    }

    #[test]
    fn partition_count_unknown_estimate_uses_worker_count_only() {
        // No estimate → deterministic worker-count-only (here 6), clamped.
        assert_eq!(choose_partition_count(6, None, 1_000_000, 64), 6);
        // Zero rows is treated like unknown.
        assert_eq!(choose_partition_count(6, Some(0), 1_000_000, 64), 6);
    }

    #[test]
    fn partition_count_monotonic_in_workers() {
        let max = 256;
        let mut prev = 0;
        for w in [1usize, 2, 4, 8, 16, 32] {
            let n = choose_partition_count(w, None, 1_000_000, max);
            assert!(n >= prev, "non-decreasing in worker_count");
            prev = n;
        }
    }

    #[test]
    fn partition_count_monotonic_in_cardinality() {
        let max = 1_000_000; // effectively uncapped for this range
        let mut prev = 0;
        for rows in [1u64, 10_000, 1_000_000, 90_000_000, 180_000_000] {
            let n = choose_partition_count(2, Some(rows), 4_000_000, max);
            assert!(n >= prev, "non-decreasing in estimated_rows");
            prev = n;
        }
    }

    #[test]
    fn partition_count_cardinality_drives_above_workers() {
        // 90M rows / 4M target = 23 partitions, well above the 2-worker floor.
        // This is the q09/q18 SF30 case: wide intermediate, small cluster.
        assert_eq!(
            choose_partition_count(2, Some(90_000_000), 4_000_000, 256),
            23
        );
    }
}
