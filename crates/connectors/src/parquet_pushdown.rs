//! Row group pruning and predicate pushdown for Parquet files.
//!
//! Provides utilities to skip row groups based on column statistics
//! and to translate plan expressions into Arrow predicates for
//! within-row-group filtering.

use std::sync::Arc;

use arneb_common::types::ScalarValue;
use arneb_planner::PlanExpr;
use arneb_sql_parser::ast::BinaryOp;
use arrow::array::BooleanArray;
use arrow::compute::kernels;
use parquet::arrow::arrow_reader::{ArrowPredicateFn, RowFilter};
use parquet::arrow::ProjectionMask;
use parquet::file::metadata::RowGroupMetaData;
use parquet::file::statistics::Statistics;
use parquet::schema::types::SchemaDescriptor;
use tracing::debug;

/// Determines which row groups to read based on filter predicates.
///
/// Returns a list of row group indices that *may* contain matching rows.
/// Row groups whose statistics prove they cannot match are excluded.
pub fn prune_row_groups(
    row_groups: &[RowGroupMetaData],
    filters: &[PlanExpr],
    _column_names: &[String],
) -> Vec<usize> {
    if filters.is_empty() {
        return (0..row_groups.len()).collect();
    }

    let mut selected = Vec::new();
    for (rg_idx, rg) in row_groups.iter().enumerate() {
        if can_prune_row_group(rg, filters) {
            debug!("pruning row group {rg_idx} (statistics prove no match)");
        } else {
            selected.push(rg_idx);
        }
    }

    let pruned = row_groups.len() - selected.len();
    if pruned > 0 {
        debug!(
            "row group pruning: {pruned}/{} pruned, {} remaining",
            row_groups.len(),
            selected.len()
        );
    }

    selected
}

/// Returns `true` if statistics prove the row group cannot contain any matching rows.
fn can_prune_row_group(rg: &RowGroupMetaData, filters: &[PlanExpr]) -> bool {
    for filter in filters {
        if filter_prunes_row_group(rg, filter) {
            return true;
        }
    }
    false
}

/// Check if a single filter expression prunes the given row group.
fn filter_prunes_row_group(rg: &RowGroupMetaData, filter: &PlanExpr) -> bool {
    match filter {
        PlanExpr::BinaryOp {
            left, op, right, ..
        } => {
            // AND conjunction: prune if either side proves no match
            if *op == BinaryOp::And {
                return filter_prunes_row_group(rg, left) || filter_prunes_row_group(rg, right);
            }

            // Try Column op Literal
            if let Some((col_idx, scalar, comparison_op)) =
                extract_column_literal_comparison(left, op, right)
            {
                return column_stats_prune(rg, col_idx, &scalar, comparison_op);
            }

            false
        }
        _ => false,
    }
}

/// Extract (column_index, literal_value, op) from a comparison expression.
/// Handles both `Column op Literal` and `Literal op Column` (reversing the op).
fn extract_column_literal_comparison(
    left: &PlanExpr,
    op: &BinaryOp,
    right: &PlanExpr,
) -> Option<(usize, ScalarValue, BinaryOp)> {
    match (left, right) {
        (PlanExpr::Column { index, .. }, PlanExpr::Literal { value, .. }) => {
            Some((*index, value.clone(), *op))
        }
        (PlanExpr::Literal { value, .. }, PlanExpr::Column { index, .. }) => {
            // Reverse the operator: Literal op Column → Column reverse(op) Literal
            let reversed = match op {
                BinaryOp::Lt => BinaryOp::Gt,
                BinaryOp::LtEq => BinaryOp::GtEq,
                BinaryOp::Gt => BinaryOp::Lt,
                BinaryOp::GtEq => BinaryOp::LtEq,
                other => *other,
            };
            Some((*index, value.clone(), reversed))
        }
        _ => None,
    }
}

/// Check if column statistics for a row group prove no match against the predicate.
fn column_stats_prune(
    rg: &RowGroupMetaData,
    col_idx: usize,
    literal: &ScalarValue,
    op: BinaryOp,
) -> bool {
    if col_idx >= rg.num_columns() {
        return false;
    }
    let col = rg.column(col_idx);
    let stats = match col.statistics() {
        Some(s) => s,
        None => return false, // No stats → cannot prune
    };

    // Extract min/max from statistics and compare with the literal.
    match (stats, literal) {
        (Statistics::Int32(s), ScalarValue::Int32(v)) => prune_with_minmax_i64(
            s.min_opt().map(|x| *x as i64),
            s.max_opt().map(|x| *x as i64),
            *v as i64,
            op,
        ),
        (Statistics::Int64(s), ScalarValue::Int64(v)) => {
            prune_with_minmax_i64(s.min_opt().copied(), s.max_opt().copied(), *v, op)
        }
        (Statistics::Int32(s), ScalarValue::Date32(v)) => prune_with_minmax_i64(
            s.min_opt().map(|x| *x as i64),
            s.max_opt().map(|x| *x as i64),
            *v as i64,
            op,
        ),
        (Statistics::Int64(s), ScalarValue::Date32(v)) => {
            prune_with_minmax_i64(s.min_opt().copied(), s.max_opt().copied(), *v as i64, op)
        }
        (Statistics::Double(s), ScalarValue::Float64(v)) => {
            prune_with_minmax_f64(s.min_opt().copied(), s.max_opt().copied(), *v, op)
        }
        _ => false, // Unsupported type pair → don't prune
    }
}

/// Prune using integer-like min/max statistics.
/// Returns `true` if statistics prove the predicate cannot match any row.
fn prune_with_minmax_i64(
    min_opt: Option<i64>,
    max_opt: Option<i64>,
    literal: i64,
    op: BinaryOp,
) -> bool {
    let (min, max) = match (min_opt, max_opt) {
        (Some(min), Some(max)) => (min, max),
        _ => return false,
    };

    match op {
        // Column = literal: prune if literal outside [min, max]
        BinaryOp::Eq => literal < min || literal > max,
        // Column != literal: prune if entire range equals literal (min == max == literal)
        BinaryOp::NotEq => min == literal && max == literal,
        // Column < literal: prune if min >= literal (all values >= literal)
        BinaryOp::Lt => min >= literal,
        // Column <= literal: prune if min > literal
        BinaryOp::LtEq => min > literal,
        // Column > literal: prune if max <= literal (all values <= literal)
        BinaryOp::Gt => max <= literal,
        // Column >= literal: prune if max < literal
        BinaryOp::GtEq => max < literal,
        _ => false,
    }
}

fn prune_with_minmax_f64(
    min_opt: Option<f64>,
    max_opt: Option<f64>,
    literal: f64,
    op: BinaryOp,
) -> bool {
    let (min, max) = match (min_opt, max_opt) {
        (Some(min), Some(max)) => (min, max),
        _ => return false,
    };

    match op {
        BinaryOp::Eq => literal < min || literal > max,
        BinaryOp::NotEq => {
            (min - literal).abs() < f64::EPSILON && (max - literal).abs() < f64::EPSILON
        }
        BinaryOp::Lt => min >= literal,
        BinaryOp::LtEq => min > literal,
        BinaryOp::Gt => max <= literal,
        BinaryOp::GtEq => max < literal,
        _ => false,
    }
}

/// Try to build a `RowFilter` from plan expressions for predicate pushdown.
///
/// Returns `None` if no expressions can be translated. Unsupported expressions
/// are silently skipped — they remain as in-memory filters above the scan.
pub fn build_row_filter(
    filters: &[PlanExpr],
    parquet_schema: &SchemaDescriptor,
) -> Option<RowFilter> {
    let mut predicates: Vec<Box<dyn parquet::arrow::arrow_reader::ArrowPredicate>> = Vec::new();

    for filter in filters {
        if let Some(pred) = try_build_predicate(filter, parquet_schema) {
            predicates.push(pred);
        }
    }

    if predicates.is_empty() {
        None
    } else {
        Some(RowFilter::new(predicates))
    }
}

/// Try to translate a single PlanExpr into an ArrowPredicate.
fn try_build_predicate(
    filter: &PlanExpr,
    schema: &SchemaDescriptor,
) -> Option<Box<dyn parquet::arrow::arrow_reader::ArrowPredicate>> {
    match filter {
        PlanExpr::BinaryOp {
            left, op, right, ..
        } => {
            // AND: build predicates for both sides
            if *op == BinaryOp::And {
                // Just return the left side — the caller will process both
                // sides in the outer loop via flattened filters.
                // For now, skip AND and let the caller handle it.
                return None;
            }

            // Simple Column op Literal
            let (col_idx, scalar, cmp_op) = extract_column_literal_comparison(left, op, right)?;

            // Build projection mask for just this column
            let mask = ProjectionMask::leaves(schema, [col_idx]);

            // Build the predicate closure
            let predicate = build_comparison_predicate(scalar, cmp_op)?;

            Some(Box::new(ArrowPredicateFn::new(mask, predicate)))
        }
        _ => None,
    }
}

/// Build a closure that evaluates a comparison against a literal value.
fn build_comparison_predicate(
    literal: ScalarValue,
    op: BinaryOp,
) -> Option<
    impl FnMut(arrow::record_batch::RecordBatch) -> Result<BooleanArray, arrow::error::ArrowError>,
> {
    Some(
        move |batch: arrow::record_batch::RecordBatch| -> Result<BooleanArray, arrow::error::ArrowError> {
            let column = batch.column(0);
            let num_rows = column.len();

            // Create a scalar array from the literal
            let scalar_arr = match &literal {
                ScalarValue::Int32(v) => {
                    Arc::new(arrow::array::Int32Array::new_scalar(*v)) as Arc<dyn arrow::array::Datum>
                }
                ScalarValue::Int64(v) => {
                    Arc::new(arrow::array::Int64Array::new_scalar(*v)) as Arc<dyn arrow::array::Datum>
                }
                ScalarValue::Float64(v) => {
                    Arc::new(arrow::array::Float64Array::new_scalar(*v)) as Arc<dyn arrow::array::Datum>
                }
                ScalarValue::Date32(v) => {
                    Arc::new(arrow::array::Date32Array::new_scalar(*v)) as Arc<dyn arrow::array::Datum>
                }
                _ => {
                    // Unsupported type — return all true (don't filter)
                    return Ok(BooleanArray::from(vec![true; num_rows]));
                }
            };

            let result = match op {
                BinaryOp::Eq => kernels::cmp::eq(column, scalar_arr.as_ref())?,
                BinaryOp::NotEq => kernels::cmp::neq(column, scalar_arr.as_ref())?,
                BinaryOp::Lt => kernels::cmp::lt(column, scalar_arr.as_ref())?,
                BinaryOp::LtEq => kernels::cmp::lt_eq(column, scalar_arr.as_ref())?,
                BinaryOp::Gt => kernels::cmp::gt(column, scalar_arr.as_ref())?,
                BinaryOp::GtEq => kernels::cmp::gt_eq(column, scalar_arr.as_ref())?,
                _ => return Ok(BooleanArray::from(vec![true; num_rows])),
            };
            Ok(result)
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: RowGroupMetaData is hard to construct in tests without building
    // a real Parquet file. Integration tests in file.rs cover the full path.
    // These tests focus on the pruning logic helpers.

    #[test]
    fn prune_with_minmax_i64_eq() {
        // Range [10, 20], literal = 5 → prune (5 < 10)
        assert!(prune_with_minmax_i64(Some(10), Some(20), 5, BinaryOp::Eq));
        // Range [10, 20], literal = 25 → prune (25 > 20)
        assert!(prune_with_minmax_i64(Some(10), Some(20), 25, BinaryOp::Eq));
        // Range [10, 20], literal = 15 → don't prune
        assert!(!prune_with_minmax_i64(Some(10), Some(20), 15, BinaryOp::Eq));
    }

    #[test]
    fn prune_with_minmax_i64_lt() {
        // Column < 10, range [10, 20] → prune (min >= 10, no values < 10)
        assert!(prune_with_minmax_i64(Some(10), Some(20), 10, BinaryOp::Lt));
        // Column < 15, range [10, 20] → don't prune (min=10 < 15)
        assert!(!prune_with_minmax_i64(Some(10), Some(20), 15, BinaryOp::Lt));
    }

    #[test]
    fn prune_with_minmax_i64_gt() {
        // Column > 20, range [10, 20] → prune (max <= 20, no values > 20)
        assert!(prune_with_minmax_i64(Some(10), Some(20), 20, BinaryOp::Gt));
        // Column > 15, range [10, 20] → don't prune (max=20 > 15)
        assert!(!prune_with_minmax_i64(Some(10), Some(20), 15, BinaryOp::Gt));
    }

    #[test]
    fn prune_with_no_stats() {
        // No statistics → never prune
        assert!(!prune_with_minmax_i64(None, None, 5, BinaryOp::Eq));
        assert!(!prune_with_minmax_i64(Some(10), None, 5, BinaryOp::Lt));
    }
}
