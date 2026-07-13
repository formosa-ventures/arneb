//! Row group pruning and predicate pushdown for Parquet files.
//!
//! Provides utilities to skip row groups based on column statistics
//! and to translate plan expressions into Arrow predicates for
//! within-row-group filtering.

use std::sync::Arc;

use arneb_common::types::ScalarValue;
use arneb_common::Domain;
use arneb_execution::DynamicFilterDomain;
use arneb_planner::PlanExpr;
use arneb_sql_parser::ast::BinaryOp;
use arrow::array::{ArrayRef, BooleanArray};
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
        // Dynamic-filter InList: prune the row group if every list
        // value falls outside the column's [min, max] range. Only
        // applies to non-negated InList over a single column with
        // all-literal items (the shape produced by
        // `inject_inlist_dynamic_filters`).
        PlanExpr::InList {
            expr,
            list,
            negated: false,
            ..
        } => match (expr.as_ref(), extract_inlist_literals(list)) {
            (PlanExpr::Column { index, .. }, Some(values)) => {
                inlist_stats_prune(rg, *index, &values)
            }
            _ => false,
        },
        _ => false,
    }
}

/// Pulls all `ScalarValue`s out of an InList's item list when every
/// item is a Literal. Returns `None` if any item is non-literal.
fn extract_inlist_literals(list: &[PlanExpr]) -> Option<Vec<ScalarValue>> {
    let mut out = Vec::with_capacity(list.len());
    for item in list {
        match item {
            PlanExpr::Literal { value, .. } => out.push(value.clone()),
            _ => return None,
        }
    }
    Some(out)
}

/// Returns `true` if every value in `values` falls outside the
/// row group's `[min, max]` range for column `col_idx`, proving that
/// `col IN (values...)` cannot match any row in this row group.
/// When stats are missing or the type is unsupported, returns `false`
/// (conservative: keep the row group).
fn inlist_stats_prune(rg: &RowGroupMetaData, col_idx: usize, values: &[ScalarValue]) -> bool {
    if col_idx >= rg.num_columns() || values.is_empty() {
        return false;
    }
    let col = rg.column(col_idx);
    let stats = match col.statistics() {
        Some(s) => s,
        None => return false,
    };

    // Reduce all supported integer-shaped stats to `(min_i64, max_i64)`
    // and check each list value against the range. As soon as ONE
    // value is inside the range, we can't prune. If ALL values are
    // outside, we can.
    let (min, max) = match stats {
        Statistics::Int32(s) => match (s.min_opt(), s.max_opt()) {
            (Some(lo), Some(hi)) => (*lo as i64, *hi as i64),
            _ => return false,
        },
        Statistics::Int64(s) => match (s.min_opt(), s.max_opt()) {
            (Some(lo), Some(hi)) => (*lo, *hi),
            _ => return false,
        },
        _ => return false,
    };

    for v in values {
        let candidate: i64 = match v {
            ScalarValue::Int32(x) => *x as i64,
            ScalarValue::Int64(x) => *x,
            ScalarValue::Date32(x) => *x as i64,
            _ => return false, // unsupported value type in list → don't prune
        };
        if candidate >= min && candidate <= max {
            // At least one value is inside the row group's range —
            // can't safely prune.
            return false;
        }
    }
    true
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
    build_row_filter_with_dynamic_domains(filters, &[], parquet_schema)
}

/// Try to build a `RowFilter` from plan expressions and dynamic domains.
pub fn build_row_filter_with_dynamic_domains(
    filters: &[PlanExpr],
    dynamic_domains: &[DynamicFilterDomain],
    parquet_schema: &SchemaDescriptor,
) -> Option<RowFilter> {
    let mut predicates: Vec<Box<dyn parquet::arrow::arrow_reader::ArrowPredicate>> = Vec::new();

    for filter in filters {
        if let Some(pred) = try_build_predicate(filter, parquet_schema) {
            predicates.push(pred);
        }
    }
    for dynamic_domain in dynamic_domains {
        if let Some(pred) = try_build_dynamic_domain_predicate(dynamic_domain, parquet_schema) {
            predicates.push(pred);
        }
    }

    if predicates.is_empty() {
        None
    } else {
        Some(RowFilter::new(predicates))
    }
}

fn try_build_dynamic_domain_predicate(
    dynamic_domain: &DynamicFilterDomain,
    schema: &SchemaDescriptor,
) -> Option<Box<dyn parquet::arrow::arrow_reader::ArrowPredicate>> {
    let Domain::Bloom(domain) = &dynamic_domain.domain else {
        return None;
    };
    let mask = ProjectionMask::leaves(schema, [dynamic_domain.column_index]);
    let domain = Domain::Bloom(domain.clone());
    let predicate =
        move |batch: arrow::record_batch::RecordBatch| -> Result<BooleanArray, arrow::error::ArrowError> {
            build_domain_filter_mask(&domain, batch.column(0))
        };
    Some(Box::new(ArrowPredicateFn::new(mask, predicate)))
}

fn build_domain_filter_mask(
    domain: &Domain,
    column: &ArrayRef,
) -> Result<BooleanArray, arrow::error::ArrowError> {
    use arrow::array::{Array, BooleanBuilder};

    let n = column.len();
    let mut builder = BooleanBuilder::with_capacity(n);
    match column.data_type() {
        arrow::datatypes::DataType::Int32 => {
            let a = column
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .ok_or_else(|| {
                    arrow::error::ArrowError::CastError(
                        "expected Int32 column for dynamic filter".into(),
                    )
                })?;
            for i in 0..n {
                if a.is_null(i) {
                    builder.append_value(false);
                } else {
                    builder.append_value(domain.contains(&ScalarValue::Int32(a.value(i))));
                }
            }
        }
        arrow::datatypes::DataType::Int64 => {
            let a = column
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .ok_or_else(|| {
                    arrow::error::ArrowError::CastError(
                        "expected Int64 column for dynamic filter".into(),
                    )
                })?;
            for i in 0..n {
                if a.is_null(i) {
                    builder.append_value(false);
                } else {
                    builder.append_value(domain.contains(&ScalarValue::Int64(a.value(i))));
                }
            }
        }
        arrow::datatypes::DataType::Date32 => {
            let a = column
                .as_any()
                .downcast_ref::<arrow::array::Date32Array>()
                .ok_or_else(|| {
                    arrow::error::ArrowError::CastError(
                        "expected Date32 column for dynamic filter".into(),
                    )
                })?;
            for i in 0..n {
                if a.is_null(i) {
                    builder.append_value(false);
                } else {
                    builder.append_value(domain.contains(&ScalarValue::Date32(a.value(i))));
                }
            }
        }
        arrow::datatypes::DataType::Utf8 => {
            let a = column
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .ok_or_else(|| {
                    arrow::error::ArrowError::CastError(
                        "expected Utf8 column for dynamic filter".into(),
                    )
                })?;
            for i in 0..n {
                if a.is_null(i) {
                    builder.append_value(false);
                } else {
                    builder
                        .append_value(domain.contains(&ScalarValue::Utf8(a.value(i).to_string())));
                }
            }
        }
        other => {
            return Err(arrow::error::ArrowError::CastError(format!(
                "unsupported dynamic filter column type {other:?}"
            )));
        }
    }
    Ok(builder.finish())
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
        // InList from dynamic filters — every item is a Literal,
        // single-column reference, non-negated. Build a typed
        // HashSet-backed `ArrowPredicate` that lets the Parquet
        // reader filter rows before they reach the engine.
        PlanExpr::InList {
            expr,
            list,
            negated: false,
            ..
        } => {
            let PlanExpr::Column { index, .. } = expr.as_ref() else {
                return None;
            };
            let values = extract_inlist_literals(list)?;
            let mask = ProjectionMask::leaves(schema, [*index]);
            let predicate = build_inlist_predicate(values)?;
            Some(Box::new(ArrowPredicateFn::new(mask, predicate)))
        }
        _ => None,
    }
}

/// Build a closure that evaluates `col IN (values...)` over a Parquet
/// batch. Specialised by element type (Int32/Int64/Date32/Utf8) — the
/// types `inject_inlist_dynamic_filters` produces and the same types
/// covered by `evaluate_inlist_hashset` in the execution layer.
fn build_inlist_predicate(
    values: Vec<ScalarValue>,
) -> Option<
    impl FnMut(arrow::record_batch::RecordBatch) -> Result<BooleanArray, arrow::error::ArrowError>,
> {
    use std::collections::HashSet;

    // Bucket the literals by Arrow type so the closure can do a
    // single typed `.contains` per row. Aborts if `values` mixes
    // incompatible types (caller should have produced uniform).
    let mut i64_set: Option<HashSet<i64>> = None;
    let mut i32_set: Option<HashSet<i32>> = None;
    let mut date32_set: Option<HashSet<i32>> = None;
    let mut utf8_set: Option<HashSet<String>> = None;

    for v in &values {
        match v {
            ScalarValue::Int64(x) => {
                i64_set.get_or_insert_with(HashSet::new).insert(*x);
            }
            ScalarValue::Int32(x) => {
                i32_set.get_or_insert_with(HashSet::new).insert(*x);
            }
            ScalarValue::Date32(x) => {
                date32_set.get_or_insert_with(HashSet::new).insert(*x);
            }
            ScalarValue::Utf8(s) => {
                utf8_set.get_or_insert_with(HashSet::new).insert(s.clone());
            }
            _ => return None, // unsupported type → bail; caller leaves filter to FilterExec
        }
    }

    // Require uniform types for the v1 predicate path.
    let n_kinds = [
        i64_set.is_some(),
        i32_set.is_some(),
        date32_set.is_some(),
        utf8_set.is_some(),
    ]
    .iter()
    .filter(|b| **b)
    .count();
    if n_kinds != 1 {
        return None;
    }

    Some(
        move |batch: arrow::record_batch::RecordBatch| -> Result<BooleanArray, arrow::error::ArrowError> {
            use arrow::array::{Array, BooleanBuilder};
            let column = batch.column(0);
            let n = column.len();
            let mut builder = BooleanBuilder::with_capacity(n);

            if let Some(set) = &i64_set {
                let a = column.as_any().downcast_ref::<arrow::array::Int64Array>().ok_or_else(|| {
                    arrow::error::ArrowError::CastError("expected Int64 column for InList".into())
                })?;
                for i in 0..n {
                    if a.is_null(i) {
                        builder.append_null();
                    } else {
                        builder.append_value(set.contains(&a.value(i)));
                    }
                }
            } else if let Some(set) = &i32_set {
                let a = column.as_any().downcast_ref::<arrow::array::Int32Array>().ok_or_else(|| {
                    arrow::error::ArrowError::CastError("expected Int32 column for InList".into())
                })?;
                for i in 0..n {
                    if a.is_null(i) {
                        builder.append_null();
                    } else {
                        builder.append_value(set.contains(&a.value(i)));
                    }
                }
            } else if let Some(set) = &date32_set {
                let a = column.as_any().downcast_ref::<arrow::array::Date32Array>().ok_or_else(|| {
                    arrow::error::ArrowError::CastError("expected Date32 column for InList".into())
                })?;
                for i in 0..n {
                    if a.is_null(i) {
                        builder.append_null();
                    } else {
                        builder.append_value(set.contains(&a.value(i)));
                    }
                }
            } else if let Some(set) = &utf8_set {
                let a = column.as_any().downcast_ref::<arrow::array::StringArray>().ok_or_else(|| {
                    arrow::error::ArrowError::CastError("expected Utf8 column for InList".into())
                })?;
                for i in 0..n {
                    if a.is_null(i) {
                        builder.append_null();
                    } else {
                        builder.append_value(set.contains(a.value(i)));
                    }
                }
            }
            Ok(builder.finish())
        },
    )
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

    #[test]
    fn bloom_probe_filter_admits_inserted_keys_and_rejects_absent_key() {
        let mut bloom = arneb_common::BloomFilter::with_fixed_params();
        bloom.insert(&ScalarValue::Int64(10));
        bloom.insert(&ScalarValue::Int64(20));
        bloom.insert(&ScalarValue::Int64(30));
        let domain = Domain::Bloom(bloom);
        let column: ArrayRef = Arc::new(arrow::array::Int64Array::from(vec![
            Some(10),
            Some(20),
            Some(9_876_543_210),
            None,
            Some(30),
        ]));

        let mask = build_domain_filter_mask(&domain, &column).unwrap();
        assert!(mask.value(0));
        assert!(mask.value(1));
        assert!(!mask.value(2));
        assert!(!mask.value(3));
        assert!(mask.value(4));
    }
}
