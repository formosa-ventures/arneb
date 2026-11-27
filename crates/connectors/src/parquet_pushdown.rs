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

    // Prepared once per file: an IN list of a dynamic filter can hold
    // hundreds of thousands of values, too many to convert per row group.
    let checks: Vec<PruneCheck> = filters.iter().map(PruneCheck::new).collect();
    let mut selected = Vec::new();
    for (rg_idx, rg) in row_groups.iter().enumerate() {
        if checks.iter().any(|check| check.prunes(rg)) {
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

/// A filter readied for checks against row-group statistics.
enum PruneCheck {
    /// A conjunction: prunes if either side proves no match.
    And(Box<PruneCheck>, Box<PruneCheck>),
    /// `Column op Literal` (either order).
    Compare {
        column: usize,
        literal: ScalarValue,
        op: BinaryOp,
    },
    /// A non-negated `Column IN (literals)` of integer-shaped values, as
    /// `inject_inlist_dynamic_filters` produces, sorted for binary search.
    InList { column: usize, keys: Vec<i64> },
    /// Anything statistics cannot decide.
    Never,
}

impl PruneCheck {
    fn new(filter: &PlanExpr) -> Self {
        match filter {
            PlanExpr::BinaryOp {
                left, op, right, ..
            } => {
                if *op == BinaryOp::And {
                    return Self::And(Box::new(Self::new(left)), Box::new(Self::new(right)));
                }
                match extract_column_literal_comparison(left, op, right) {
                    Some((column, literal, op)) => Self::Compare {
                        column,
                        literal,
                        op,
                    },
                    None => Self::Never,
                }
            }
            PlanExpr::InList {
                expr,
                list,
                negated: false,
                ..
            } => {
                let PlanExpr::Column { index, .. } = expr.as_ref() else {
                    return Self::Never;
                };
                match inlist_keys(list) {
                    Some(keys) if !keys.is_empty() => Self::InList {
                        column: *index,
                        keys,
                    },
                    _ => Self::Never,
                }
            }
            _ => Self::Never,
        }
    }

    /// Whether statistics prove `rg` holds no row this filter keeps.
    fn prunes(&self, rg: &RowGroupMetaData) -> bool {
        match self {
            Self::And(left, right) => left.prunes(rg) || right.prunes(rg),
            Self::Compare {
                column,
                literal,
                op,
            } => column_stats_prune(rg, *column, literal, *op),
            Self::InList { column, keys } => inlist_stats_prune(rg, *column, keys),
            Self::Never => false,
        }
    }
}

/// The IN list's values as sorted integers, or `None` when an item is not
/// an integer-shaped literal (statistics then cannot prune).
fn inlist_keys(list: &[PlanExpr]) -> Option<Vec<i64>> {
    let mut keys = list
        .iter()
        .map(|item| match item {
            PlanExpr::Literal {
                value: ScalarValue::Int32(x) | ScalarValue::Date32(x),
                ..
            } => Some(*x as i64),
            PlanExpr::Literal {
                value: ScalarValue::Int64(x),
                ..
            } => Some(*x),
            _ => None,
        })
        .collect::<Option<Vec<i64>>>()?;
    keys.sort_unstable();
    Some(keys)
}

/// Returns `true` if every key in sorted `keys` falls outside the row
/// group's `[min, max]` range for column `col_idx`, proving that
/// `col IN (keys...)` cannot match any row in it. When stats are missing
/// or not integer-shaped, returns `false` (conservative: keep the row group).
fn inlist_stats_prune(rg: &RowGroupMetaData, col_idx: usize, keys: &[i64]) -> bool {
    if col_idx >= rg.num_columns() || keys.is_empty() {
        return false;
    }
    let (min, max) = match rg.column(col_idx).statistics() {
        Some(Statistics::Int32(s)) => match (s.min_opt(), s.max_opt()) {
            (Some(lo), Some(hi)) => (*lo as i64, *hi as i64),
            _ => return false,
        },
        Some(Statistics::Int64(s)) => match (s.min_opt(), s.max_opt()) {
            (Some(lo), Some(hi)) => (*lo, *hi),
            _ => return false,
        },
        _ => return false,
    };
    // The smallest key not below `min`; the row group may match only if
    // it is within the range.
    let first = keys.partition_point(|k| *k < min);
    first == keys.len() || keys[first] > max
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
    if dynamic_domain.column_index >= schema.num_columns() {
        return None;
    }
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

    // Bloom fast path for the integer key types: probe the bitmap without
    // building a `ScalarValue` (and a canonical-bytes Vec) per row. NULL
    // never matches.
    if let Domain::Bloom(bloom) = domain {
        use arrow::array::AsArray;
        match column.data_type() {
            arrow::datatypes::DataType::Int32 => {
                return Ok(column
                    .as_primitive::<arrow::datatypes::Int32Type>()
                    .iter()
                    .map(|v| Some(v.is_some_and(|x| bloom.contains_int32(x))))
                    .collect());
            }
            arrow::datatypes::DataType::Int64 => {
                return Ok(column
                    .as_primitive::<arrow::datatypes::Int64Type>()
                    .iter()
                    .map(|v| Some(v.is_some_and(|x| bloom.contains_int64(x))))
                    .collect());
            }
            arrow::datatypes::DataType::Date32 => {
                return Ok(column
                    .as_primitive::<arrow::datatypes::Date32Type>()
                    .iter()
                    .map(|v| Some(v.is_some_and(|x| bloom.contains_date32(x))))
                    .collect());
            }
            _ => {}
        }
    }

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

    /// Row groups of a real file: `rows` Int32 ids `0..rows`, `per_group`
    /// rows to a row group.
    fn id_row_groups(rows: i32, per_group: usize) -> Vec<RowGroupMetaData> {
        use parquet::file::reader::{FileReader, SerializedFileReader};
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int32, false),
        ]));
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow::array::Int32Array::from(
                (0..rows).collect::<Vec<_>>(),
            ))],
        )
        .unwrap();
        let file = tempfile::tempfile().unwrap();
        let props = parquet::file::properties::WriterProperties::builder()
            .set_max_row_group_row_count(Some(per_group))
            .build();
        let mut writer =
            parquet::arrow::ArrowWriter::try_new(file.try_clone().unwrap(), schema, Some(props))
                .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let reader = SerializedFileReader::new(file).unwrap();
        reader.metadata().row_groups().to_vec()
    }

    fn id_in(values: impl IntoIterator<Item = ScalarValue>) -> PlanExpr {
        PlanExpr::InList {
            expr: Box::new(PlanExpr::Column {
                index: 0,
                name: "id".to_string(),
                span: None,
            }),
            list: values
                .into_iter()
                .map(|value| PlanExpr::Literal { value, span: None })
                .collect(),
            negated: false,
            span: None,
        }
    }

    #[test]
    fn an_in_list_keeps_only_the_row_groups_its_values_fall_in() {
        // Row groups: [0, 99], [100, 199], ..., [900, 999].
        let groups = id_row_groups(1_000, 100);
        assert_eq!(groups.len(), 10);
        // Unsorted, with many values past the file and one in each of
        // groups 1 and 7, plus one on a group's upper bound (group 4).
        let mut values: Vec<ScalarValue> = (5_000..105_000).map(ScalarValue::Int32).collect();
        values.extend([
            ScalarValue::Int32(750),
            ScalarValue::Int32(150),
            ScalarValue::Int32(499),
        ]);
        assert_eq!(
            prune_row_groups(&groups, &[id_in(values)], &[]),
            vec![1, 4, 7]
        );
        // Int64 and Date32 literals compare the same way.
        let wide = id_in([ScalarValue::Int64(250), ScalarValue::Date32(0)]);
        assert_eq!(prune_row_groups(&groups, &[wide], &[]), vec![0, 2]);
        // None of the values in the file: every group is pruned.
        let absent = id_in([ScalarValue::Int32(-1), ScalarValue::Int32(1_000)]);
        assert!(prune_row_groups(&groups, &[absent], &[]).is_empty());
        // A value statistics cannot compare keeps every group.
        let mixed = id_in([ScalarValue::Int32(150), ScalarValue::Utf8("x".into())]);
        assert_eq!(prune_row_groups(&groups, &[mixed], &[]).len(), 10);
    }

    /// A dynamic filter's IN list can hold hundreds of thousands of values;
    /// pruning a file prepares it once, not once per row group (#291: 0.8 s
    /// per split of SF1 lineitem before).
    #[test]
    fn a_long_in_list_prunes_many_row_groups_quickly() {
        let groups = id_row_groups(100_000, 1_000);
        assert_eq!(groups.len(), 100);
        let values = (0..1_000_000)
            .rev()
            .map(|v| ScalarValue::Int32(v * 7 + 1_000_000));
        let filter = id_in(values);
        let started = std::time::Instant::now();
        assert!(prune_row_groups(&groups, &[filter], &[]).is_empty());
        assert!(
            started.elapsed() < std::time::Duration::from_secs(2),
            "took {:?}",
            started.elapsed()
        );
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
