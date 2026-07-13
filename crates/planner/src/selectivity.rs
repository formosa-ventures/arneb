//! Per-predicate selectivity estimator for the cost-based join reorderer.
//!
//! Selectivity is the fraction of input rows that satisfy a predicate,
//! clamped to `[0.0, 1.0]`. The estimator walks a `PlanExpr` tree and
//! applies per-shape rules (see design.md Decision 3). When statistics are
//! missing, it falls back to conservative defaults so the reorderer never
//! over-trusts an under-described predicate.
//!
//! The module is decoupled from `LogicalPlan` traversal via the
//! `ColumnStatsLookup` trait: callers (notably the cost model's `Filter`
//! arm) implement column→stats resolution; the estimator only consumes
//! the resolved `ColumnStatistics`.

use arneb_catalog::ColumnStatistics;
use arneb_common::types::ScalarValue;
use arneb_sql_parser::ast::{BinaryOp, UnaryOp};

use crate::plan::PlanExpr;

// ---------------------------------------------------------------------------
// Public defaults (tunable in one place)
// ---------------------------------------------------------------------------

/// Default selectivity for an equality predicate when NDV is unknown.
pub const DEFAULT_EQ_SELECTIVITY: f64 = 0.1;

/// Default selectivity for a range predicate (`<`, `<=`, `>`, `>=`) when
/// `min`/`max` is unknown.
pub const DEFAULT_RANGE_SELECTIVITY: f64 = 0.33;

/// Default selectivity for `BETWEEN` when `min`/`max` is unknown.
pub const DEFAULT_BETWEEN_SELECTIVITY: f64 = 0.25;

/// Default selectivity for `LIKE` predicates.
pub const DEFAULT_LIKE_SELECTIVITY: f64 = 0.1;

/// Default null fraction used when `null_fraction` is unknown.
pub const DEFAULT_NULL_SELECTIVITY: f64 = 0.05;

/// Default selectivity for any predicate shape not otherwise modeled.
pub const DEFAULT_UNKNOWN_SELECTIVITY: f64 = 0.5;

/// Selectivity assumed for `aggregate_function(col) <comparison> literal`
/// — HAVING-style filters above an `Aggregate`. Users writing
/// `HAVING SUM(...) > N` typically pick `N` to cut down to the tail
/// of the distribution (otherwise the filter is moot), so this is
/// much more aggressive than the generic range default. Calibrated
/// against TPC-H Q18 (`HAVING SUM(l_quantity) > 300` keeps ~0.004%
/// of group keys at SF1) — we don't reach single-percent but cut
/// the cardinality estimate enough that JoinReorder picks the
/// aggregate output as an early build side.
pub const HAVING_AGGREGATE_SELECTIVITY: f64 = 0.05;

// ---------------------------------------------------------------------------
// Column-stats resolution trait
// ---------------------------------------------------------------------------

/// Resolves a column name to its `ColumnStatistics`, if known.
///
/// Implementors decide how to map a name to a specific table (e.g., walk
/// a `LogicalPlan` subtree to find the originating `TableScan`). The
/// estimator does not need to know about plan structure.
pub trait ColumnStatsLookup {
    /// Returns the stats for `column`, or `None` if no information is known.
    fn lookup(&self, column: &str) -> Option<ColumnStatistics>;
}

/// Lookup that always returns `None`. Useful for unit tests of fallback
/// paths and as a default placeholder.
pub struct EmptyLookup;
impl ColumnStatsLookup for EmptyLookup {
    fn lookup(&self, _column: &str) -> Option<ColumnStatistics> {
        None
    }
}

// ---------------------------------------------------------------------------
// Estimator entry point
// ---------------------------------------------------------------------------

/// Estimates the selectivity of `predicate` in `[0.0, 1.0]`.
///
/// The result is always finite, non-negative, and `<= 1.0`. Unknown
/// predicate shapes fall back to `DEFAULT_UNKNOWN_SELECTIVITY`.
pub fn selectivity(predicate: &PlanExpr, lookup: &dyn ColumnStatsLookup) -> f64 {
    let raw = estimate(predicate, lookup);
    clamp01(raw)
}

fn estimate(predicate: &PlanExpr, lookup: &dyn ColumnStatsLookup) -> f64 {
    match predicate {
        // Boolean combinators
        PlanExpr::BinaryOp {
            left, op, right, ..
        } => match op {
            BinaryOp::And => estimate(left, lookup) * estimate(right, lookup),
            BinaryOp::Or => {
                let l = estimate(left, lookup);
                let r = estimate(right, lookup);
                l + r - l * r
            }
            // Comparisons handled below
            _ => binary_comparison(left, *op, right, lookup),
        },

        PlanExpr::UnaryOp {
            op: UnaryOp::Not,
            expr,
            ..
        } => 1.0 - estimate(expr, lookup),
        // Unary minus / plus on a boolean is nonsensical; fall back.
        PlanExpr::UnaryOp { .. } => DEFAULT_UNKNOWN_SELECTIVITY,

        PlanExpr::IsNull { expr, .. } => match column_name(expr) {
            Some(name) => lookup
                .lookup(name)
                .and_then(|c| c.null_fraction)
                .unwrap_or(DEFAULT_NULL_SELECTIVITY),
            None => DEFAULT_NULL_SELECTIVITY,
        },
        PlanExpr::IsNotNull { expr, .. } => {
            let null_frac = match column_name(expr) {
                Some(name) => lookup
                    .lookup(name)
                    .and_then(|c| c.null_fraction)
                    .unwrap_or(DEFAULT_NULL_SELECTIVITY),
                None => DEFAULT_NULL_SELECTIVITY,
            };
            1.0 - null_frac
        }

        PlanExpr::Between {
            expr,
            low,
            high,
            negated,
            ..
        } => {
            let raw = between_selectivity(expr, low, high, lookup);
            if *negated {
                1.0 - raw
            } else {
                raw
            }
        }

        PlanExpr::InList {
            expr,
            list,
            negated,
            ..
        } => {
            let raw = in_list_selectivity(expr, list, lookup);
            if *negated {
                1.0 - raw
            } else {
                raw
            }
        }

        // Any other expression shape (function call, scalar subquery,
        // wildcard, etc.) is treated as opaque.
        _ => DEFAULT_UNKNOWN_SELECTIVITY,
    }
}

fn binary_comparison(
    left: &PlanExpr,
    op: BinaryOp,
    right: &PlanExpr,
    lookup: &dyn ColumnStatsLookup,
) -> f64 {
    // HAVING-style filters: `<agg_fn>(col) <op> <literal>` (or flipped).
    // Recognise this shape before the (col, literal) ordering check
    // below so we don't fall to `DEFAULT_UNKNOWN_SELECTIVITY` (0.5) —
    // that estimate is wildly off for TPC-H Q18's
    // `HAVING SUM(l_quantity) > 300`, which keeps 4×10⁻⁵ of group
    // keys. Without this override, JoinReorder underestimates how
    // much the post-aggregate Filter shrinks the dedup subquery and
    // places it last in the join chain instead of first.
    let l_is_agg = is_aggregate_function_call(left);
    let r_is_agg = is_aggregate_function_call(right);
    let l_is_lit = literal_value(left).is_some();
    let r_is_lit = literal_value(right).is_some();
    if (l_is_agg && r_is_lit) || (r_is_agg && l_is_lit) {
        return HAVING_AGGREGATE_SELECTIVITY;
    }

    // Identify the (column, literal) operand ordering for asymmetric ops.
    let (col_side, lit_side, op_view) = match (column_name(left), literal_value(right)) {
        (Some(_), Some(_)) => (Side::Left, Side::Right, op),
        _ => match (column_name(right), literal_value(left)) {
            // Swap operands for asymmetric ops to keep column on the left.
            (Some(_), Some(_)) => (Side::Right, Side::Left, flip(op)),
            _ => return DEFAULT_UNKNOWN_SELECTIVITY,
        },
    };

    let col = match col_side {
        Side::Left => column_name(left),
        Side::Right => column_name(right),
    };
    let col_name = match col {
        Some(n) => n,
        None => return DEFAULT_UNKNOWN_SELECTIVITY,
    };

    let lit = match lit_side {
        Side::Left => literal_value(left),
        Side::Right => literal_value(right),
    };
    let lit_value = lit.expect("literal_value guaranteed by ordering above");

    let stats = lookup.lookup(col_name);
    match op_view {
        BinaryOp::Eq => eq_selectivity(stats.as_ref()),
        BinaryOp::NotEq => 1.0 - eq_selectivity(stats.as_ref()),
        BinaryOp::Lt | BinaryOp::LtEq => {
            range_selectivity_le(stats.as_ref(), &lit_value).unwrap_or(DEFAULT_RANGE_SELECTIVITY)
        }
        BinaryOp::Gt | BinaryOp::GtEq => {
            range_selectivity_ge(stats.as_ref(), &lit_value).unwrap_or(DEFAULT_RANGE_SELECTIVITY)
        }
        // LIKE / NOT LIKE — the constants existed since the selectivity
        // module was first written but were never wired into the dispatch,
        // so `BinaryOp::Like` fell through to UNKNOWN (0.5). For TPC-H Q09's
        // `p_name LIKE '%green%'` that left filtered-part estimated 5× too
        // large, which pushed it to the END of the join chain instead of
        // joining it with lineitem first — 4 downstream joins then probed
        // 3M lineitem rows that should have been 160k.
        BinaryOp::Like => DEFAULT_LIKE_SELECTIVITY,
        BinaryOp::NotLike => 1.0 - DEFAULT_LIKE_SELECTIVITY,
        _ => DEFAULT_UNKNOWN_SELECTIVITY,
    }
}

fn eq_selectivity(stats: Option<&ColumnStatistics>) -> f64 {
    match stats.and_then(|s| s.ndv) {
        Some(ndv) if ndv > 0 => 1.0 / ndv as f64,
        _ => DEFAULT_EQ_SELECTIVITY,
    }
}

fn between_selectivity(
    expr: &PlanExpr,
    low: &PlanExpr,
    high: &PlanExpr,
    lookup: &dyn ColumnStatsLookup,
) -> f64 {
    let col_name = match column_name(expr) {
        Some(n) => n,
        None => return DEFAULT_BETWEEN_SELECTIVITY,
    };
    let lo = match literal_value(low) {
        Some(v) => v,
        None => return DEFAULT_BETWEEN_SELECTIVITY,
    };
    let hi = match literal_value(high) {
        Some(v) => v,
        None => return DEFAULT_BETWEEN_SELECTIVITY,
    };

    let stats = lookup.lookup(col_name);
    range_fraction(stats.as_ref(), &lo, &hi).unwrap_or(DEFAULT_BETWEEN_SELECTIVITY)
}

fn in_list_selectivity(expr: &PlanExpr, list: &[PlanExpr], lookup: &dyn ColumnStatsLookup) -> f64 {
    let col_name = match column_name(expr) {
        Some(n) => n,
        None => return clamp01(DEFAULT_EQ_SELECTIVITY * list.len() as f64),
    };
    let k = list.len() as f64;
    let stats = lookup.lookup(col_name);
    match stats.and_then(|s| s.ndv) {
        Some(ndv) if ndv > 0 => (k / ndv as f64).min(1.0),
        _ => (DEFAULT_EQ_SELECTIVITY * k).min(1.0),
    }
}

// ---------------------------------------------------------------------------
// Range selectivity helpers (Int64/Float64 first-class; Utf8 ignored)
// ---------------------------------------------------------------------------

/// `col <= literal` — fraction of `[min, max]` at or below `literal`.
fn range_selectivity_le(stats: Option<&ColumnStatistics>, literal: &ScalarValue) -> Option<f64> {
    let stats = stats?;
    let min = stats.min_value.as_ref()?;
    let max = stats.max_value.as_ref()?;
    let (min_f, max_f, lit_f) = (numeric(min)?, numeric(max)?, numeric(literal)?);
    if max_f <= min_f {
        return None;
    }
    let frac = ((lit_f - min_f) / (max_f - min_f)).clamp(0.0, 1.0);
    Some(frac)
}

/// `col >= literal` — fraction of `[min, max]` at or above `literal`.
fn range_selectivity_ge(stats: Option<&ColumnStatistics>, literal: &ScalarValue) -> Option<f64> {
    let stats = stats?;
    let min = stats.min_value.as_ref()?;
    let max = stats.max_value.as_ref()?;
    let (min_f, max_f, lit_f) = (numeric(min)?, numeric(max)?, numeric(literal)?);
    if max_f <= min_f {
        return None;
    }
    let frac = ((max_f - lit_f) / (max_f - min_f)).clamp(0.0, 1.0);
    Some(frac)
}

/// `col BETWEEN lo AND hi`.
fn range_fraction(
    stats: Option<&ColumnStatistics>,
    lo: &ScalarValue,
    hi: &ScalarValue,
) -> Option<f64> {
    let stats = stats?;
    let min = stats.min_value.as_ref()?;
    let max = stats.max_value.as_ref()?;
    let (min_f, max_f, lo_f, hi_f) = (numeric(min)?, numeric(max)?, numeric(lo)?, numeric(hi)?);
    if max_f <= min_f {
        return None;
    }
    let frac = ((hi_f - lo_f) / (max_f - min_f)).clamp(0.0, 1.0);
    Some(frac)
}

fn numeric(v: &ScalarValue) -> Option<f64> {
    match v {
        ScalarValue::Int32(x) => Some(*x as f64),
        ScalarValue::Int64(x) => Some(*x as f64),
        ScalarValue::Float32(x) => Some(*x as f64),
        ScalarValue::Float64(x) => Some(*x),
        ScalarValue::Decimal128 { value, scale, .. } => {
            // Best-effort: convert to f64 (precision loss accepted for
            // selectivity; conservatism comes from the clamp above).
            let scale_factor = 10_f64.powi(*scale as i32);
            Some(*value as f64 / scale_factor)
        }
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

enum Side {
    Left,
    Right,
}

fn flip(op: BinaryOp) -> BinaryOp {
    match op {
        BinaryOp::Lt => BinaryOp::Gt,
        BinaryOp::LtEq => BinaryOp::GtEq,
        BinaryOp::Gt => BinaryOp::Lt,
        BinaryOp::GtEq => BinaryOp::LtEq,
        // Symmetric ops stay the same.
        other => other,
    }
}

/// `true` when `expr` is a SQL aggregate function call (or wrapped in
/// a `Cast` over one). Used by [`binary_comparison`] to detect the
/// HAVING-filter shape `<agg_fn>(col) <op> <literal>`.
fn is_aggregate_function_call(expr: &PlanExpr) -> bool {
    match expr {
        PlanExpr::Function { name, .. } => matches!(
            name.to_uppercase().as_str(),
            "SUM" | "COUNT" | "AVG" | "MIN" | "MAX" | "BOOL_OR"
        ),
        PlanExpr::Cast { expr, .. } => is_aggregate_function_call(expr),
        PlanExpr::BinaryOp { left, right, .. } => {
            // Allow patterns like `0.5 * SUM(...)` to still register
            // as an aggregate-driven expression.
            is_aggregate_function_call(left) || is_aggregate_function_call(right)
        }
        _ => false,
    }
}

fn column_name(expr: &PlanExpr) -> Option<&str> {
    match expr {
        PlanExpr::Column { name, .. } => Some(name.as_str()),
        // A `Cast` over a column is still a column reference for
        // selectivity purposes.
        PlanExpr::Cast { expr, .. } => column_name(expr),
        _ => None,
    }
}

fn literal_value(expr: &PlanExpr) -> Option<ScalarValue> {
    match expr {
        PlanExpr::Literal { value, .. } => Some(value.clone()),
        PlanExpr::Cast { expr, .. } => literal_value(expr),
        _ => None,
    }
}

fn clamp01(x: f64) -> f64 {
    if !x.is_finite() || x < 0.0 {
        0.0
    } else if x > 1.0 {
        1.0
    } else {
        x
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    struct MapLookup(HashMap<String, ColumnStatistics>);
    impl ColumnStatsLookup for MapLookup {
        fn lookup(&self, column: &str) -> Option<ColumnStatistics> {
            self.0.get(column).cloned()
        }
    }

    fn col(name: &str) -> PlanExpr {
        PlanExpr::Column {
            index: 0,
            name: name.to_string(),
            span: None,
        }
    }
    fn lit_i64(v: i64) -> PlanExpr {
        PlanExpr::Literal {
            value: ScalarValue::Int64(v),
            span: None,
        }
    }
    fn bin(left: PlanExpr, op: BinaryOp, right: PlanExpr) -> PlanExpr {
        PlanExpr::BinaryOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
            span: None,
        }
    }
    fn not(expr: PlanExpr) -> PlanExpr {
        PlanExpr::UnaryOp {
            op: UnaryOp::Not,
            expr: Box::new(expr),
            span: None,
        }
    }
    fn empty() -> EmptyLookup {
        EmptyLookup
    }
    fn map_lookup<I: IntoIterator<Item = (&'static str, ColumnStatistics)>>(it: I) -> MapLookup {
        MapLookup(it.into_iter().map(|(k, v)| (k.to_string(), v)).collect())
    }

    // -- Equality --

    #[test]
    fn eq_with_ndv_returns_one_over_ndv() {
        let lookup = map_lookup([(
            "c",
            ColumnStatistics {
                ndv: Some(25),
                ..ColumnStatistics::default()
            },
        )]);
        let predicate = bin(col("c"), BinaryOp::Eq, lit_i64(5));
        assert!((selectivity(&predicate, &lookup) - 1.0 / 25.0).abs() < 1e-9);
    }

    #[test]
    fn eq_without_ndv_uses_default() {
        let predicate = bin(col("c"), BinaryOp::Eq, lit_i64(5));
        assert_eq!(selectivity(&predicate, &empty()), DEFAULT_EQ_SELECTIVITY);
    }

    #[test]
    fn eq_with_zero_ndv_falls_back() {
        let lookup = map_lookup([(
            "c",
            ColumnStatistics {
                ndv: Some(0),
                ..ColumnStatistics::default()
            },
        )]);
        let predicate = bin(col("c"), BinaryOp::Eq, lit_i64(5));
        assert_eq!(selectivity(&predicate, &lookup), DEFAULT_EQ_SELECTIVITY);
    }

    #[test]
    fn neq_is_complement_of_eq() {
        let lookup = map_lookup([(
            "c",
            ColumnStatistics {
                ndv: Some(10),
                ..ColumnStatistics::default()
            },
        )]);
        let predicate = bin(col("c"), BinaryOp::NotEq, lit_i64(5));
        assert!((selectivity(&predicate, &lookup) - 0.9).abs() < 1e-9);
    }

    // -- Range --

    #[test]
    fn lt_with_min_max() {
        let lookup = map_lookup([(
            "q",
            ColumnStatistics {
                min_value: Some(ScalarValue::Int64(1)),
                max_value: Some(ScalarValue::Int64(50)),
                ..ColumnStatistics::default()
            },
        )]);
        let predicate = bin(col("q"), BinaryOp::Lt, lit_i64(25));
        let expected = (25.0 - 1.0) / (50.0 - 1.0);
        assert!((selectivity(&predicate, &lookup) - expected).abs() < 1e-9);
    }

    #[test]
    fn gt_with_min_max() {
        let lookup = map_lookup([(
            "q",
            ColumnStatistics {
                min_value: Some(ScalarValue::Int64(1)),
                max_value: Some(ScalarValue::Int64(50)),
                ..ColumnStatistics::default()
            },
        )]);
        let predicate = bin(col("q"), BinaryOp::Gt, lit_i64(40));
        let expected = (50.0 - 40.0) / (50.0 - 1.0);
        assert!((selectivity(&predicate, &lookup) - expected).abs() < 1e-9);
    }

    #[test]
    fn lt_without_min_max_uses_default() {
        let predicate = bin(col("c"), BinaryOp::Lt, lit_i64(100));
        assert_eq!(selectivity(&predicate, &empty()), DEFAULT_RANGE_SELECTIVITY);
    }

    #[test]
    fn literal_on_left_is_handled() {
        // `100 > col` should be equivalent to `col < 100`.
        let lookup = map_lookup([(
            "q",
            ColumnStatistics {
                min_value: Some(ScalarValue::Int64(0)),
                max_value: Some(ScalarValue::Int64(200)),
                ..ColumnStatistics::default()
            },
        )]);
        let lhs_form = bin(lit_i64(100), BinaryOp::Gt, col("q"));
        let rhs_form = bin(col("q"), BinaryOp::Lt, lit_i64(100));
        let a = selectivity(&lhs_form, &lookup);
        let b = selectivity(&rhs_form, &lookup);
        assert!((a - b).abs() < 1e-9);
    }

    // -- BETWEEN --

    #[test]
    fn between_with_min_max() {
        let lookup = map_lookup([(
            "q",
            ColumnStatistics {
                min_value: Some(ScalarValue::Int64(1)),
                max_value: Some(ScalarValue::Int64(101)),
                ..ColumnStatistics::default()
            },
        )]);
        let predicate = PlanExpr::Between {
            expr: Box::new(col("q")),
            negated: false,
            low: Box::new(lit_i64(20)),
            high: Box::new(lit_i64(40)),
            span: None,
        };
        let expected = (40.0 - 20.0) / (101.0 - 1.0);
        assert!((selectivity(&predicate, &lookup) - expected).abs() < 1e-9);
    }

    #[test]
    fn between_without_min_max_uses_default() {
        let predicate = PlanExpr::Between {
            expr: Box::new(col("q")),
            negated: false,
            low: Box::new(lit_i64(20)),
            high: Box::new(lit_i64(40)),
            span: None,
        };
        assert_eq!(
            selectivity(&predicate, &empty()),
            DEFAULT_BETWEEN_SELECTIVITY
        );
    }

    #[test]
    fn not_between_is_complement() {
        let lookup = map_lookup([(
            "q",
            ColumnStatistics {
                min_value: Some(ScalarValue::Int64(0)),
                max_value: Some(ScalarValue::Int64(100)),
                ..ColumnStatistics::default()
            },
        )]);
        let pos = PlanExpr::Between {
            expr: Box::new(col("q")),
            negated: false,
            low: Box::new(lit_i64(10)),
            high: Box::new(lit_i64(30)),
            span: None,
        };
        let neg = PlanExpr::Between {
            expr: Box::new(col("q")),
            negated: true,
            low: Box::new(lit_i64(10)),
            high: Box::new(lit_i64(30)),
            span: None,
        };
        let a = selectivity(&pos, &lookup);
        let b = selectivity(&neg, &lookup);
        assert!((a + b - 1.0).abs() < 1e-9);
    }

    // -- IN --

    #[test]
    fn in_list_with_ndv() {
        let lookup = map_lookup([(
            "c",
            ColumnStatistics {
                ndv: Some(50),
                ..ColumnStatistics::default()
            },
        )]);
        let predicate = PlanExpr::InList {
            expr: Box::new(col("c")),
            list: (1..=5).map(lit_i64).collect(),
            negated: false,
            span: None,
        };
        assert!((selectivity(&predicate, &lookup) - 0.1).abs() < 1e-9);
    }

    #[test]
    fn in_list_caps_at_one() {
        let lookup = map_lookup([(
            "c",
            ColumnStatistics {
                ndv: Some(5),
                ..ColumnStatistics::default()
            },
        )]);
        let predicate = PlanExpr::InList {
            expr: Box::new(col("c")),
            list: (1..=100).map(lit_i64).collect(),
            negated: false,
            span: None,
        };
        assert!((selectivity(&predicate, &lookup) - 1.0).abs() < 1e-9);
    }

    #[test]
    fn not_in_is_complement() {
        let lookup = map_lookup([(
            "c",
            ColumnStatistics {
                ndv: Some(50),
                ..ColumnStatistics::default()
            },
        )]);
        let pos = PlanExpr::InList {
            expr: Box::new(col("c")),
            list: (1..=5).map(lit_i64).collect(),
            negated: false,
            span: None,
        };
        let neg = PlanExpr::InList {
            expr: Box::new(col("c")),
            list: (1..=5).map(lit_i64).collect(),
            negated: true,
            span: None,
        };
        let a = selectivity(&pos, &lookup);
        let b = selectivity(&neg, &lookup);
        assert!((a + b - 1.0).abs() < 1e-9);
    }

    // -- IS NULL / IS NOT NULL --

    #[test]
    fn is_null_uses_null_fraction() {
        let lookup = map_lookup([(
            "c",
            ColumnStatistics {
                null_fraction: Some(0.15),
                ..ColumnStatistics::default()
            },
        )]);
        let predicate = PlanExpr::IsNull {
            expr: Box::new(col("c")),
            span: None,
        };
        assert!((selectivity(&predicate, &lookup) - 0.15).abs() < 1e-9);
    }

    #[test]
    fn is_null_without_stats_uses_default() {
        let predicate = PlanExpr::IsNull {
            expr: Box::new(col("c")),
            span: None,
        };
        assert_eq!(selectivity(&predicate, &empty()), DEFAULT_NULL_SELECTIVITY);
    }

    #[test]
    fn is_not_null_is_complement() {
        let lookup = map_lookup([(
            "c",
            ColumnStatistics {
                null_fraction: Some(0.2),
                ..ColumnStatistics::default()
            },
        )]);
        let predicate = PlanExpr::IsNotNull {
            expr: Box::new(col("c")),
            span: None,
        };
        assert!((selectivity(&predicate, &lookup) - 0.8).abs() < 1e-9);
    }

    // -- AND / OR / NOT --

    #[test]
    fn and_multiplies_under_independence() {
        let lookup = map_lookup([
            (
                "a",
                ColumnStatistics {
                    ndv: Some(10),
                    ..ColumnStatistics::default()
                },
            ),
            (
                "b",
                ColumnStatistics {
                    ndv: Some(2),
                    ..ColumnStatistics::default()
                },
            ),
        ]);
        let predicate = bin(
            bin(col("a"), BinaryOp::Eq, lit_i64(1)),
            BinaryOp::And,
            bin(col("b"), BinaryOp::Eq, lit_i64(1)),
        );
        // sel(a=1) = 0.1, sel(b=1) = 0.5 → and = 0.05
        assert!((selectivity(&predicate, &lookup) - 0.05).abs() < 1e-9);
    }

    #[test]
    fn or_uses_inclusion_exclusion() {
        let lookup = map_lookup([
            (
                "a",
                ColumnStatistics {
                    ndv: Some(10),
                    ..ColumnStatistics::default()
                },
            ),
            (
                "b",
                ColumnStatistics {
                    ndv: Some(5),
                    ..ColumnStatistics::default()
                },
            ),
        ]);
        let predicate = bin(
            bin(col("a"), BinaryOp::Eq, lit_i64(1)),
            BinaryOp::Or,
            bin(col("b"), BinaryOp::Eq, lit_i64(1)),
        );
        // sel(a=1) = 0.1, sel(b=1) = 0.2 → or = 0.1 + 0.2 - 0.02 = 0.28
        assert!((selectivity(&predicate, &lookup) - 0.28).abs() < 1e-9);
    }

    #[test]
    fn not_is_complement() {
        let lookup = map_lookup([(
            "a",
            ColumnStatistics {
                ndv: Some(4),
                ..ColumnStatistics::default()
            },
        )]);
        let inner = bin(col("a"), BinaryOp::Eq, lit_i64(1));
        let negated = not(inner.clone());
        let s = selectivity(&inner, &lookup);
        let n = selectivity(&negated, &lookup);
        assert!((s + n - 1.0).abs() < 1e-9);
    }

    // -- Fallback / unknown --

    #[test]
    fn unknown_expression_uses_default() {
        let predicate = PlanExpr::Function {
            name: "weird_udf".to_string(),
            args: vec![col("c")],
            distinct: false,
            span: None,
        };
        assert_eq!(
            selectivity(&predicate, &empty()),
            DEFAULT_UNKNOWN_SELECTIVITY
        );
    }

    #[test]
    fn result_is_always_in_zero_one_for_many_predicates() {
        // Exhaustive sanity sweep over hand-picked combinations.
        let lookups: Vec<&dyn ColumnStatsLookup> = vec![&EmptyLookup];
        let predicates: Vec<PlanExpr> = vec![
            bin(col("a"), BinaryOp::Eq, lit_i64(1)),
            bin(col("a"), BinaryOp::NotEq, lit_i64(1)),
            bin(col("a"), BinaryOp::Lt, lit_i64(100)),
            bin(col("a"), BinaryOp::GtEq, lit_i64(0)),
            PlanExpr::Between {
                expr: Box::new(col("a")),
                negated: false,
                low: Box::new(lit_i64(0)),
                high: Box::new(lit_i64(100)),
                span: None,
            },
            PlanExpr::InList {
                expr: Box::new(col("a")),
                list: vec![lit_i64(1), lit_i64(2), lit_i64(3)],
                negated: false,
                span: None,
            },
            PlanExpr::IsNull {
                expr: Box::new(col("a")),
                span: None,
            },
            bin(
                bin(col("a"), BinaryOp::Eq, lit_i64(1)),
                BinaryOp::And,
                bin(col("b"), BinaryOp::Lt, lit_i64(10)),
            ),
            bin(
                bin(col("a"), BinaryOp::Eq, lit_i64(1)),
                BinaryOp::Or,
                bin(col("b"), BinaryOp::Lt, lit_i64(10)),
            ),
            not(bin(col("a"), BinaryOp::Eq, lit_i64(1))),
        ];
        for lookup in lookups {
            for p in &predicates {
                let s = selectivity(p, lookup);
                assert!((0.0..=1.0).contains(&s), "out of [0,1]: {s} for {p:?}");
            }
        }
    }
}
