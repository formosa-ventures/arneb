//! `PlanExpr` filter → remote SQL, conservatively.
//!
//! Each top-level `AND` conjunct is translated on its own and either
//! translates completely or not at all. A conjunct is pushed only when the
//! remote database is certain to evaluate it exactly as Arneb would:
//!
//! - comparisons between a column and a literal (or two columns) of
//!   compatible types — no implicit casts that could change semantics;
//! - no NULL literals (Arneb keeps three-valued `= NULL` semantics locally);
//! - no range comparisons on text (remote collation order can differ from
//!   Arneb's byte order) — text equality and `IN` are fine;
//! - no comparisons on floating-point columns (remote NaN ordering can
//!   differ: PostgreSQL sorts NaN above every number, so `NOT (x > 1)`
//!   would drop NaN rows Arneb keeps);
//! - no columns the dialect refuses ([`Dialect::pushable_column`]);
//! - `IS [NOT] NULL`, `[NOT] IN`, `[NOT] BETWEEN`, `[NOT] LIKE`, `AND`, `OR`,
//!   `NOT` over translatable operands.

use arneb_common::types::{ColumnInfo, DataType, ScalarValue};
use arneb_planner::PlanExpr;
use arneb_sql_parser::ast::{BinaryOp, UnaryOp};

use crate::{Dialect, RemoteTable};

/// The translatable part of a scan's filters.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Translation {
    /// Translated conjuncts, to be `AND`ed into the remote `WHERE`.
    pub conjuncts: Vec<String>,
    /// `true` when every conjunct was translated exactly — the remote query
    /// then returns exactly the rows Arneb's filter would keep, so a `LIMIT`
    /// may be pushed.
    pub complete: bool,
    /// Dynamic-filter conjuncts pushed as they are.
    pub dynamic_pushed: usize,
    /// Dynamic `IN` lists over the compaction threshold, pushed as a
    /// `BETWEEN min AND max` range.
    pub dynamic_compacted: usize,
    /// Dynamic-filter conjuncts not pushed at all.
    pub dynamic_skipped: usize,
}

/// Default for [`TranslateOptions::compaction_threshold`].
pub const DEFAULT_DOMAIN_COMPACTION_THRESHOLD: usize = 1000;

/// How [`translate_filters_with`] treats runtime dynamic filters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TranslateOptions {
    /// Index of the first dynamic-filter entry in `filters`
    /// (`ScanContext::dynamic_filters_start`); `None` = all static.
    pub dynamic_from: Option<usize>,
    /// A dynamic `IN` list with more literals than this is compacted to a
    /// range. Static lists are never compacted.
    pub compaction_threshold: usize,
}

impl Default for TranslateOptions {
    fn default() -> Self {
        Self {
            dynamic_from: None,
            compaction_threshold: DEFAULT_DOMAIN_COMPACTION_THRESHOLD,
        }
    }
}

/// Translate the scan's pushed-down filters, treating every filter as
/// static.
///
/// `schema` is the Arneb table schema the filters' column indices refer to;
/// `table` gives each column's remote name (same order).
pub fn translate_filters(
    filters: &[PlanExpr],
    schema: &[ColumnInfo],
    table: &RemoteTable,
    dialect: &dyn Dialect,
) -> Translation {
    translate_filters_with(filters, schema, table, dialect, TranslateOptions::default())
}

/// Translate the scan's pushed-down filters; entries from
/// `options.dynamic_from` on are runtime dynamic filters.
///
/// A dynamic filter only prunes the probe side of the join that produced it
/// — the join still checks every key — so pushing a weaker range in place
/// of a long `IN` list, or nothing at all, never changes a result.
pub fn translate_filters_with(
    filters: &[PlanExpr],
    schema: &[ColumnInfo],
    table: &RemoteTable,
    dialect: &dyn Dialect,
    options: TranslateOptions,
) -> Translation {
    let ctx = Ctx {
        schema,
        table,
        dialect,
    };
    let mut t = Translation {
        complete: true,
        ..Default::default()
    };
    for (i, filter) in filters.iter().enumerate() {
        let dynamic = options.dynamic_from.is_some_and(|from| i >= from);
        let mut parts = Vec::new();
        flatten_and(filter, &mut parts);
        for part in parts {
            if !dynamic {
                match ctx.predicate(part) {
                    Some(sql) => t.conjuncts.push(sql),
                    None => t.complete = false,
                }
                continue;
            }
            match compact(part, options.compaction_threshold) {
                Compaction::Unchanged => match ctx.predicate(part) {
                    Some(sql) => {
                        t.conjuncts.push(sql);
                        t.dynamic_pushed += 1;
                    }
                    None => {
                        t.complete = false;
                        t.dynamic_skipped += 1;
                    }
                },
                Compaction::Range(range) => {
                    // Weaker than the list: Arneb must still filter.
                    t.complete = false;
                    match ctx.predicate(&range) {
                        Some(sql) => {
                            t.conjuncts.push(sql);
                            t.dynamic_compacted += 1;
                        }
                        None => t.dynamic_skipped += 1,
                    }
                }
                Compaction::Drop => {
                    t.complete = false;
                    t.dynamic_skipped += 1;
                }
            }
        }
    }
    t
}

enum Compaction {
    Unchanged,
    Range(PlanExpr),
    Drop,
}

/// An `IN` list over `threshold` literals becomes `BETWEEN min AND max` when
/// all literals share one orderable type, else it is dropped.
fn compact(expr: &PlanExpr, threshold: usize) -> Compaction {
    let PlanExpr::InList {
        expr,
        list,
        negated: false,
        span,
    } = expr
    else {
        return Compaction::Unchanged;
    };
    if list.len() <= threshold {
        return Compaction::Unchanged;
    }
    let mut bounds: Option<(&ScalarValue, &ScalarValue)> = None;
    for item in list {
        let PlanExpr::Literal { value, .. } = item else {
            return Compaction::Drop;
        };
        bounds = match bounds {
            None if orderable(value) => Some((value, value)),
            None => return Compaction::Drop,
            Some((lo, hi)) => match (order(value, lo), order(value, hi)) {
                (Some(l), Some(h)) => Some((
                    if l.is_lt() { value } else { lo },
                    if h.is_gt() { value } else { hi },
                )),
                _ => return Compaction::Drop,
            },
        };
    }
    let Some((lo, hi)) = bounds else {
        return Compaction::Drop;
    };
    let bound = |value: &ScalarValue| {
        Box::new(PlanExpr::Literal {
            value: value.clone(),
            span: None,
        })
    };
    Compaction::Range(PlanExpr::Between {
        expr: expr.clone(),
        negated: false,
        low: bound(lo),
        high: bound(hi),
        span: *span,
    })
}

fn orderable(v: &ScalarValue) -> bool {
    order(v, v).is_some()
}

/// Order two literals of the same orderable type; `None` across types.
fn order(a: &ScalarValue, b: &ScalarValue) -> Option<std::cmp::Ordering> {
    use ScalarValue as S;
    match (a, b) {
        (S::Int32(x), S::Int32(y)) => Some(x.cmp(y)),
        (S::Int64(x), S::Int64(y)) => Some(x.cmp(y)),
        (S::Date32(x), S::Date32(y)) => Some(x.cmp(y)),
        (
            S::Decimal128 {
                value: x,
                scale: sx,
                ..
            },
            S::Decimal128 {
                value: y,
                scale: sy,
                ..
            },
        ) if sx == sy => Some(x.cmp(y)),
        _ => None,
    }
}

fn flatten_and<'a>(expr: &'a PlanExpr, out: &mut Vec<&'a PlanExpr>) {
    match expr {
        PlanExpr::BinaryOp {
            left,
            op: BinaryOp::And,
            right,
            ..
        } => {
            flatten_and(left, out);
            flatten_and(right, out);
        }
        other => out.push(other),
    }
}

/// A translated operand and the Arneb type it has.
struct Operand {
    sql: String,
    data_type: DataType,
    is_column: bool,
}

struct Ctx<'a> {
    schema: &'a [ColumnInfo],
    table: &'a RemoteTable,
    dialect: &'a dyn Dialect,
}

impl Ctx<'_> {
    /// A boolean-valued expression.
    fn predicate(&self, expr: &PlanExpr) -> Option<String> {
        match expr {
            PlanExpr::BinaryOp {
                left,
                op: op @ (BinaryOp::And | BinaryOp::Or),
                right,
                ..
            } => {
                let l = self.predicate(left)?;
                let r = self.predicate(right)?;
                let word = if *op == BinaryOp::And { "AND" } else { "OR" };
                Some(format!("({l} {word} {r})"))
            }
            PlanExpr::UnaryOp {
                op: UnaryOp::Not,
                expr,
                ..
            } => Some(format!("(NOT {})", self.predicate(expr)?)),
            PlanExpr::BinaryOp {
                left, op, right, ..
            } => match op {
                BinaryOp::Like | BinaryOp::NotLike => {
                    let col = self.column(left)?;
                    let pattern = self.literal(right)?;
                    if col.data_type != DataType::Utf8 || pattern.data_type != DataType::Utf8 {
                        return None;
                    }
                    let not = if *op == BinaryOp::NotLike { "NOT " } else { "" };
                    Some(format!("({} {not}LIKE {})", col.sql, pattern.sql))
                }
                _ => {
                    let sym = comparison_symbol(*op)?;
                    let l = self.operand(left)?;
                    let r = self.operand(right)?;
                    if !l.is_column && !r.is_column {
                        return None;
                    }
                    let range = !matches!(op, BinaryOp::Eq | BinaryOp::NotEq);
                    if !comparable(&l, &r, range) {
                        return None;
                    }
                    Some(format!("({} {sym} {})", l.sql, r.sql))
                }
            },
            PlanExpr::IsNull { expr, .. } => Some(format!("({} IS NULL)", self.column(expr)?.sql)),
            PlanExpr::IsNotNull { expr, .. } => {
                Some(format!("({} IS NOT NULL)", self.column(expr)?.sql))
            }
            PlanExpr::InList {
                expr,
                list,
                negated,
                ..
            } => {
                if list.is_empty() {
                    return None;
                }
                let col = self.column(expr)?;
                let mut items = Vec::with_capacity(list.len());
                for item in list {
                    let lit = self.literal(item)?;
                    if !comparable(&col, &lit, false) {
                        return None;
                    }
                    items.push(lit.sql);
                }
                let not = if *negated { "NOT " } else { "" };
                Some(format!("({} {not}IN ({}))", col.sql, items.join(", ")))
            }
            PlanExpr::Between {
                expr,
                negated,
                low,
                high,
                ..
            } => {
                let col = self.column(expr)?;
                let lo = self.literal(low)?;
                let hi = self.literal(high)?;
                if !comparable(&col, &lo, true) || !comparable(&col, &hi, true) {
                    return None;
                }
                let not = if *negated { "NOT " } else { "" };
                Some(format!(
                    "({} {not}BETWEEN {} AND {})",
                    col.sql, lo.sql, hi.sql
                ))
            }
            _ => None,
        }
    }

    fn operand(&self, expr: &PlanExpr) -> Option<Operand> {
        self.column(expr).or_else(|| self.literal(expr))
    }

    fn column(&self, expr: &PlanExpr) -> Option<Operand> {
        // A lossless widening cast of a column (the analyzer's coercion of
        // `int4_col IN (1, 3)` to `CAST(int4_col AS BIGINT) IN …`) compares
        // the same values remotely without it.
        if let PlanExpr::Cast {
            expr: inner,
            data_type,
            ..
        } = expr
        {
            let col = self.column(inner)?;
            if !widens_losslessly(&col.data_type, data_type) {
                return None;
            }
            return Some(Operand {
                data_type: data_type.clone(),
                ..col
            });
        }
        let PlanExpr::Column { index, .. } = expr else {
            return None;
        };
        let info = self.schema.get(*index)?;
        let remote = self.table.columns.get(*index)?;
        if !self.dialect.pushable_column(remote) {
            return None;
        }
        Some(Operand {
            sql: self.dialect.quote_identifier(&remote.name),
            data_type: info.data_type.clone(),
            is_column: true,
        })
    }

    fn literal(&self, expr: &PlanExpr) -> Option<Operand> {
        let PlanExpr::Literal { value, .. } = expr else {
            return None;
        };
        if matches!(value, ScalarValue::Null) {
            return None;
        }
        Some(Operand {
            sql: self.dialect.render_literal(value)?,
            data_type: value.data_type(),
            is_column: false,
        })
    }
}

/// Whether every value of `from` converts to `to` exactly (and so compares
/// the same with or without the cast).
fn widens_losslessly(from: &DataType, to: &DataType) -> bool {
    /// Decimal digits needed for any value of an integer type.
    fn int_digits(t: &DataType) -> Option<i32> {
        Some(match t {
            DataType::Int8 => 3,
            DataType::Int16 => 5,
            DataType::Int32 => 10,
            DataType::Int64 => 19,
            _ => return None,
        })
    }
    fn decimal(t: &DataType) -> Option<(i32, i32)> {
        match t {
            DataType::Decimal128 { precision, scale }
            | DataType::Decimal64 { precision, scale } => Some((*precision as i32, *scale as i32)),
            _ => None,
        }
    }
    if from == to {
        return true;
    }
    if let (Some(a), Some(b)) = (int_digits(from), int_digits(to)) {
        return b >= a;
    }
    if let (Some(digits), Some((p, s))) = (int_digits(from), decimal(to)) {
        return s >= 0 && p - s >= digits;
    }
    if let (Some((p1, s1)), Some((p2, s2))) = (decimal(from), decimal(to)) {
        return s1 >= 0 && s2 >= s1 && p2 - s2 >= p1 - s1;
    }
    false
}

fn comparison_symbol(op: BinaryOp) -> Option<&'static str> {
    Some(match op {
        BinaryOp::Eq => "=",
        BinaryOp::NotEq => "<>",
        BinaryOp::Lt => "<",
        BinaryOp::LtEq => "<=",
        BinaryOp::Gt => ">",
        BinaryOp::GtEq => ">=",
        _ => return None,
    })
}

enum Family {
    Exact,
    Text,
    Boolean,
    Date,
    Timestamp(bool),
}

fn family(t: &DataType) -> Option<Family> {
    Some(match t {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Decimal128 { .. }
        | DataType::Decimal64 { .. } => Family::Exact,
        DataType::Utf8 | DataType::LargeUtf8 => Family::Text,
        DataType::Boolean => Family::Boolean,
        DataType::Date32 => Family::Date,
        DataType::Timestamp { timezone, .. } => Family::Timestamp(timezone.is_some()),
        _ => return None,
    })
}

/// Whether comparing `a` and `b` remotely gives Arneb's answer.
///
/// Integers and decimals compare exactly with each other on both sides;
/// text, booleans, dates and timestamps (with matching time-zone-ness) only
/// with themselves. Floats never (see the module docs).
fn comparable(a: &Operand, b: &Operand, range: bool) -> bool {
    let (Some(fa), Some(fb)) = (family(&a.data_type), family(&b.data_type)) else {
        return false;
    };
    match (fa, fb) {
        (Family::Exact, Family::Exact) => true,
        (Family::Text, Family::Text) => !range,
        (Family::Boolean, Family::Boolean) => !range,
        (Family::Date, Family::Date) => true,
        (Family::Timestamp(x), Family::Timestamp(y)) => x == y,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::{col, lit, nation, TestDialect};

    fn bin(l: PlanExpr, op: BinaryOp, r: PlanExpr) -> PlanExpr {
        PlanExpr::BinaryOp {
            left: Box::new(l),
            op,
            right: Box::new(r),
            span: None,
        }
    }

    fn translate(filters: &[PlanExpr]) -> Translation {
        let (schema, table) = nation();
        translate_filters(filters, &schema, &table, &TestDialect)
    }

    fn func(name: &str, args: Vec<PlanExpr>) -> PlanExpr {
        PlanExpr::Function {
            name: name.into(),
            args,
            distinct: false,
            span: None,
        }
    }

    #[test]
    fn supported_conjuncts_are_pushed() {
        let f = bin(
            bin(col(2), BinaryOp::Eq, lit(ScalarValue::Int32(1))),
            BinaryOp::And,
            bin(col(1), BinaryOp::Like, lit(ScalarValue::Utf8("A%".into()))),
        );
        let t = translate(&[f]);
        assert!(t.complete);
        assert_eq!(
            t.conjuncts,
            vec![
                "(\"n_regionkey\" = 1)".to_string(),
                "(\"n_name\" LIKE 'A%')".to_string()
            ]
        );
    }

    #[test]
    fn unsupported_conjunct_is_kept_local() {
        let f = bin(
            bin(
                func("upper", vec![col(1)]),
                BinaryOp::Eq,
                lit(ScalarValue::Utf8("CHINA".into())),
            ),
            BinaryOp::And,
            bin(col(2), BinaryOp::Eq, lit(ScalarValue::Int64(2))),
        );
        let t = translate(&[f]);
        assert!(!t.complete);
        assert_eq!(t.conjuncts, vec!["(\"n_regionkey\" = 2)".to_string()]);
    }

    #[test]
    fn or_with_untranslatable_branch_is_not_pushed() {
        let f = bin(
            bin(col(2), BinaryOp::Eq, lit(ScalarValue::Int32(1))),
            BinaryOp::Or,
            bin(
                func("upper", vec![col(1)]),
                BinaryOp::Eq,
                lit(ScalarValue::Utf8("CHINA".into())),
            ),
        );
        let t = translate(&[f]);
        assert!(!t.complete);
        assert!(t.conjuncts.is_empty());
    }

    #[test]
    fn or_and_not_of_translatable_parts_are_pushed() {
        let f = PlanExpr::UnaryOp {
            op: UnaryOp::Not,
            expr: Box::new(bin(
                bin(col(0), BinaryOp::Lt, lit(ScalarValue::Int32(3))),
                BinaryOp::Or,
                PlanExpr::IsNull {
                    expr: Box::new(col(3)),
                    span: None,
                },
            )),
            span: None,
        };
        let t = translate(&[f]);
        assert!(t.complete);
        assert_eq!(
            t.conjuncts,
            vec!["(NOT ((\"n_nationkey\" < 3) OR (\"n_comment\" IS NULL)))".to_string()]
        );
    }

    #[test]
    fn null_literal_is_not_pushed() {
        let t = translate(&[bin(col(2), BinaryOp::Eq, lit(ScalarValue::Null))]);
        assert!(!t.complete);
        assert!(t.conjuncts.is_empty());
    }

    #[test]
    fn text_range_is_not_pushed_but_text_equality_is() {
        let range = bin(col(1), BinaryOp::Lt, lit(ScalarValue::Utf8("C".into())));
        assert!(translate(&[range]).conjuncts.is_empty());
        let eq = bin(col(1), BinaryOp::NotEq, lit(ScalarValue::Utf8("C".into())));
        assert_eq!(
            translate(&[eq]).conjuncts,
            vec!["(\"n_name\" <> 'C')".to_string()]
        );
    }

    #[test]
    fn mismatched_types_are_not_pushed() {
        // Text column vs integer literal: the remote would cast implicitly.
        let t = translate(&[bin(col(1), BinaryOp::Eq, lit(ScalarValue::Int32(1)))]);
        assert!(t.conjuncts.is_empty());
        // Float literal vs decimal column.
        let t = translate(&[bin(col(4), BinaryOp::Gt, lit(ScalarValue::Float64(1.5)))]);
        assert!(t.conjuncts.is_empty());
        // Decimal literal vs decimal column is exact.
        let d = ScalarValue::Decimal128 {
            value: 150,
            precision: 15,
            scale: 2,
        };
        let t = translate(&[bin(col(4), BinaryOp::Gt, lit(d))]);
        assert_eq!(t.conjuncts, vec!["(\"n_balance\" > 1.50)".to_string()]);
    }

    #[test]
    fn float_and_refused_columns_are_not_pushed() {
        let t = translate(&[bin(col(6), BinaryOp::Gt, lit(ScalarValue::Int32(1)))]);
        assert!(t.conjuncts.is_empty());
        let t = translate(&[bin(
            col(5),
            BinaryOp::NotEq,
            lit(ScalarValue::Utf8("R".into())),
        )]);
        assert!(t.conjuncts.is_empty());
        let t = translate(&[PlanExpr::IsNull {
            expr: Box::new(col(5)),
            span: None,
        }]);
        assert!(t.conjuncts.is_empty());
    }

    #[test]
    fn literal_first_comparison_and_column_pairs() {
        let t = translate(&[bin(lit(ScalarValue::Int32(5)), BinaryOp::GtEq, col(0))]);
        assert_eq!(t.conjuncts, vec!["(5 >= \"n_nationkey\")".to_string()]);
        let t = translate(&[bin(col(0), BinaryOp::Eq, col(2))]);
        assert_eq!(
            t.conjuncts,
            vec!["(\"n_nationkey\" = \"n_regionkey\")".to_string()]
        );
        // Two literals: nothing to push.
        let t = translate(&[bin(
            lit(ScalarValue::Int32(1)),
            BinaryOp::Eq,
            lit(ScalarValue::Int32(1)),
        )]);
        assert!(t.conjuncts.is_empty());
    }

    #[test]
    fn in_list_and_between() {
        let in_list = PlanExpr::InList {
            expr: Box::new(col(0)),
            list: vec![lit(ScalarValue::Int32(1)), lit(ScalarValue::Int64(7))],
            negated: true,
            span: None,
        };
        let between = PlanExpr::Between {
            expr: Box::new(col(0)),
            negated: false,
            low: Box::new(lit(ScalarValue::Int32(1))),
            high: Box::new(lit(ScalarValue::Int32(9))),
            span: None,
        };
        let t = translate(&[in_list, between]);
        assert!(t.complete);
        assert_eq!(
            t.conjuncts,
            vec![
                "(\"n_nationkey\" NOT IN (1, 7))".to_string(),
                "(\"n_nationkey\" BETWEEN 1 AND 9)".to_string()
            ]
        );
    }

    #[test]
    fn in_list_with_null_or_empty_is_not_pushed() {
        let with_null = PlanExpr::InList {
            expr: Box::new(col(0)),
            list: vec![lit(ScalarValue::Int32(1)), lit(ScalarValue::Null)],
            negated: true,
            span: None,
        };
        let empty = PlanExpr::InList {
            expr: Box::new(col(0)),
            list: vec![],
            negated: false,
            span: None,
        };
        assert!(translate(&[with_null, empty]).conjuncts.is_empty());
    }

    fn cast(e: PlanExpr, data_type: DataType) -> PlanExpr {
        PlanExpr::Cast {
            expr: Box::new(e),
            data_type,
            span: None,
        }
    }

    #[test]
    fn lossless_widening_casts_of_columns_are_seen_through() {
        // int4 → int8 (the analyzer's coercion for IN lists).
        let t = translate(&[PlanExpr::InList {
            expr: Box::new(cast(col(2), DataType::Int64)),
            list: vec![lit(ScalarValue::Int64(1)), lit(ScalarValue::Int64(3))],
            negated: false,
            span: None,
        }]);
        assert_eq!(t.conjuncts, vec!["(\"n_regionkey\" IN (1, 3))".to_string()]);
        // decimal(15,2) → decimal(21,2).
        let wide = DataType::Decimal128 {
            precision: 21,
            scale: 2,
        };
        let five = ScalarValue::Decimal128 {
            value: 500,
            precision: 21,
            scale: 2,
        };
        let t = translate(&[bin(cast(col(4), wide), BinaryOp::Gt, lit(five))]);
        assert_eq!(t.conjuncts, vec!["(\"n_balance\" > 5.00)".to_string()]);
    }

    #[test]
    fn narrowing_or_other_casts_and_unknown_columns_are_not_pushed() {
        let narrow = DataType::Decimal128 {
            precision: 5,
            scale: 0,
        };
        let t = translate(&[
            // int4 → decimal(5,0) can overflow.
            bin(
                cast(col(0), narrow),
                BinaryOp::Eq,
                lit(ScalarValue::Int64(1)),
            ),
            // decimal(15,2) → int8 truncates.
            bin(
                cast(col(4), DataType::Int64),
                BinaryOp::Eq,
                lit(ScalarValue::Int64(1)),
            ),
            // int4 → text changes comparison semantics.
            bin(
                cast(col(0), DataType::Utf8),
                BinaryOp::Eq,
                lit(ScalarValue::Utf8("1".into())),
            ),
            bin(col(99), BinaryOp::Eq, lit(ScalarValue::Int64(1))),
        ]);
        assert!(t.conjuncts.is_empty());
        assert!(!t.complete);
    }

    #[test]
    fn widening_rules() {
        let d = |precision, scale| DataType::Decimal128 { precision, scale };
        assert!(widens_losslessly(&DataType::Int16, &DataType::Int64));
        assert!(!widens_losslessly(&DataType::Int64, &DataType::Int32));
        assert!(widens_losslessly(&DataType::Int32, &d(12, 2)));
        assert!(!widens_losslessly(&DataType::Int32, &d(11, 2)));
        assert!(widens_losslessly(&d(15, 2), &d(21, 4)));
        assert!(!widens_losslessly(&d(15, 2), &d(15, 3)));
        assert!(!widens_losslessly(&d(15, 2), &d(21, 1)));
        assert!(!widens_losslessly(&DataType::Int32, &DataType::Float64));
    }

    #[test]
    fn unrenderable_literal_is_not_pushed() {
        // TestDialect refuses strings with a backslash.
        let t = translate(&[bin(
            col(1),
            BinaryOp::Eq,
            lit(ScalarValue::Utf8("a\\b".into())),
        )]);
        assert!(t.conjuncts.is_empty());
    }

    #[test]
    fn no_filters_is_complete() {
        let t = translate(&[]);
        assert!(t.complete);
        assert!(t.conjuncts.is_empty());
    }

    fn in_list(index: usize, values: Vec<ScalarValue>) -> PlanExpr {
        PlanExpr::InList {
            expr: Box::new(col(index)),
            list: values.into_iter().map(lit).collect(),
            negated: false,
            span: None,
        }
    }

    fn translate_dynamic(
        filters: &[PlanExpr],
        dynamic_from: usize,
        threshold: usize,
    ) -> Translation {
        let (schema, table) = nation();
        translate_filters_with(
            filters,
            &schema,
            &table,
            &TestDialect,
            TranslateOptions {
                dynamic_from: Some(dynamic_from),
                compaction_threshold: threshold,
            },
        )
    }

    fn ints(values: impl IntoIterator<Item = i32>) -> Vec<ScalarValue> {
        values.into_iter().map(ScalarValue::Int32).collect()
    }

    #[test]
    fn small_dynamic_in_list_is_pushed_exactly() {
        let t = translate_dynamic(&[in_list(0, ints([3, 1, 2]))], 0, 3);
        assert_eq!(
            t.conjuncts,
            vec!["(\"n_nationkey\" IN (3, 1, 2))".to_string()]
        );
        assert!(t.complete);
        assert_eq!(
            (t.dynamic_pushed, t.dynamic_compacted, t.dynamic_skipped),
            (1, 0, 0)
        );
    }

    #[test]
    fn large_dynamic_in_list_is_compacted_to_a_range() {
        let t = translate_dynamic(&[in_list(0, ints([40, 7, 9000, 12]))], 0, 3);
        assert_eq!(
            t.conjuncts,
            vec!["(\"n_nationkey\" BETWEEN 7 AND 9000)".to_string()]
        );
        assert!(!t.complete, "a range returns a superset: no LIMIT");
        assert_eq!(
            (t.dynamic_pushed, t.dynamic_compacted, t.dynamic_skipped),
            (0, 1, 0)
        );
    }

    #[test]
    fn decimal_list_compacts_within_one_scale() {
        let d = |value| ScalarValue::Decimal128 {
            value,
            precision: 15,
            scale: 2,
        };
        let t = translate_dynamic(&[in_list(4, vec![d(500), d(-125), d(99)])], 0, 2);
        assert_eq!(
            t.conjuncts,
            vec!["(\"n_balance\" BETWEEN -1.25 AND 5.00)".to_string()]
        );
    }

    #[test]
    fn large_list_without_one_orderable_type_is_skipped() {
        let mixed = vec![
            ScalarValue::Int32(1),
            ScalarValue::Int64(2),
            ScalarValue::Int32(3),
        ];
        let text = ["a", "b", "c"]
            .map(|v| ScalarValue::Utf8(v.into()))
            .to_vec();
        let t = translate_dynamic(&[in_list(0, mixed), in_list(1, text)], 0, 2);
        assert!(t.conjuncts.is_empty());
        assert!(!t.complete);
        assert_eq!(t.dynamic_skipped, 2);
    }

    #[test]
    fn static_in_list_is_never_compacted() {
        let static_list = in_list(0, ints(0..20));
        let dynamic = in_list(2, ints(0..20));
        let t = translate_dynamic(&[static_list, dynamic], 1, 5);
        assert_eq!(t.conjuncts.len(), 2);
        assert!(t.conjuncts[0].contains("IN (0, 1, 2"), "{}", t.conjuncts[0]);
        assert_eq!(t.conjuncts[1], "(\"n_regionkey\" BETWEEN 0 AND 19)");
        assert_eq!(
            (t.dynamic_pushed, t.dynamic_compacted, t.dynamic_skipped),
            (0, 1, 0)
        );
    }

    #[test]
    fn untranslatable_dynamic_filter_is_counted_as_skipped() {
        // A float column is never pushed, compacted or not.
        let floats = vec![ScalarValue::Float64(1.0)];
        let t = translate_dynamic(&[in_list(6, floats)], 0, 10);
        assert!(t.conjuncts.is_empty());
        assert_eq!(t.dynamic_skipped, 1);
    }

    #[test]
    fn static_only_options_match_translate_filters() {
        let f = [in_list(0, ints(0..5000))];
        let (schema, table) = nation();
        assert_eq!(
            translate_filters_with(
                &f,
                &schema,
                &table,
                &TestDialect,
                TranslateOptions::default()
            ),
            translate(&f)
        );
    }
}
