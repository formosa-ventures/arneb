//! File-level pruning from manifest metadata.
//!
//! Every data file in an Iceberg manifest carries per-column lower/upper
//! bounds and its partition tuple. Before opening a file, the scan checks
//! whether the pushed-down filters can match *any* row within those
//! bounds; if not, the file is skipped without a single object-store
//! request.
//!
//! The evaluator is deliberately conservative: anything it does not fully
//! understand (unsupported types, casts, OR, float columns where NaN
//! ordering matters, mismatched literal types) keeps the file. The filters
//! are always re-applied above the scan, so pruning only has to be sound,
//! never complete.

use std::cmp::Ordering;
use std::collections::HashMap;

use apache_avro::types::Value;

use arneb_common::types::{ScalarValue, TimeUnit};
use arneb_planner::PlanExpr;
use arneb_sql_parser::ast::BinaryOp;

use crate::manifest::DataFile;
use crate::metadata::{IcebergType, PartitionSpec};

/// A comparable value decoded from a bound, a partition value, or a
/// filter literal. Integers, dates, timestamps, and decimal unscaled
/// values are all carried as `i128` in the column's own unit/scale.
#[derive(Debug, Clone, PartialEq)]
pub enum Datum {
    /// Integer-like value (int, long, date, timestamp, unscaled decimal).
    Num(i128),
    /// UTF-8 string (Iceberg orders strings by UTF-8 bytes, like `str`).
    Str(String),
}

impl Datum {
    fn cmp(&self, other: &Datum) -> Option<Ordering> {
        match (self, other) {
            (Datum::Num(a), Datum::Num(b)) => Some(a.cmp(b)),
            (Datum::Str(a), Datum::Str(b)) => Some(a.as_bytes().cmp(b.as_bytes())),
            _ => None,
        }
    }
}

/// Decode an Iceberg single-value-serialized bound for a column of type
/// `ty`. Handles `int -> long` promotion (4-byte bounds on a long column).
pub fn decode_bound(ty: &IcebergType, bytes: &[u8]) -> Option<Datum> {
    let le_int = |b: &[u8]| -> Option<i128> {
        match b.len() {
            4 => Some(i32::from_le_bytes(b.try_into().ok()?) as i128),
            8 => Some(i64::from_le_bytes(b.try_into().ok()?) as i128),
            _ => None,
        }
    };
    match ty {
        IcebergType::Int
        | IcebergType::Long
        | IcebergType::Date
        | IcebergType::Timestamp { .. }
        | IcebergType::TimestampTz { .. } => le_int(bytes).map(Datum::Num),
        IcebergType::Decimal { .. } => decode_be_signed(bytes).map(Datum::Num),
        IcebergType::String => String::from_utf8(bytes.to_vec()).ok().map(Datum::Str),
        _ => None,
    }
}

/// Big-endian two's-complement (decimal unscaled value) → i128.
fn decode_be_signed(bytes: &[u8]) -> Option<i128> {
    if bytes.is_empty() || bytes.len() > 16 {
        return None;
    }
    let negative = bytes[0] & 0x80 != 0;
    let mut buf = if negative { [0xffu8; 16] } else { [0u8; 16] };
    buf[16 - bytes.len()..].copy_from_slice(bytes);
    Some(i128::from_be_bytes(buf))
}

/// Convert an identity-partition Avro value into a [`Datum`].
fn partition_datum(ty: &IcebergType, v: &Value) -> Option<Datum> {
    match (ty, v) {
        (_, Value::Int(i)) | (_, Value::Date(i)) => Some(Datum::Num(*i as i128)),
        (_, Value::Long(l))
        | (_, Value::TimestampMicros(l))
        | (_, Value::LocalTimestampMicros(l))
        | (_, Value::TimestampNanos(l))
        | (_, Value::LocalTimestampNanos(l)) => Some(Datum::Num(*l as i128)),
        (IcebergType::String, Value::String(s)) => Some(Datum::Str(s.clone())),
        (IcebergType::Decimal { .. }, Value::Bytes(b))
        | (IcebergType::Decimal { .. }, Value::Fixed(_, b)) => decode_be_signed(b).map(Datum::Num),
        (IcebergType::Decimal { .. }, Value::Decimal(d)) => Vec::<u8>::try_from(d)
            .ok()
            .and_then(|b| decode_be_signed(&b))
            .map(Datum::Num),
        _ => None,
    }
}

/// Convert a filter literal to a [`Datum`] in the column's unit/scale.
/// Returns `None` whenever the conversion is not exact.
fn literal_datum(ty: &IcebergType, lit: &ScalarValue) -> Option<Datum> {
    match (ty, lit) {
        (IcebergType::Int | IcebergType::Long, ScalarValue::Int32(v)) => {
            Some(Datum::Num(*v as i128))
        }
        (IcebergType::Int | IcebergType::Long, ScalarValue::Int64(v)) => {
            Some(Datum::Num(*v as i128))
        }
        (IcebergType::Date, ScalarValue::Date32(v)) => Some(Datum::Num(*v as i128)),
        (
            IcebergType::Timestamp { nanos } | IcebergType::TimestampTz { nanos },
            ScalarValue::Timestamp {
                value,
                unit,
                timezone,
            },
        ) => {
            // Only compare zoned with zoned and naive with naive; mixing
            // them depends on the session time zone.
            if timezone.is_some() != matches!(ty, IcebergType::TimestampTz { .. }) {
                return None;
            }
            let v = *value as i128;
            let col_nanos = *nanos;
            let v = match (unit, col_nanos) {
                (TimeUnit::Microsecond, false) | (TimeUnit::Nanosecond, true) => v,
                (TimeUnit::Microsecond, true) => v * 1_000,
                (TimeUnit::Millisecond, false) => v * 1_000,
                (TimeUnit::Millisecond, true) => v * 1_000_000,
                (TimeUnit::Second, false) => v * 1_000_000,
                (TimeUnit::Second, true) => v * 1_000_000_000,
                (TimeUnit::Nanosecond, false) => return None,
            };
            Some(Datum::Num(v))
        }
        (
            IcebergType::Decimal { scale, .. },
            ScalarValue::Decimal128 {
                value, scale: ls, ..
            },
        ) => {
            let diff = *scale as i32 - *ls as i32;
            if !(0..=38).contains(&diff) {
                return None;
            }
            value
                .checked_mul(10i128.checked_pow(diff as u32)?)
                .map(Datum::Num)
        }
        (IcebergType::Decimal { scale, .. }, ScalarValue::Int32(_) | ScalarValue::Int64(_)) => {
            let v = match lit {
                ScalarValue::Int32(v) => *v as i128,
                ScalarValue::Int64(v) => *v as i128,
                _ => unreachable!(),
            };
            if *scale < 0 {
                return None;
            }
            v.checked_mul(10i128.checked_pow(*scale as u32)?)
                .map(Datum::Num)
        }
        (IcebergType::String, ScalarValue::Utf8(s)) => Some(Datum::Str(s.clone())),
        _ => None,
    }
}

/// Resolved per-file value range of one column.
#[derive(Debug, Clone, Default)]
struct Range {
    lower: Option<Datum>,
    upper: Option<Datum>,
}

/// Everything the evaluator needs about the table's columns: for each
/// table column index, its field ID and Iceberg type.
pub type ColumnFields = [(i32, IcebergType)];

/// Build the per-column ranges for one data file from its column bounds
/// and (for identity partition fields) its partition tuple.
fn file_ranges(
    file: &DataFile,
    spec: Option<&PartitionSpec>,
    columns: &ColumnFields,
) -> HashMap<i32, Range> {
    let mut out: HashMap<i32, Range> = HashMap::new();
    for (field_id, ty) in columns {
        let lower = file
            .lower_bounds
            .get(field_id)
            .and_then(|b| decode_bound(ty, b));
        let upper = file
            .upper_bounds
            .get(field_id)
            .and_then(|b| decode_bound(ty, b));
        if lower.is_some() || upper.is_some() {
            out.insert(*field_id, Range { lower, upper });
        }
    }
    if let Some(spec) = spec {
        for pf in spec.fields.iter().filter(|f| f.transform == "identity") {
            let Some((_, ty)) = columns.iter().find(|(id, _)| *id == pf.source_id) else {
                continue;
            };
            let Some(Some(v)) = file.partition.get(&pf.name) else {
                continue;
            };
            if let Some(d) = partition_datum(ty, v) {
                // An identity partition value is an exact range.
                out.insert(
                    pf.source_id,
                    Range {
                        lower: Some(d.clone()),
                        upper: Some(d),
                    },
                );
            }
        }
    }
    out
}

/// `true` when the file provably contains no row matching `filters`
/// (a conjunction). Column indices in `filters` refer to `columns`.
pub fn file_can_be_skipped(
    file: &DataFile,
    spec: Option<&PartitionSpec>,
    columns: &ColumnFields,
    filters: &[PlanExpr],
) -> bool {
    if filters.is_empty() {
        return false;
    }
    let ranges = file_ranges(file, spec, columns);
    if ranges.is_empty() {
        return false;
    }
    filters.iter().any(|f| expr_excludes(f, &ranges, columns))
}

fn column_ref<'a>(
    e: &PlanExpr,
    ranges: &'a HashMap<i32, Range>,
    columns: &'a ColumnFields,
) -> Option<(&'a Range, &'a IcebergType)> {
    let PlanExpr::Column { index, .. } = e else {
        return None;
    };
    let (field_id, ty) = columns.get(*index)?;
    Some((ranges.get(field_id)?, ty))
}

fn literal(e: &PlanExpr) -> Option<&ScalarValue> {
    match e {
        PlanExpr::Literal { value, .. } if !matches!(value, ScalarValue::Null) => Some(value),
        _ => None,
    }
}

/// `true` when `lit` lies strictly outside `[lower, upper]`.
fn outside(range: &Range, lit: &Datum) -> bool {
    let below = range
        .lower
        .as_ref()
        .and_then(|l| lit.cmp(l))
        .is_some_and(|o| o == Ordering::Less);
    let above = range
        .upper
        .as_ref()
        .and_then(|u| lit.cmp(u))
        .is_some_and(|o| o == Ordering::Greater);
    below || above
}

fn expr_excludes(e: &PlanExpr, ranges: &HashMap<i32, Range>, columns: &ColumnFields) -> bool {
    match e {
        PlanExpr::BinaryOp {
            left, op, right, ..
        } => {
            if *op == BinaryOp::And {
                return expr_excludes(left, ranges, columns)
                    || expr_excludes(right, ranges, columns);
            }
            let (col, lit, op) = match (column_ref(left, ranges, columns), literal(right)) {
                (Some(c), Some(l)) => (c, l, *op),
                _ => match (literal(left), column_ref(right, ranges, columns)) {
                    (Some(l), Some(c)) => {
                        let flipped = match op {
                            BinaryOp::Lt => BinaryOp::Gt,
                            BinaryOp::LtEq => BinaryOp::GtEq,
                            BinaryOp::Gt => BinaryOp::Lt,
                            BinaryOp::GtEq => BinaryOp::LtEq,
                            other => *other,
                        };
                        (c, l, flipped)
                    }
                    _ => return false,
                },
            };
            let (range, ty) = col;
            let Some(lit) = literal_datum(ty, lit) else {
                return false;
            };
            let cmp_lower = range.lower.as_ref().and_then(|l| l.cmp(&lit));
            let cmp_upper = range.upper.as_ref().and_then(|u| u.cmp(&lit));
            match op {
                BinaryOp::Eq => outside(range, &lit),
                // col < lit: no match if min >= lit
                BinaryOp::Lt => matches!(cmp_lower, Some(Ordering::Greater | Ordering::Equal)),
                BinaryOp::LtEq => matches!(cmp_lower, Some(Ordering::Greater)),
                // col > lit: no match if max <= lit
                BinaryOp::Gt => matches!(cmp_upper, Some(Ordering::Less | Ordering::Equal)),
                BinaryOp::GtEq => matches!(cmp_upper, Some(Ordering::Less)),
                _ => false,
            }
        }
        PlanExpr::InList {
            expr,
            list,
            negated: false,
            ..
        } => {
            let Some((range, ty)) = column_ref(expr, ranges, columns) else {
                return false;
            };
            if list.is_empty() {
                return false;
            }
            list.iter().all(|item| {
                literal(item)
                    .and_then(|l| literal_datum(ty, l))
                    .is_some_and(|d| outside(range, &d))
            })
        }
        PlanExpr::Between {
            expr,
            negated: false,
            low,
            high,
            ..
        } => {
            let Some((range, ty)) = column_ref(expr, ranges, columns) else {
                return false;
            };
            let low = literal(low).and_then(|l| literal_datum(ty, l));
            let high = literal(high).and_then(|l| literal_datum(ty, l));
            // No match if max < low or min > high.
            let above_max = match (&low, &range.upper) {
                (Some(lo), Some(up)) => up.cmp(lo) == Some(Ordering::Less),
                _ => false,
            };
            let below_min = match (&high, &range.lower) {
                (Some(hi), Some(lw)) => lw.cmp(hi) == Some(Ordering::Greater),
                _ => false,
            };
            above_max || below_min
        }
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::DataFileContent;
    use crate::metadata::PartitionField;

    fn col(index: usize) -> PlanExpr {
        PlanExpr::Column {
            index,
            name: format!("c{index}"),
            span: None,
        }
    }
    fn lit(v: ScalarValue) -> PlanExpr {
        PlanExpr::Literal {
            value: v,
            span: None,
        }
    }
    fn bin(l: PlanExpr, op: BinaryOp, r: PlanExpr) -> PlanExpr {
        PlanExpr::BinaryOp {
            left: Box::new(l),
            op,
            right: Box::new(r),
            span: None,
        }
    }

    fn file(lower: i64, upper: i64, region: Option<i64>) -> DataFile {
        let mut partition = HashMap::new();
        partition.insert("n_regionkey".to_string(), region.map(Value::Long));
        DataFile {
            content: DataFileContent::Data,
            file_path: "f.parquet".into(),
            file_format: "PARQUET".into(),
            record_count: 10,
            file_size_in_bytes: 100,
            partition,
            lower_bounds: [(1, lower.to_le_bytes().to_vec()), (2, b"ALGERIA".to_vec())].into(),
            upper_bounds: [(1, upper.to_le_bytes().to_vec()), (2, b"KENYA".to_vec())].into(),
        }
    }

    fn columns() -> Vec<(i32, IcebergType)> {
        vec![
            (1, IcebergType::Long),
            (2, IcebergType::String),
            (3, IcebergType::Long),
        ]
    }

    fn spec() -> PartitionSpec {
        PartitionSpec {
            spec_id: 0,
            fields: vec![PartitionField {
                source_id: 3,
                name: "n_regionkey".into(),
                transform: "identity".into(),
            }],
        }
    }

    fn skip(f: &DataFile, filters: &[PlanExpr]) -> bool {
        file_can_be_skipped(f, Some(&spec()), &columns(), filters)
    }

    #[test]
    fn comparison_against_bounds() {
        let f = file(10, 20, None);
        let i = |v| lit(ScalarValue::Int64(v));
        assert!(skip(&f, &[bin(col(0), BinaryOp::Eq, i(25))]));
        assert!(skip(&f, &[bin(col(0), BinaryOp::Eq, i(9))]));
        assert!(!skip(&f, &[bin(col(0), BinaryOp::Eq, i(15))]));
        assert!(skip(&f, &[bin(col(0), BinaryOp::Lt, i(10))]));
        assert!(!skip(&f, &[bin(col(0), BinaryOp::LtEq, i(10))]));
        assert!(skip(&f, &[bin(col(0), BinaryOp::Gt, i(20))]));
        assert!(!skip(&f, &[bin(col(0), BinaryOp::GtEq, i(20))]));
        // Literal on the left: 25 < col  ==  col > 25
        assert!(skip(&f, &[bin(i(25), BinaryOp::Lt, col(0))]));
        // Int32 literal against a long column
        assert!(skip(
            &f,
            &[bin(col(0), BinaryOp::Eq, lit(ScalarValue::Int32(99)))]
        ));
        // NotEq / OR are never used to prune
        assert!(!skip(&f, &[bin(col(0), BinaryOp::NotEq, i(15))]));
        assert!(!skip(
            &f,
            &[bin(
                bin(col(0), BinaryOp::Eq, i(99)),
                BinaryOp::Or,
                bin(col(0), BinaryOp::Eq, i(15))
            )]
        ));
        // AND prunes if either side does
        assert!(skip(
            &f,
            &[bin(
                bin(col(0), BinaryOp::Eq, i(15)),
                BinaryOp::And,
                bin(col(0), BinaryOp::Eq, i(99))
            )]
        ));
    }

    #[test]
    fn string_bounds() {
        let f = file(10, 20, None);
        let s = |v: &str| lit(ScalarValue::Utf8(v.into()));
        assert!(skip(&f, &[bin(col(1), BinaryOp::Eq, s("ZAMBIA"))]));
        assert!(!skip(&f, &[bin(col(1), BinaryOp::Eq, s("FRANCE"))]));
        assert!(skip(&f, &[bin(col(1), BinaryOp::Lt, s("AA"))]));
    }

    #[test]
    fn identity_partition_value() {
        let f = file(10, 20, Some(3));
        let i = |v| lit(ScalarValue::Int64(v));
        assert!(skip(&f, &[bin(col(2), BinaryOp::Eq, i(1))]));
        assert!(!skip(&f, &[bin(col(2), BinaryOp::Eq, i(3))]));
        // null partition value: no information → keep
        let f = file(10, 20, None);
        assert!(!skip(&f, &[bin(col(2), BinaryOp::Eq, i(1))]));
    }

    #[test]
    fn in_list_and_between() {
        let f = file(10, 20, None);
        let i = |v| lit(ScalarValue::Int64(v));
        let inlist = |vals: Vec<i64>| PlanExpr::InList {
            expr: Box::new(col(0)),
            list: vals.into_iter().map(i).collect(),
            negated: false,
            span: None,
        };
        assert!(skip(&f, &[inlist(vec![1, 2, 30])]));
        assert!(!skip(&f, &[inlist(vec![1, 12])]));
        let between = |lo, hi| PlanExpr::Between {
            expr: Box::new(col(0)),
            negated: false,
            low: Box::new(i(lo)),
            high: Box::new(i(hi)),
            span: None,
        };
        assert!(skip(&f, &[between(21, 30)]));
        assert!(skip(&f, &[between(0, 9)]));
        assert!(!skip(&f, &[between(0, 10)]));
    }

    #[test]
    fn missing_bounds_or_unknown_types_keep_file() {
        let mut f = file(10, 20, None);
        f.lower_bounds.clear();
        f.upper_bounds.clear();
        assert!(!skip(
            &f,
            &[bin(col(0), BinaryOp::Eq, lit(ScalarValue::Int64(99)))]
        ));
        // Type mismatch (string literal against long column) → keep
        let f = file(10, 20, None);
        assert!(!skip(
            &f,
            &[bin(
                col(0),
                BinaryOp::Eq,
                lit(ScalarValue::Utf8("x".into()))
            )]
        ));
        // Out-of-range column index → keep
        assert!(!skip(
            &f,
            &[bin(col(9), BinaryOp::Eq, lit(ScalarValue::Int64(99)))]
        ));
    }

    #[test]
    fn decode_bounds() {
        assert_eq!(
            decode_bound(&IcebergType::Int, &7i32.to_le_bytes()),
            Some(Datum::Num(7))
        );
        // int → long promotion: 4-byte bound on a long column
        assert_eq!(
            decode_bound(&IcebergType::Long, &(-3i32).to_le_bytes()),
            Some(Datum::Num(-3))
        );
        // decimal unscaled 12345 (0x3039) and -1 (0xff)
        assert_eq!(
            decode_bound(
                &IcebergType::Decimal {
                    precision: 12,
                    scale: 2
                },
                &[0x30, 0x39]
            ),
            Some(Datum::Num(12345))
        );
        assert_eq!(
            decode_bound(
                &IcebergType::Decimal {
                    precision: 12,
                    scale: 2
                },
                &[0xff]
            ),
            Some(Datum::Num(-1))
        );
        assert_eq!(decode_bound(&IcebergType::Double, &[0; 8]), None);
    }

    #[test]
    fn decimal_literal_rescaled() {
        let ty = IcebergType::Decimal {
            precision: 12,
            scale: 2,
        };
        let d = literal_datum(
            &ty,
            &ScalarValue::Decimal128 {
                value: 15,
                precision: 3,
                scale: 1,
            },
        );
        assert_eq!(d, Some(Datum::Num(150)));
        // finer scale than the column cannot be represented exactly
        let d = literal_datum(
            &ty,
            &ScalarValue::Decimal128 {
                value: 1234,
                precision: 5,
                scale: 3,
            },
        );
        assert_eq!(d, None);
        assert_eq!(
            literal_datum(&ty, &ScalarValue::Int64(3)),
            Some(Datum::Num(300))
        );
    }
}
