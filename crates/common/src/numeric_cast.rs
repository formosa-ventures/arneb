//! Rounding for casts from fractional numeric values to SQL integer types.

use std::sync::Arc;

use arrow::array::{ArrayRef, AsArray, PrimitiveArray};
use arrow::compute::CastOptions;
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, Decimal128Type, Decimal64Type, Float32Type, Float64Type,
    Int16Type, Int32Type, Int64Type, Int8Type,
};
use arrow::error::ArrowError;

/// Cast a decimal or floating-point array to an integer, rounding half away from zero.
/// Returns `None` when the source or target is outside this conversion family.
///
/// As in Trino, a finite value whose *rounded* result is outside the target
/// range is an error (so `CAST(127.5 AS TINYINT)` fails rather than
/// returning 127), whatever `options.safe` says. `options.safe` only decides
/// what NaN and ±Infinity become: NULL when safe, an error otherwise. A NaN
/// error is worded as Trino's (`Cannot cast double NaN to integer`), and
/// [`is_nan_cast_error`] tells it from an out-of-range one.
pub fn cast_fractional_to_integer(
    array: &ArrayRef,
    target: &DataType,
    options: &CastOptions<'_>,
) -> Result<Option<ArrayRef>, ArrowError> {
    let (min, max) = match target {
        DataType::Int8 => (i8::MIN as i64, i8::MAX as i64),
        DataType::Int16 => (i16::MIN as i64, i16::MAX as i64),
        DataType::Int32 => (i32::MIN as i64, i32::MAX as i64),
        DataType::Int64 => (i64::MIN, i64::MAX),
        _ => return Ok(None),
    };
    let rounded: Vec<Option<i64>> = match array.data_type() {
        DataType::Float32 => array
            .as_primitive::<Float32Type>()
            .iter()
            .map(|value| {
                value
                    .map(|v| rounded_float(v as f64, "real", min, max, target, options))
                    .transpose()
                    .map(Option::flatten)
            })
            .collect::<Result<_, _>>()?,
        DataType::Float64 => array
            .as_primitive::<Float64Type>()
            .iter()
            .map(|value| {
                value
                    .map(|v| rounded_float(v, "double", min, max, target, options))
                    .transpose()
                    .map(Option::flatten)
            })
            .collect::<Result<_, _>>()?,
        DataType::Decimal64(_, scale) => array
            .as_primitive::<Decimal64Type>()
            .iter()
            .map(|value| {
                value
                    .map(|v| rounded_decimal(v as i128, *scale, min, max, target))
                    .transpose()
            })
            .collect::<Result<_, _>>()?,
        DataType::Decimal128(_, scale) => array
            .as_primitive::<Decimal128Type>()
            .iter()
            .map(|value| {
                value
                    .map(|v| rounded_decimal(v, *scale, min, max, target))
                    .transpose()
            })
            .collect::<Result<_, _>>()?,
        _ => return Ok(None),
    };

    // Every value is in range by now, so the narrowing `as` is exact.
    let result: ArrayRef = match target {
        DataType::Int8 => build::<Int8Type>(&rounded, |v| v as i8),
        DataType::Int16 => build::<Int16Type>(&rounded, |v| v as i16),
        DataType::Int32 => build::<Int32Type>(&rounded, |v| v as i32),
        _ => build::<Int64Type>(&rounded, |v| v),
    };
    Ok(Some(result))
}

fn build<T: ArrowPrimitiveType>(
    values: &[Option<i64>],
    narrow: impl Fn(i64) -> T::Native,
) -> ArrayRef {
    Arc::new(
        values
            .iter()
            .map(|v| v.map(&narrow))
            .collect::<PrimitiveArray<T>>(),
    )
}

fn out_of_range(value: impl std::fmt::Display, target: &DataType) -> ArrowError {
    ArrowError::CastError(format!("value {value} is out of range for {target}"))
}

/// Start of the error a NaN cast to an integer raises; see [`is_nan_cast_error`].
const NAN_CAST: &str = "Cannot cast ";

/// Whether `err` is a NaN cast to an integer type failing. Trino reports
/// that as an invalid cast argument, unlike a value out of range.
pub fn is_nan_cast_error(err: &ArrowError) -> bool {
    matches!(err, ArrowError::CastError(m) if m.starts_with(NAN_CAST) && m.contains(" NaN to "))
}

/// The SQL name of an integer target, as Trino's messages use it.
fn sql_integer_name(target: &DataType) -> &'static str {
    match target {
        DataType::Int8 => "tinyint",
        DataType::Int16 => "smallint",
        DataType::Int32 => "integer",
        _ => "bigint",
    }
}

/// Round a float; `Ok(None)` is a NULL result (NaN/Infinity under a safe cast).
/// `source` is the SQL name of the float type, for the NaN message.
fn rounded_float(
    value: f64,
    source: &str,
    min: i64,
    max: i64,
    target: &DataType,
    options: &CastOptions<'_>,
) -> Result<Option<i64>, ArrowError> {
    if !value.is_finite() {
        return if options.safe {
            Ok(None)
        } else if value.is_nan() {
            Err(ArrowError::CastError(format!(
                "{NAN_CAST}{source} NaN to {}",
                sql_integer_name(target)
            )))
        } else {
            Err(out_of_range(value, target))
        };
    }
    let rounded = value.round();
    // `min` (a negated power of two) and `max + 1` are exact as f64.
    let upper = (max as i128 + 1) as f64;
    if rounded >= min as f64 && rounded < upper {
        Ok(Some(rounded as i64))
    } else {
        Err(out_of_range(value, target))
    }
}

fn rounded_decimal(
    value: i128,
    scale: i8,
    min: i64,
    max: i64,
    target: &DataType,
) -> Result<i64, ArrowError> {
    let rounded = if scale <= 0 {
        10_i128
            .checked_pow(scale.unsigned_abs() as u32)
            .and_then(|factor| value.checked_mul(factor))
    } else {
        let factor = 10_i128.pow(scale as u32);
        let quotient = value / factor;
        let remainder = value % factor;
        Some(if remainder.abs() >= factor / 2 {
            quotient + value.signum()
        } else {
            quotient
        })
    };
    match rounded {
        Some(r) if r >= min as i128 && r <= max as i128 => Ok(r as i64),
        _ => Err(out_of_range(format_decimal(value, scale), target)),
    }
}

fn format_decimal(value: i128, scale: i8) -> String {
    if scale <= 0 {
        return format!("{value}e{}", -(scale as i32));
    }
    let factor = 10_i128.pow(scale as u32);
    let sign = if value < 0 { "-" } else { "" };
    let abs = value.unsigned_abs();
    let factor = factor as u128;
    format!(
        "{sign}{}.{:0width$}",
        abs / factor,
        abs % factor,
        width = scale as usize
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Decimal128Array, Decimal64Array, Float32Array, Float64Array};

    fn cast(array: ArrayRef, target: DataType, safe: bool) -> Result<ArrayRef, ArrowError> {
        let options = CastOptions {
            safe,
            ..Default::default()
        };
        Ok(cast_fractional_to_integer(&array, &target, &options)?.expect("handled"))
    }

    fn values(array: &ArrayRef) -> Vec<Option<i64>> {
        let array = arrow::compute::cast(array, &DataType::Int64).unwrap();
        array.as_primitive::<Int64Type>().iter().collect()
    }

    #[test]
    fn floats_just_inside_each_bound_round_to_the_bound() {
        for (target, min, max) in [
            (DataType::Int8, i8::MIN as f64, i8::MAX as f64),
            (DataType::Int16, i16::MIN as f64, i16::MAX as f64),
            (DataType::Int32, i32::MIN as f64, i32::MAX as f64),
        ] {
            let array: ArrayRef =
                Arc::new(Float64Array::from(vec![max + 0.49, min - 0.49, max, min]));
            let got = cast(array, target.clone(), true).unwrap();
            assert_eq!(got.data_type(), &target);
            assert_eq!(
                values(&got),
                vec![
                    Some(max as i64),
                    Some(min as i64),
                    Some(max as i64),
                    Some(min as i64)
                ]
            );
        }
    }

    #[test]
    fn floats_rounding_past_each_bound_are_errors_even_when_safe() {
        for (target, min, max) in [
            (DataType::Int8, i8::MIN as f64, i8::MAX as f64),
            (DataType::Int16, i16::MIN as f64, i16::MAX as f64),
            (DataType::Int32, i32::MIN as f64, i32::MAX as f64),
        ] {
            for v in [max + 0.5, min - 0.5, max + 1.4] {
                for safe in [true, false] {
                    let array: ArrayRef = Arc::new(Float64Array::from(vec![Some(1.0), Some(v)]));
                    let err = cast(array, target.clone(), safe).unwrap_err();
                    assert!(
                        err.to_string().contains("out of range"),
                        "{v} {target}: {err}"
                    );
                }
            }
        }
        // f32 and i64: 2^63 is the first float past i64::MAX.
        let array: ArrayRef = Arc::new(Float32Array::from(vec![127.5_f32]));
        assert!(cast(array, DataType::Int8, true).is_err());
        let array: ArrayRef = Arc::new(Float64Array::from(vec![2f64.powi(63)]));
        assert!(cast(array, DataType::Int64, true).is_err());
    }

    #[test]
    fn non_finite_floats_follow_the_safe_option() {
        let array: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(f64::NAN),
            Some(f64::INFINITY),
            None,
            Some(2.5),
        ]));
        let got = cast(array.clone(), DataType::Int32, true).unwrap();
        assert_eq!(values(&got), vec![None, None, None, Some(3)]);
        assert!(cast(array, DataType::Int32, false).is_err());
    }

    /// Trino: NaN is an invalid cast argument, worded after the source and
    /// target types; ±Infinity is out of range.
    #[test]
    fn nan_and_infinity_fail_differently() {
        let strict = CastOptions {
            safe: false,
            ..Default::default()
        };
        let nan64: ArrayRef = Arc::new(Float64Array::from(vec![f64::NAN]));
        let err = cast_fractional_to_integer(&nan64, &DataType::Int32, &strict).unwrap_err();
        assert!(is_nan_cast_error(&err), "{err}");
        assert!(
            err.to_string()
                .contains("Cannot cast double NaN to integer"),
            "{err}"
        );
        let nan32: ArrayRef = Arc::new(Float32Array::from(vec![f32::NAN]));
        let err = cast_fractional_to_integer(&nan32, &DataType::Int8, &strict).unwrap_err();
        assert!(
            err.to_string().contains("Cannot cast real NaN to tinyint"),
            "{err}"
        );
        for inf in [f64::INFINITY, f64::NEG_INFINITY] {
            let array: ArrayRef = Arc::new(Float64Array::from(vec![inf]));
            let err = cast_fractional_to_integer(&array, &DataType::Int64, &strict).unwrap_err();
            assert!(!is_nan_cast_error(&err), "{err}");
            assert!(err.to_string().contains("out of range"), "{err}");
        }
        let finite: ArrayRef = Arc::new(Float64Array::from(vec![1e20]));
        let err = cast_fractional_to_integer(&finite, &DataType::Int32, &strict).unwrap_err();
        assert!(!is_nan_cast_error(&err), "{err}");
    }

    #[test]
    fn decimals_rounding_past_each_bound_are_errors() {
        // 9223372036854775807.5 and -9223372036854775808.5 as DECIMAL(20, 1).
        let max = i64::MAX as i128 * 10 + 5;
        let min = i64::MIN as i128 * 10 - 5;
        for v in [max, min] {
            let array: ArrayRef = Arc::new(
                Decimal128Array::from(vec![v])
                    .with_precision_and_scale(20, 1)
                    .unwrap(),
            );
            let err = cast(array, DataType::Int64, true).unwrap_err();
            assert!(err.to_string().contains("out of range"), "{err}");
        }
        let inside: ArrayRef = Arc::new(
            Decimal128Array::from(vec![max - 1, min + 1])
                .with_precision_and_scale(20, 1)
                .unwrap(),
        );
        assert_eq!(
            values(&cast(inside, DataType::Int64, true).unwrap()),
            vec![Some(i64::MAX), Some(i64::MIN)]
        );
        // 127.5 / -128.5 as Decimal64.
        for v in [1275_i64, -1285] {
            let array: ArrayRef = Arc::new(
                Decimal64Array::from(vec![v])
                    .with_precision_and_scale(5, 1)
                    .unwrap(),
            );
            let err = cast(array, DataType::Int8, true).unwrap_err();
            assert!(err.to_string().contains("value"), "{err}");
        }
    }

    #[test]
    fn non_positive_scale_decimals_are_exact_and_range_checked() {
        let array: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(12), Some(-3), None])
                .with_precision_and_scale(5, -1)
                .unwrap(),
        );
        assert_eq!(
            values(&cast(array.clone(), DataType::Int8, true).unwrap()),
            vec![Some(120), Some(-30), None]
        );
        let array: ArrayRef = Arc::new(
            Decimal128Array::from(vec![13])
                .with_precision_and_scale(5, -1)
                .unwrap(),
        );
        assert!(cast(array, DataType::Int8, true).is_err());
    }

    #[test]
    fn formats_decimals_for_errors() {
        assert_eq!(format_decimal(1275, 1), "127.5");
        assert_eq!(format_decimal(-5, 2), "-0.05");
        assert_eq!(format_decimal(13, -1), "13e1");
    }
}
