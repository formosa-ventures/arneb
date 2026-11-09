//! Rounding for casts from fractional numeric values to SQL integer types.

use std::sync::Arc;

use arrow::array::{ArrayRef, AsArray, Int64Array};
use arrow::compute::{cast_with_options, CastOptions};
use arrow::datatypes::{
    DataType, Decimal128Type, Decimal64Type, Float32Type, Float64Type, Int64Type,
};
use arrow::error::ArrowError;

/// Cast a decimal or floating-point array to an integer, rounding half away from zero.
/// Returns `None` when the source or target is outside this conversion family.
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
            .map(|value| value.and_then(|v| rounded_float(v.round() as f64, min, max)))
            .collect(),
        DataType::Float64 => array
            .as_primitive::<Float64Type>()
            .iter()
            .map(|value| value.and_then(|v| rounded_float(v.round(), min, max)))
            .collect(),
        DataType::Decimal64(_, scale) => array
            .as_primitive::<Decimal64Type>()
            .iter()
            .map(|value| value.and_then(|v| rounded_decimal(v as i128, *scale, min, max)))
            .collect(),
        DataType::Decimal128(_, scale) => array
            .as_primitive::<Decimal128Type>()
            .iter()
            .map(|value| value.and_then(|v| rounded_decimal(v, *scale, min, max)))
            .collect(),
        _ => return Ok(None),
    };

    // Keep Arrow's existing result for values whose rounded result is outside
    // the target range, including its overflow and non-finite handling.
    let original = cast_with_options(array, target, options)?;
    let original_i64 = cast_with_options(&original, &DataType::Int64, options)?;
    let values: Int64Array = original_i64
        .as_primitive::<Int64Type>()
        .iter()
        .zip(rounded)
        .map(|(original, rounded)| original.map(|value| rounded.unwrap_or(value)))
        .collect();
    let values: ArrayRef = Arc::new(values);
    Ok(Some(cast_with_options(&values, target, options)?))
}

fn rounded_float(value: f64, min: i64, max: i64) -> Option<i64> {
    let upper = (max as i128 + 1) as f64;
    (value.is_finite() && value >= min as f64 && value < upper).then_some(value as i64)
}

fn rounded_decimal(value: i128, scale: i8, min: i64, max: i64) -> Option<i64> {
    if scale <= 0 {
        return None;
    }
    let factor = 10_i128.pow(scale as u32);
    let quotient = value / factor;
    let remainder = value % factor;
    let rounded = if remainder.abs() >= factor / 2 {
        quotient + value.signum()
    } else {
        quotient
    };
    (rounded >= min as i128 && rounded <= max as i128).then_some(rounded as i64)
}
