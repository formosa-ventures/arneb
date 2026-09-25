//! Argument helpers shared by the scalar function implementations.
//!
//! Every helper normalises an input [`ArrayRef`] to the physical Arrow
//! type a function body wants, using Arrow's `cast` kernel. That makes
//! the functions tolerant of the shapes the evaluator actually hands
//! them: `NULL` literals arrive as [`NullArray`](arrow::array::NullArray),
//! integer literals as `Int64`, table columns as `Int32` / `Decimal128`
//! / `LargeUtf8`, and so on.

use arneb_common::error::ExecutionError;
use arrow::array::{
    Array, ArrayRef, Float64Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType as ArrowDataType, TimeUnit};

/// Build an `InvalidOperation` error.
pub(crate) fn invalid(msg: impl Into<String>) -> ExecutionError {
    ExecutionError::InvalidOperation(msg.into())
}

/// Check that `args.len()` lies in `min..=max`.
pub(crate) fn check_arity(
    name: &str,
    args: &[ArrayRef],
    min: usize,
    max: usize,
) -> Result<(), ExecutionError> {
    if args.len() < min || args.len() > max {
        let expected = if min == max {
            format!("{min}")
        } else {
            format!("{min} to {max}")
        };
        return Err(invalid(format!(
            "{name} expects {expected} argument(s), got {}",
            args.len()
        )));
    }
    Ok(())
}

fn cast_to(arr: &ArrayRef, to: &ArrowDataType, name: &str) -> Result<ArrayRef, ExecutionError> {
    if arr.data_type() == to {
        return Ok(arr.clone());
    }
    arrow::compute::cast(arr, to).map_err(|e| {
        invalid(format!(
            "{name}: cannot cast argument of type {} to {to}: {e}",
            arr.data_type()
        ))
    })
}

/// Normalise to a Utf8 [`StringArray`].
pub(crate) fn utf8(arr: &ArrayRef, name: &str) -> Result<StringArray, ExecutionError> {
    let casted = cast_to(arr, &ArrowDataType::Utf8, name)?;
    Ok(casted
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("cast to Utf8 yields StringArray")
        .clone())
}

/// Normalise to an [`Int64Array`].
pub(crate) fn int64(arr: &ArrayRef, name: &str) -> Result<Int64Array, ExecutionError> {
    let casted = cast_to(arr, &ArrowDataType::Int64, name)?;
    Ok(casted
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("cast to Int64 yields Int64Array")
        .clone())
}

/// Normalise to a [`Float64Array`].
pub(crate) fn float64(arr: &ArrayRef, name: &str) -> Result<Float64Array, ExecutionError> {
    let casted = cast_to(arr, &ArrowDataType::Float64, name)?;
    Ok(casted
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("cast to Float64 yields Float64Array")
        .clone())
}

/// The canonical timestamp type produced by the date/time functions:
/// microsecond precision, no zone (values are UTC wall-clock).
pub(crate) fn timestamp_us_type() -> ArrowDataType {
    ArrowDataType::Timestamp(TimeUnit::Microsecond, None)
}

/// Normalise a DATE / TIMESTAMP / VARCHAR / NULL argument to
/// microsecond timestamps (UTC wall-clock).
pub(crate) fn timestamp_us(
    arr: &ArrayRef,
    name: &str,
) -> Result<TimestampMicrosecondArray, ExecutionError> {
    match arr.data_type() {
        ArrowDataType::Date32
        | ArrowDataType::Date64
        | ArrowDataType::Timestamp(_, _)
        | ArrowDataType::Utf8
        | ArrowDataType::LargeUtf8
        | ArrowDataType::Null => {}
        other => {
            return Err(invalid(format!(
                "{name}: expected a DATE or TIMESTAMP argument, got {other}"
            )))
        }
    }
    let casted = cast_to(arr, &timestamp_us_type(), name)?;
    Ok(casted
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .expect("cast to Timestamp(us) yields TimestampMicrosecondArray")
        .clone())
}

/// Cast a result back to `target` (used to hand a DATE/TIMESTAMP
/// function result back in the caller's input type).
pub(crate) fn cast_result(
    arr: ArrayRef,
    target: &ArrowDataType,
) -> Result<ArrayRef, ExecutionError> {
    if arr.data_type() == target {
        return Ok(arr);
    }
    arrow::compute::cast(&arr, target).map_err(ExecutionError::from)
}

/// Return the value of a constant (all-rows-equal) Utf8 argument, if
/// the array is non-empty, non-null and constant. Lets per-row loops
/// hoist expensive setup (regex compilation, format parsing) out of
/// the loop in the common "literal pattern" case.
pub(crate) fn constant_str(arr: &StringArray) -> Option<&str> {
    if arr.is_empty() || arr.null_count() > 0 {
        return None;
    }
    let first = arr.value(0);
    (1..arr.len())
        .all(|i| arr.value(i) == first)
        .then_some(first)
}
