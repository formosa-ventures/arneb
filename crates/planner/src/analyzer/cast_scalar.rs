//! Plan-time `ScalarValue` → `ScalarValue` cast.
//!
//! Used by [`super::super::optimizer::ConstantFolding`] to fold
//! `Cast(Literal(v), T)` into `Literal(v.cast_to(T))`. The cast
//! runs through Arrow's `cast_with_options` applied to a
//! single-element array — the slow path for a compile-time one-shot
//! conversion but authoritative: if Arrow can cast the runtime
//! array, Arrow can cast the literal.
//!
//! Parse failures (e.g., `DATE '1998-13-45'`) surface as
//! [`PlanError::InvalidLiteral`] so the query fails at plan time
//! rather than mid-stream.

use std::sync::Arc;

use arneb_common::error::PlanError;
use arneb_common::types::{DataType, ScalarValue, TimeUnit};
use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int32Array, Int64Array, NullArray, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
};
use arrow::compute::{cast_with_options, CastOptions};
use arrow::datatypes::DataType as ArrowDataType;

/// Cast a literal from its current representation to `target`,
/// returning the converted `ScalarValue` or a `PlanError::InvalidLiteral`
/// if the cast fails.
///
/// Strict mode (`safe: false`) is used so parse failures become
/// errors rather than silent nulls — this matches the user's
/// expectation that `DATE '1998-13-45'` is a programming mistake,
/// not a null row.
pub fn cast_scalar(value: &ScalarValue, target: &DataType) -> Result<ScalarValue, PlanError> {
    if &value.data_type() == target {
        return Ok(value.clone());
    }
    let source_array =
        scalar_to_single_element_array(value).map_err(|e| PlanError::InvalidLiteral {
            message: format!("cannot build array for literal {value}: {e}"),
            location: None,
        })?;
    let target_arrow: ArrowDataType = target.clone().into();
    let options = CastOptions {
        safe: false,
        ..Default::default()
    };
    let cast = cast_with_options(&source_array, &target_arrow, &options).map_err(|e| {
        PlanError::InvalidLiteral {
            message: format!("cannot cast literal {value} to {target}: {e}"),
            location: None,
        }
    })?;
    array_element_to_scalar(&cast, 0, target).map_err(|e| PlanError::InvalidLiteral {
        message: format!("cannot extract cast result for {value} as {target}: {e}"),
        location: None,
    })
}

/// Build a single-element Arrow array from a `ScalarValue`. Mirror
/// of the execution-side `scalar_to_array(value, 1)` but lives here
/// to keep the planner from depending on `arneb-execution`.
fn scalar_to_single_element_array(value: &ScalarValue) -> Result<ArrayRef, String> {
    let arr: ArrayRef = match value {
        ScalarValue::Null => Arc::new(NullArray::new(1)),
        ScalarValue::Boolean(v) => Arc::new(BooleanArray::from(vec![*v])),
        ScalarValue::Int32(v) => Arc::new(Int32Array::from(vec![*v])),
        ScalarValue::Int64(v) => Arc::new(Int64Array::from(vec![*v])),
        ScalarValue::Float32(v) => Arc::new(Float32Array::from(vec![*v])),
        ScalarValue::Float64(v) => Arc::new(Float64Array::from(vec![*v])),
        ScalarValue::Utf8(v) => Arc::new(StringArray::from(vec![v.as_str()])),
        ScalarValue::Binary(v) => Arc::new(arrow::array::BinaryArray::from(vec![v.as_slice()])),
        ScalarValue::Date32(v) => Arc::new(Date32Array::from(vec![*v])),
        ScalarValue::Decimal128 {
            value,
            precision,
            scale,
        } => {
            let a = Decimal128Array::from(vec![*value])
                .with_precision_and_scale(*precision, *scale)
                .map_err(|e| format!("invalid decimal: {e}"))?;
            Arc::new(a)
        }
        ScalarValue::Timestamp {
            value,
            unit,
            timezone,
        } => match unit {
            TimeUnit::Second => {
                let a: TimestampSecondArray = vec![*value].into();
                match timezone {
                    Some(tz) => Arc::new(a.with_timezone(tz.clone())),
                    None => Arc::new(a),
                }
            }
            TimeUnit::Millisecond => {
                let a: TimestampMillisecondArray = vec![*value].into();
                match timezone {
                    Some(tz) => Arc::new(a.with_timezone(tz.clone())),
                    None => Arc::new(a),
                }
            }
            TimeUnit::Microsecond => {
                let a: TimestampMicrosecondArray = vec![*value].into();
                match timezone {
                    Some(tz) => Arc::new(a.with_timezone(tz.clone())),
                    None => Arc::new(a),
                }
            }
            TimeUnit::Nanosecond => {
                let a: TimestampNanosecondArray = vec![*value].into();
                match timezone {
                    Some(tz) => Arc::new(a.with_timezone(tz.clone())),
                    None => Arc::new(a),
                }
            }
        },
        // `ScalarValue` is `#[non_exhaustive]` — refuse unknown variants.
        other => {
            return Err(format!(
                "cannot build single-element array for scalar variant: {other:?}"
            ))
        }
    };
    Ok(arr)
}

/// Extract index `i` of `array` as a `ScalarValue` of type `expected`.
fn array_element_to_scalar(
    array: &ArrayRef,
    i: usize,
    expected: &DataType,
) -> Result<ScalarValue, String> {
    if array.is_null(i) {
        return Ok(ScalarValue::Null);
    }
    Ok(match expected {
        DataType::Null => ScalarValue::Null,
        DataType::Boolean => ScalarValue::Boolean(
            array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or("expected BooleanArray")?
                .value(i),
        ),
        DataType::Int32 => ScalarValue::Int32(
            array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or("expected Int32Array")?
                .value(i),
        ),
        DataType::Int64 => ScalarValue::Int64(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or("expected Int64Array")?
                .value(i),
        ),
        DataType::Float32 => ScalarValue::Float32(
            array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or("expected Float32Array")?
                .value(i),
        ),
        DataType::Float64 => ScalarValue::Float64(
            array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or("expected Float64Array")?
                .value(i),
        ),
        DataType::Utf8 | DataType::LargeUtf8 => ScalarValue::Utf8(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or("expected StringArray")?
                .value(i)
                .to_string(),
        ),
        DataType::Binary => ScalarValue::Binary(
            array
                .as_any()
                .downcast_ref::<arrow::array::BinaryArray>()
                .ok_or("expected BinaryArray")?
                .value(i)
                .to_vec(),
        ),
        DataType::Date32 => ScalarValue::Date32(
            array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or("expected Date32Array")?
                .value(i),
        ),
        DataType::Decimal128 { precision, scale } => {
            let a = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or("expected Decimal128Array")?;
            ScalarValue::Decimal128 {
                value: a.value(i),
                precision: *precision,
                scale: *scale,
            }
        }
        DataType::Timestamp { unit, timezone } => {
            let value = match unit {
                TimeUnit::Second => array
                    .as_any()
                    .downcast_ref::<TimestampSecondArray>()
                    .ok_or("expected TimestampSecondArray")?
                    .value(i),
                TimeUnit::Millisecond => array
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .ok_or("expected TimestampMillisecondArray")?
                    .value(i),
                TimeUnit::Microsecond => array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .ok_or("expected TimestampMicrosecondArray")?
                    .value(i),
                TimeUnit::Nanosecond => array
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .ok_or("expected TimestampNanosecondArray")?
                    .value(i),
            };
            ScalarValue::Timestamp {
                value,
                unit: *unit,
                timezone: timezone.clone(),
            }
        }
        other => {
            return Err(format!(
                "cast target {other} not supported by planner-time folding"
            ))
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cast_utf8_to_date32_folds_to_days_since_epoch() {
        // 1998-12-01 = day 10561 since 1970-01-01.
        let v = ScalarValue::Utf8("1998-12-01".to_string());
        let got = cast_scalar(&v, &DataType::Date32).unwrap();
        assert!(matches!(got, ScalarValue::Date32(10561)), "got: {got:?}");
    }

    #[test]
    fn cast_invalid_utf8_date_returns_plan_error() {
        let v = ScalarValue::Utf8("not-a-date".to_string());
        let err = cast_scalar(&v, &DataType::Date32).unwrap_err();
        assert!(matches!(err, PlanError::InvalidLiteral { .. }));
    }

    #[test]
    fn cast_is_idempotent_when_types_match() {
        let v = ScalarValue::Int64(42);
        let got = cast_scalar(&v, &DataType::Int64).unwrap();
        assert_eq!(got, v);
    }

    #[test]
    fn cast_int32_to_int64_widens() {
        let v = ScalarValue::Int32(7);
        let got = cast_scalar(&v, &DataType::Int64).unwrap();
        assert_eq!(got, ScalarValue::Int64(7));
    }

    #[test]
    fn cast_utf8_to_timestamp_microsecond() {
        let v = ScalarValue::Utf8("2025-01-02 03:04:05".to_string());
        let got = cast_scalar(
            &v,
            &DataType::Timestamp {
                unit: TimeUnit::Microsecond,
                timezone: None,
            },
        )
        .unwrap();
        assert!(matches!(
            got,
            ScalarValue::Timestamp {
                unit: TimeUnit::Microsecond,
                timezone: None,
                ..
            }
        ));
    }

    #[test]
    fn cast_invalid_timestamp_errors() {
        let v = ScalarValue::Utf8("not-a-timestamp".to_string());
        let err = cast_scalar(
            &v,
            &DataType::Timestamp {
                unit: TimeUnit::Microsecond,
                timezone: None,
            },
        )
        .unwrap_err();
        assert!(matches!(err, PlanError::InvalidLiteral { .. }));
    }
}
