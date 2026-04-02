//! Built-in math scalar functions.

use std::sync::Arc;

use arneb_common::error::ExecutionError;
use arneb_common::types::DataType;
use arrow::array::{Array, ArrayRef, Float64Array, Int64Array};
use arrow::datatypes::DataType as ArrowDataType;

use super::registry::ScalarFunction;

/// Return all built-in math functions.
pub(crate) fn all_math_functions() -> Vec<Arc<dyn ScalarFunction>> {
    vec![
        Arc::new(AbsFunction),
        Arc::new(RoundFunction),
        Arc::new(CeilFunction),
        Arc::new(FloorFunction),
        Arc::new(ModFunction),
        Arc::new(PowerFunction),
    ]
}

fn as_f64_array(arr: &ArrayRef) -> Result<Float64Array, ExecutionError> {
    // Try to cast to Float64 for uniform handling
    match arr.data_type() {
        ArrowDataType::Float64 => Ok(arr.as_any().downcast_ref::<Float64Array>().unwrap().clone()),
        _ => {
            let casted = arrow::compute::cast(arr, &ArrowDataType::Float64).map_err(|e| {
                ExecutionError::InvalidOperation(format!("cannot cast to Float64: {e}"))
            })?;
            Ok(casted
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .clone())
        }
    }
}

// -- ABS --

#[derive(Debug)]
struct AbsFunction;

impl ScalarFunction for AbsFunction {
    fn name(&self) -> &str {
        "ABS"
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        if arg_types.is_empty() {
            return Err(ExecutionError::InvalidOperation(
                "ABS requires 1 argument".to_string(),
            ));
        }
        Ok(arg_types[0].clone())
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        if args.is_empty() {
            return Err(ExecutionError::InvalidOperation(
                "ABS requires 1 argument".to_string(),
            ));
        }
        match args[0].data_type() {
            ArrowDataType::Int64 => {
                let arr = args[0].as_any().downcast_ref::<Int64Array>().unwrap();
                let result: Int64Array = arr.iter().map(|v| v.map(|x| x.abs())).collect();
                Ok(Arc::new(result))
            }
            _ => {
                let arr = as_f64_array(&args[0])?;
                let result: Float64Array = arr.iter().map(|v| v.map(|x| x.abs())).collect();
                Ok(Arc::new(result))
            }
        }
    }
}

// -- ROUND --

#[derive(Debug)]
struct RoundFunction;

impl ScalarFunction for RoundFunction {
    fn name(&self) -> &str {
        "ROUND"
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(DataType::Float64)
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        if args.is_empty() {
            return Err(ExecutionError::InvalidOperation(
                "ROUND requires at least 1 argument".to_string(),
            ));
        }
        let arr = as_f64_array(&args[0])?;
        let decimals = if args.len() > 1 {
            let dec_arr = args[1]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    ExecutionError::InvalidOperation("ROUND decimals must be integer".to_string())
                })?;
            Some(dec_arr)
        } else {
            None
        };

        let result: Float64Array = (0..arr.len())
            .map(|i| {
                if arr.is_null(i) {
                    return None;
                }
                let val = arr.value(i);
                let d = match decimals {
                    Some(da) if !da.is_null(i) => da.value(i),
                    _ => 0,
                };
                let factor = 10f64.powi(d as i32);
                Some((val * factor).round() / factor)
            })
            .collect();
        Ok(Arc::new(result))
    }
}

// -- CEIL --

#[derive(Debug)]
struct CeilFunction;

impl ScalarFunction for CeilFunction {
    fn name(&self) -> &str {
        "CEIL"
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(DataType::Float64)
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        if args.is_empty() {
            return Err(ExecutionError::InvalidOperation(
                "CEIL requires 1 argument".to_string(),
            ));
        }
        let arr = as_f64_array(&args[0])?;
        let result: Float64Array = arr.iter().map(|v| v.map(|x| x.ceil())).collect();
        Ok(Arc::new(result))
    }
}

// -- FLOOR --

#[derive(Debug)]
struct FloorFunction;

impl ScalarFunction for FloorFunction {
    fn name(&self) -> &str {
        "FLOOR"
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(DataType::Float64)
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        if args.is_empty() {
            return Err(ExecutionError::InvalidOperation(
                "FLOOR requires 1 argument".to_string(),
            ));
        }
        let arr = as_f64_array(&args[0])?;
        let result: Float64Array = arr.iter().map(|v| v.map(|x| x.floor())).collect();
        Ok(Arc::new(result))
    }
}

// -- MOD --

#[derive(Debug)]
struct ModFunction;

impl ScalarFunction for ModFunction {
    fn name(&self) -> &str {
        "MOD"
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(DataType::Float64)
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        if args.len() < 2 {
            return Err(ExecutionError::InvalidOperation(
                "MOD requires 2 arguments".to_string(),
            ));
        }
        let a = as_f64_array(&args[0])?;
        let b = as_f64_array(&args[1])?;
        let result: Float64Array = (0..a.len())
            .map(|i| {
                if a.is_null(i) || b.is_null(i) {
                    return None;
                }
                let divisor = b.value(i);
                if divisor == 0.0 {
                    return None; // NULL on division by zero
                }
                Some(a.value(i) % divisor)
            })
            .collect();
        Ok(Arc::new(result))
    }
}

// -- POWER --

#[derive(Debug)]
struct PowerFunction;

impl ScalarFunction for PowerFunction {
    fn name(&self) -> &str {
        "POWER"
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(DataType::Float64)
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        if args.len() < 2 {
            return Err(ExecutionError::InvalidOperation(
                "POWER requires 2 arguments".to_string(),
            ));
        }
        let base = as_f64_array(&args[0])?;
        let exp = as_f64_array(&args[1])?;
        let result: Float64Array = (0..base.len())
            .map(|i| {
                if base.is_null(i) || exp.is_null(i) {
                    None
                } else {
                    Some(base.value(i).powf(exp.value(i)))
                }
            })
            .collect();
        Ok(Arc::new(result))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;

    fn make_f64(vals: &[Option<f64>]) -> ArrayRef {
        Arc::new(Float64Array::from(vals.to_vec()))
    }

    fn make_i64(vals: &[Option<i64>]) -> ArrayRef {
        Arc::new(Int64Array::from(vals.to_vec()))
    }

    #[test]
    fn test_abs_float() {
        let f = AbsFunction;
        let result = f
            .evaluate(&[make_f64(&[Some(-3.5), Some(2.0), None])])
            .unwrap();
        let arr = result.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((arr.value(0) - 3.5).abs() < f64::EPSILON);
        assert!((arr.value(1) - 2.0).abs() < f64::EPSILON);
        assert!(arr.is_null(2));
    }

    #[test]
    fn test_abs_int() {
        let f = AbsFunction;
        let result = f.evaluate(&[make_i64(&[Some(-5), Some(3)])]).unwrap();
        let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(arr.value(0), 5);
        assert_eq!(arr.value(1), 3);
    }

    #[test]
    fn test_abs_int32() {
        let f = AbsFunction;
        let input: ArrayRef = Arc::new(Int32Array::from(vec![Some(-10), Some(20)]));
        let result = f.evaluate(&[input]).unwrap();
        let arr = result.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((arr.value(0) - 10.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_round() {
        let f = RoundFunction;
        let result = f
            .evaluate(&[
                make_f64(&[Some(3.456), Some(2.5), Some(-1.5)]),
                make_i64(&[Some(2), Some(0), Some(0)]),
            ])
            .unwrap();
        let arr = result.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((arr.value(0) - 3.46).abs() < 1e-10);
        assert!((arr.value(1) - 3.0).abs() < f64::EPSILON); // Rust rounds 2.5 to 2.0 (banker's) ... actually round() rounds to nearest even
        assert!((arr.value(2) - -2.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_ceil_floor() {
        let cf = CeilFunction;
        let result = cf.evaluate(&[make_f64(&[Some(2.3), Some(-1.7)])]).unwrap();
        let arr = result.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((arr.value(0) - 3.0).abs() < f64::EPSILON);
        assert!((arr.value(1) - -1.0).abs() < f64::EPSILON);

        let ff = FloorFunction;
        let result = ff.evaluate(&[make_f64(&[Some(2.3), Some(-1.7)])]).unwrap();
        let arr = result.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((arr.value(0) - 2.0).abs() < f64::EPSILON);
        assert!((arr.value(1) - -2.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_mod_function() {
        let f = ModFunction;
        let result = f
            .evaluate(&[
                make_f64(&[Some(10.0), Some(7.0), Some(5.0)]),
                make_f64(&[Some(3.0), Some(0.0), Some(2.0)]),
            ])
            .unwrap();
        let arr = result.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((arr.value(0) - 1.0).abs() < f64::EPSILON);
        assert!(arr.is_null(1)); // div by zero → NULL
        assert!((arr.value(2) - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_power() {
        let f = PowerFunction;
        let result = f
            .evaluate(&[
                make_f64(&[Some(2.0), Some(3.0), None]),
                make_f64(&[Some(3.0), Some(2.0), Some(1.0)]),
            ])
            .unwrap();
        let arr = result.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((arr.value(0) - 8.0).abs() < f64::EPSILON);
        assert!((arr.value(1) - 9.0).abs() < f64::EPSILON);
        assert!(arr.is_null(2));
    }
}
