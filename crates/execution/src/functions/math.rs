//! Built-in math scalar functions.

use std::sync::Arc;

use arneb_common::error::ExecutionError;
use arneb_common::types::DataType;
use arrow::array::{Array, ArrayRef, BooleanArray, Float64Array, Int64Array};
use arrow::datatypes::{DataType as ArrowDataType, Float64Type, Int64Type};

use super::args::{check_arity, float64, int64, invalid};
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
        // Trino batch 1 (trino-functions-batch1)
        unary_f64("SQRT", f64::sqrt),
        unary_f64("CBRT", f64::cbrt),
        unary_f64("EXP", f64::exp),
        unary_f64("LN", f64::ln),
        unary_f64("LOG2", f64::log2),
        unary_f64("LOG10", f64::log10),
        unary_f64("DEGREES", f64::to_degrees),
        unary_f64("RADIANS", f64::to_radians),
        unary_f64("SIN", f64::sin),
        unary_f64("COS", f64::cos),
        unary_f64("TAN", f64::tan),
        unary_f64("ASIN", f64::asin),
        unary_f64("ACOS", f64::acos),
        unary_f64("ATAN", f64::atan),
        unary_f64("SINH", f64::sinh),
        unary_f64("COSH", f64::cosh),
        unary_f64("TANH", f64::tanh),
        // log(b, x) = ln(x) / ln(b)
        binary_f64("LOG", |b, x| x.ln() / b.ln()),
        binary_f64("ATAN2", f64::atan2),
        constant_f64("PI", std::f64::consts::PI),
        constant_f64("E", std::f64::consts::E),
        constant_f64("NAN", f64::NAN),
        constant_f64("INFINITY", f64::INFINITY),
        f64_predicate("IS_NAN", f64::is_nan),
        f64_predicate("IS_FINITE", f64::is_finite),
        f64_predicate("IS_INFINITE", f64::is_infinite),
        Arc::new(SignFunction),
        Arc::new(TruncateFunction),
        Arc::new(RandomFunction),
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

fn is_integer_arrow(dt: &ArrowDataType) -> bool {
    matches!(
        dt,
        ArrowDataType::Int8
            | ArrowDataType::Int16
            | ArrowDataType::Int32
            | ArrowDataType::Int64
            | ArrowDataType::UInt8
            | ArrowDataType::UInt16
            | ArrowDataType::UInt32
            | ArrowDataType::UInt64
    )
}

fn is_integer_type(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    )
}

// -- Unary DOUBLE -> DOUBLE (sqrt, ln, sin, ...) --

/// `name(x) -> double`. The argument is cast to DOUBLE (integers and
/// decimals included) and the function is applied with Arrow's
/// vectorised `unary` kernel; NULL in → NULL out. Domain errors follow
/// IEEE-754 exactly like Trino (which delegates to `java.lang.Math`):
/// `sqrt(-1)` and `ln(-1)` are NaN, `ln(0)` is -Infinity.
#[derive(Debug)]
struct UnaryF64Function {
    name: &'static str,
    f: fn(f64) -> f64,
}

fn unary_f64(name: &'static str, f: fn(f64) -> f64) -> Arc<dyn ScalarFunction> {
    Arc::new(UnaryF64Function { name, f })
}

impl ScalarFunction for UnaryF64Function {
    fn name(&self) -> &str {
        self.name
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(DataType::Float64)
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        check_arity(self.name, args, 1, 1)?;
        let x = float64(&args[0], self.name)?;
        Ok(Arc::new(x.unary::<_, Float64Type>(self.f)))
    }
}

// -- Binary DOUBLE x DOUBLE -> DOUBLE (log(b, x), atan2) --

#[derive(Debug)]
struct BinaryF64Function {
    name: &'static str,
    f: fn(f64, f64) -> f64,
}

fn binary_f64(name: &'static str, f: fn(f64, f64) -> f64) -> Arc<dyn ScalarFunction> {
    Arc::new(BinaryF64Function { name, f })
}

impl ScalarFunction for BinaryF64Function {
    fn name(&self) -> &str {
        self.name
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(DataType::Float64)
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        check_arity(self.name, args, 2, 2)?;
        let a = float64(&args[0], self.name)?;
        let b = float64(&args[1], self.name)?;
        let out: Float64Array = arrow::compute::binary(&a, &b, self.f)?;
        Ok(Arc::new(out))
    }
}

// -- Nullary constants (pi(), e(), nan(), infinity()) --

#[derive(Debug)]
struct ConstantF64Function {
    name: &'static str,
    value: f64,
}

fn constant_f64(name: &'static str, value: f64) -> Arc<dyn ScalarFunction> {
    Arc::new(ConstantF64Function { name, value })
}

impl ScalarFunction for ConstantF64Function {
    fn name(&self) -> &str {
        self.name
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(DataType::Float64)
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        self.invoke(args, 1)
    }
    fn invoke(&self, args: &[ArrayRef], num_rows: usize) -> Result<ArrayRef, ExecutionError> {
        check_arity(self.name, args, 0, 0)?;
        Ok(Arc::new(Float64Array::from_value(self.value, num_rows)))
    }
}

// -- DOUBLE predicates (is_nan, is_finite, is_infinite) --

#[derive(Debug)]
struct F64PredicateFunction {
    name: &'static str,
    f: fn(f64) -> bool,
}

fn f64_predicate(name: &'static str, f: fn(f64) -> bool) -> Arc<dyn ScalarFunction> {
    Arc::new(F64PredicateFunction { name, f })
}

impl ScalarFunction for F64PredicateFunction {
    fn name(&self) -> &str {
        self.name
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(DataType::Boolean)
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        check_arity(self.name, args, 1, 1)?;
        let x = float64(&args[0], self.name)?;
        Ok(Arc::new(BooleanArray::from_unary(&x, self.f)))
    }
}

// -- SIGN --

/// `sign(x)`: -1, 0 or 1 (NaN for NaN). Integer inputs return BIGINT,
/// everything else returns DOUBLE. Trino returns `decimal(1,0)` for a
/// DECIMAL input; arneb returns DOUBLE there (same numeric value).
#[derive(Debug)]
struct SignFunction;

impl ScalarFunction for SignFunction {
    fn name(&self) -> &str {
        "SIGN"
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(match arg_types.first() {
            Some(t) if is_integer_type(t) => DataType::Int64,
            _ => DataType::Float64,
        })
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        check_arity("SIGN", args, 1, 1)?;
        if is_integer_arrow(args[0].data_type()) {
            let x = int64(&args[0], "SIGN")?;
            return Ok(Arc::new(x.unary::<_, Int64Type>(i64::signum)));
        }
        let x = float64(&args[0], "SIGN")?;
        Ok(Arc::new(x.unary::<_, Float64Type>(|v| {
            if v == 0.0 || v.is_nan() {
                v
            } else {
                v.signum()
            }
        })))
    }
}

// -- TRUNCATE --

/// `truncate(x)` removes the fractional part (rounds toward zero);
/// `truncate(x, n)` keeps `n` decimal places (negative `n` zeroes
/// digits left of the decimal point). Integer inputs return BIGINT,
/// everything else DOUBLE. Trino only defines the 2-argument form for
/// DECIMAL and returns DECIMAL there; arneb returns DOUBLE.
#[derive(Debug)]
struct TruncateFunction;

impl ScalarFunction for TruncateFunction {
    fn name(&self) -> &str {
        "TRUNCATE"
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(match arg_types.first() {
            Some(t) if is_integer_type(t) => DataType::Int64,
            _ => DataType::Float64,
        })
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        check_arity("TRUNCATE", args, 1, 2)?;
        if is_integer_arrow(args[0].data_type()) {
            let x = int64(&args[0], "TRUNCATE")?;
            if args.len() == 1 {
                return Ok(Arc::new(x));
            }
            let n = int64(&args[1], "TRUNCATE")?;
            let out: Int64Array = arrow::compute::binary(&x, &n, |v, n| {
                if n >= 0 {
                    v
                } else {
                    match 10i64.checked_pow(n.unsigned_abs() as u32) {
                        Some(factor) => v / factor * factor,
                        None => 0,
                    }
                }
            })?;
            return Ok(Arc::new(out));
        }
        let x = float64(&args[0], "TRUNCATE")?;
        if args.len() == 1 {
            return Ok(Arc::new(x.unary::<_, Float64Type>(f64::trunc)));
        }
        let n = int64(&args[1], "TRUNCATE")?;
        let out: Float64Array = arrow::compute::binary(&x, &n, |v, n| {
            let factor = 10f64.powi(n as i32);
            (v * factor).trunc() / factor
        })?;
        Ok(Arc::new(out))
    }
}

// -- RANDOM / RAND --

thread_local! {
    static RNG_STATE: std::cell::Cell<u64> = std::cell::Cell::new(seed_rng());
}

fn seed_rng() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    // Mix in a per-thread stack address so threads seeded in the same
    // nanosecond still diverge.
    let local = 0u8;
    nanos ^ (&local as *const u8 as u64).rotate_left(32) ^ 0x9E37_79B9_7F4A_7C15
}

/// SplitMix64 step — small, fast, statistically solid non-crypto PRNG.
fn next_u64() -> u64 {
    RNG_STATE.with(|s| {
        let mut z = s.get().wrapping_add(0x9E37_79B9_7F4A_7C15);
        s.set(z);
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    })
}

/// `random()` / `rand()`: a pseudo-random DOUBLE in `[0.0, 1.0)`, a
/// fresh value per row. `random(n)`: a pseudo-random BIGINT in
/// `[0, n)`; `n <= 0` is an error (as in Trino). Not cryptographically
/// secure. Trino's `random(n)` returns the type of `n`; arneb always
/// returns BIGINT.
#[derive(Debug)]
struct RandomFunction;

impl ScalarFunction for RandomFunction {
    fn name(&self) -> &str {
        "RANDOM"
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(if arg_types.is_empty() {
            DataType::Float64
        } else {
            DataType::Int64
        })
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        let len = args.first().map_or(1, |a| a.len());
        self.invoke(args, len)
    }
    fn invoke(&self, args: &[ArrayRef], num_rows: usize) -> Result<ArrayRef, ExecutionError> {
        check_arity("RANDOM", args, 0, 1)?;
        if args.is_empty() {
            let values: Vec<f64> = (0..num_rows)
                .map(|_| (next_u64() >> 11) as f64 * (1.0 / (1u64 << 53) as f64))
                .collect();
            return Ok(Arc::new(Float64Array::from(values)));
        }
        let bound = int64(&args[0], "RANDOM")?;
        let out: Int64Array = bound.try_unary::<_, Int64Type, ExecutionError>(|n| {
            if n <= 0 {
                return Err(invalid("RANDOM: bound must be positive"));
            }
            Ok((next_u64() % n as u64) as i64)
        })?;
        Ok(Arc::new(out))
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

    // -- Trino batch 1 --

    fn by_name(name: &str) -> Arc<dyn ScalarFunction> {
        all_math_functions()
            .into_iter()
            .find(|f| f.name() == name)
            .unwrap_or_else(|| panic!("no math function {name}"))
    }

    fn f64s(a: &ArrayRef) -> Vec<Option<f64>> {
        a.as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .iter()
            .collect()
    }

    fn close(a: f64, b: f64) -> bool {
        (a - b).abs() < 1e-9
    }

    #[test]
    fn unary_double_functions() {
        let cases: [(&str, f64, f64); 17] = [
            ("SQRT", 16.0, 4.0),
            ("CBRT", 27.0, 3.0),
            ("EXP", 0.0, 1.0),
            ("LN", std::f64::consts::E, 1.0),
            ("LOG2", 8.0, 3.0),
            ("LOG10", 1000.0, 3.0),
            ("DEGREES", std::f64::consts::PI, 180.0),
            ("RADIANS", 180.0, std::f64::consts::PI),
            ("SIN", 0.0, 0.0),
            ("COS", 0.0, 1.0),
            ("TAN", 0.0, 0.0),
            ("ASIN", 1.0, std::f64::consts::FRAC_PI_2),
            ("ACOS", 1.0, 0.0),
            ("ATAN", 0.0, 0.0),
            ("SINH", 0.0, 0.0),
            ("COSH", 0.0, 1.0),
            ("TANH", 0.0, 0.0),
        ];
        for (name, input, expected) in cases {
            let out = by_name(name)
                .evaluate(&[make_f64(&[Some(input), None])])
                .unwrap();
            let out = f64s(&out);
            assert!(close(out[0].unwrap(), expected), "{name}({input})");
            assert_eq!(out[1], None, "{name}(NULL)");
        }
    }

    #[test]
    fn unary_double_accepts_integers_and_follows_ieee_domain() {
        let out = by_name("SQRT")
            .evaluate(&[Arc::new(Int32Array::from(vec![9, -1]))])
            .unwrap();
        let out = f64s(&out);
        assert_eq!(out[0], Some(3.0));
        assert!(out[1].unwrap().is_nan(), "sqrt(-1) is NaN like Trino");
        let out = by_name("LN").evaluate(&[make_f64(&[Some(0.0)])]).unwrap();
        assert_eq!(f64s(&out)[0], Some(f64::NEG_INFINITY));
    }

    #[test]
    fn log_with_base_and_atan2() {
        let out = by_name("LOG")
            .evaluate(&[
                make_f64(&[Some(2.0), None]),
                make_f64(&[Some(8.0), Some(1.0)]),
            ])
            .unwrap();
        let out = f64s(&out);
        assert!(close(out[0].unwrap(), 3.0));
        assert_eq!(out[1], None);
        let out = by_name("ATAN2")
            .evaluate(&[make_f64(&[Some(1.0)]), make_f64(&[Some(1.0)])])
            .unwrap();
        assert!(close(f64s(&out)[0].unwrap(), std::f64::consts::FRAC_PI_4));
    }

    #[test]
    fn constants_fill_the_batch() {
        for (name, v) in [("PI", std::f64::consts::PI), ("E", std::f64::consts::E)] {
            let out = by_name(name).invoke(&[], 3).unwrap();
            assert_eq!(f64s(&out), vec![Some(v); 3], "{name}");
        }
        assert!(f64s(&by_name("NAN").invoke(&[], 1).unwrap())[0]
            .unwrap()
            .is_nan());
        assert_eq!(
            f64s(&by_name("INFINITY").invoke(&[], 1).unwrap())[0],
            Some(f64::INFINITY)
        );
    }

    #[test]
    fn float_predicates() {
        let input = make_f64(&[Some(1.0), Some(f64::NAN), Some(f64::INFINITY), None]);
        let check = |name: &str| -> Vec<Option<bool>> {
            by_name(name)
                .evaluate(std::slice::from_ref(&input))
                .unwrap()
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .iter()
                .collect()
        };
        assert_eq!(
            check("IS_NAN"),
            vec![Some(false), Some(true), Some(false), None]
        );
        assert_eq!(
            check("IS_FINITE"),
            vec![Some(true), Some(false), Some(false), None]
        );
        assert_eq!(
            check("IS_INFINITE"),
            vec![Some(false), Some(false), Some(true), None]
        );
    }

    #[test]
    fn sign_keeps_integers_integral() {
        let out = SignFunction
            .evaluate(&[make_i64(&[Some(-5), Some(0), Some(7), None])])
            .unwrap();
        let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(
            out.iter().collect::<Vec<_>>(),
            vec![Some(-1), Some(0), Some(1), None]
        );
        let out = SignFunction
            .evaluate(&[make_f64(&[Some(-2.5), Some(0.0), Some(f64::NAN)])])
            .unwrap();
        let out = f64s(&out);
        assert_eq!(out[0], Some(-1.0));
        assert_eq!(out[1], Some(0.0), "sign(0.0) is 0, not 1");
        assert!(out[2].unwrap().is_nan());
    }

    #[test]
    fn truncate_semantics() {
        let out = TruncateFunction
            .evaluate(&[make_f64(&[Some(-3.7), Some(3.7), None])])
            .unwrap();
        assert_eq!(f64s(&out), vec![Some(-3.0), Some(3.0), None]);
        let out = TruncateFunction
            .evaluate(&[make_f64(&[Some(1.23456)]), make_i64(&[Some(2)])])
            .unwrap();
        assert!(close(f64s(&out)[0].unwrap(), 1.23));
        let out = TruncateFunction
            .evaluate(&[make_i64(&[Some(1234)]), make_i64(&[Some(-2)])])
            .unwrap();
        let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(out.value(0), 1200);
    }

    #[test]
    fn random_ranges() {
        let out = RandomFunction.invoke(&[], 1000).unwrap();
        let vals = f64s(&out);
        assert_eq!(vals.len(), 1000);
        assert!(vals.iter().all(|v| (0.0..1.0).contains(&v.unwrap())));
        let distinct: std::collections::HashSet<u64> =
            vals.iter().map(|v| v.unwrap().to_bits()).collect();
        assert!(distinct.len() > 990, "values should differ per row");

        let out = RandomFunction
            .invoke(&[make_i64(&[Some(10); 500])], 500)
            .unwrap();
        let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
        assert!(out.iter().all(|v| (0..10).contains(&v.unwrap())));
        assert!(RandomFunction.invoke(&[make_i64(&[Some(0)])], 1).is_err());
    }
}
