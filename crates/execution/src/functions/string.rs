//! Built-in string scalar functions.

use std::sync::Arc;

use arneb_common::error::ExecutionError;
use arneb_common::types::DataType;
use arrow::array::{as_string_array, Array, ArrayRef, Int64Array, StringArray};

use super::registry::ScalarFunction;

/// Return all built-in string functions.
pub(crate) fn all_string_functions() -> Vec<Arc<dyn ScalarFunction>> {
    vec![
        Arc::new(UpperFunction),
        Arc::new(LowerFunction),
        Arc::new(SubstringFunction),
        Arc::new(TrimFunction),
        Arc::new(LtrimFunction),
        Arc::new(RtrimFunction),
        Arc::new(ConcatFunction),
        Arc::new(LengthFunction),
        Arc::new(ReplaceFunction),
        Arc::new(PositionFunction),
    ]
}

fn require_string_arg(args: &[ArrayRef], idx: usize, fn_name: &str) -> Result<(), ExecutionError> {
    if idx >= args.len() {
        return Err(ExecutionError::InvalidOperation(format!(
            "{fn_name}: missing argument {idx}"
        )));
    }
    Ok(())
}

// -- UPPER --

#[derive(Debug)]
struct UpperFunction;

impl ScalarFunction for UpperFunction {
    fn name(&self) -> &str {
        "UPPER"
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(DataType::Utf8)
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        require_string_arg(args, 0, "UPPER")?;
        let arr = as_string_array(&args[0]);
        let result: StringArray = arr.iter().map(|v| v.map(|s| s.to_uppercase())).collect();
        Ok(Arc::new(result))
    }
}

// -- LOWER --

#[derive(Debug)]
struct LowerFunction;

impl ScalarFunction for LowerFunction {
    fn name(&self) -> &str {
        "LOWER"
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(DataType::Utf8)
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        require_string_arg(args, 0, "LOWER")?;
        let arr = as_string_array(&args[0]);
        let result: StringArray = arr.iter().map(|v| v.map(|s| s.to_lowercase())).collect();
        Ok(Arc::new(result))
    }
}

// -- SUBSTRING --

#[derive(Debug)]
struct SubstringFunction;

impl ScalarFunction for SubstringFunction {
    fn name(&self) -> &str {
        "SUBSTRING"
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(DataType::Utf8)
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        // SUBSTRING(str, start [, length])
        if args.len() < 2 {
            return Err(ExecutionError::InvalidOperation(
                "SUBSTRING requires at least 2 arguments".to_string(),
            ));
        }
        let str_arr = as_string_array(&args[0]);
        let start_arr = args[1]
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                ExecutionError::InvalidOperation("SUBSTRING start must be integer".to_string())
            })?;

        let len_arr = if args.len() > 2 {
            Some(
                args[2]
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| {
                        ExecutionError::InvalidOperation(
                            "SUBSTRING length must be integer".to_string(),
                        )
                    })?,
            )
        } else {
            None
        };

        let result: StringArray = (0..str_arr.len())
            .map(|i| {
                if str_arr.is_null(i) || start_arr.is_null(i) {
                    return None;
                }
                let s = str_arr.value(i);
                // SQL SUBSTRING is 1-based
                let start = (start_arr.value(i) - 1).max(0) as usize;
                let chars: Vec<char> = s.chars().collect();
                if start >= chars.len() {
                    return Some(String::new());
                }
                let end = if let Some(la) = len_arr {
                    if la.is_null(i) {
                        return None;
                    }
                    (start + la.value(i).max(0) as usize).min(chars.len())
                } else {
                    chars.len()
                };
                Some(chars[start..end].iter().collect())
            })
            .collect();
        Ok(Arc::new(result))
    }
}

// -- TRIM / LTRIM / RTRIM --

#[derive(Debug)]
struct TrimFunction;

impl ScalarFunction for TrimFunction {
    fn name(&self) -> &str {
        "TRIM"
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(DataType::Utf8)
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        require_string_arg(args, 0, "TRIM")?;
        let arr = as_string_array(&args[0]);
        let result: StringArray = arr
            .iter()
            .map(|v| v.map(|s| s.trim().to_string()))
            .collect();
        Ok(Arc::new(result))
    }
}

#[derive(Debug)]
struct LtrimFunction;

impl ScalarFunction for LtrimFunction {
    fn name(&self) -> &str {
        "LTRIM"
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(DataType::Utf8)
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        require_string_arg(args, 0, "LTRIM")?;
        let arr = as_string_array(&args[0]);
        let result: StringArray = arr
            .iter()
            .map(|v| v.map(|s| s.trim_start().to_string()))
            .collect();
        Ok(Arc::new(result))
    }
}

#[derive(Debug)]
struct RtrimFunction;

impl ScalarFunction for RtrimFunction {
    fn name(&self) -> &str {
        "RTRIM"
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(DataType::Utf8)
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        require_string_arg(args, 0, "RTRIM")?;
        let arr = as_string_array(&args[0]);
        let result: StringArray = arr
            .iter()
            .map(|v| v.map(|s| s.trim_end().to_string()))
            .collect();
        Ok(Arc::new(result))
    }
}

// -- CONCAT --

#[derive(Debug)]
struct ConcatFunction;

impl ScalarFunction for ConcatFunction {
    fn name(&self) -> &str {
        "CONCAT"
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(DataType::Utf8)
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        if args.is_empty() {
            return Err(ExecutionError::InvalidOperation(
                "CONCAT requires at least 1 argument".to_string(),
            ));
        }
        let len = args[0].len();
        let str_arrays: Vec<&StringArray> = args.iter().map(|a| as_string_array(a)).collect();

        let result: StringArray = (0..len)
            .map(|i| {
                let mut buf = String::new();
                let mut any_null = false;
                for arr in &str_arrays {
                    if arr.is_null(i) {
                        any_null = true;
                        break;
                    }
                    buf.push_str(arr.value(i));
                }
                if any_null {
                    None
                } else {
                    Some(buf)
                }
            })
            .collect();
        Ok(Arc::new(result))
    }
}

// -- LENGTH --

#[derive(Debug)]
struct LengthFunction;

impl ScalarFunction for LengthFunction {
    fn name(&self) -> &str {
        "LENGTH"
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(DataType::Int64)
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        require_string_arg(args, 0, "LENGTH")?;
        let arr = as_string_array(&args[0]);
        let result: Int64Array = arr
            .iter()
            .map(|v| v.map(|s| s.chars().count() as i64))
            .collect();
        Ok(Arc::new(result))
    }
}

// -- REPLACE --

#[derive(Debug)]
struct ReplaceFunction;

impl ScalarFunction for ReplaceFunction {
    fn name(&self) -> &str {
        "REPLACE"
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(DataType::Utf8)
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        // REPLACE(str, from, to)
        if args.len() < 3 {
            return Err(ExecutionError::InvalidOperation(
                "REPLACE requires 3 arguments".to_string(),
            ));
        }
        let str_arr = as_string_array(&args[0]);
        let from_arr = as_string_array(&args[1]);
        let to_arr = as_string_array(&args[2]);
        let result: StringArray = (0..str_arr.len())
            .map(|i| {
                if str_arr.is_null(i) || from_arr.is_null(i) || to_arr.is_null(i) {
                    None
                } else {
                    Some(str_arr.value(i).replace(from_arr.value(i), to_arr.value(i)))
                }
            })
            .collect();
        Ok(Arc::new(result))
    }
}

// -- POSITION --

#[derive(Debug)]
struct PositionFunction;

impl ScalarFunction for PositionFunction {
    fn name(&self) -> &str {
        "POSITION"
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(DataType::Int64)
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        // POSITION(substr, str) — returns 1-based position or 0 if not found
        if args.len() < 2 {
            return Err(ExecutionError::InvalidOperation(
                "POSITION requires 2 arguments".to_string(),
            ));
        }
        let substr_arr = as_string_array(&args[0]);
        let str_arr = as_string_array(&args[1]);
        let result: Int64Array = (0..str_arr.len())
            .map(|i| {
                if str_arr.is_null(i) || substr_arr.is_null(i) {
                    None
                } else {
                    let s = str_arr.value(i);
                    let sub = substr_arr.value(i);
                    Some(match s.find(sub) {
                        Some(pos) => {
                            // Convert byte offset to char position (1-based)
                            s[..pos].chars().count() as i64 + 1
                        }
                        None => 0,
                    })
                }
            })
            .collect();
        Ok(Arc::new(result))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_string_array(vals: &[Option<&str>]) -> ArrayRef {
        Arc::new(StringArray::from(vals.to_vec()))
    }

    fn make_int64_array(vals: &[Option<i64>]) -> ArrayRef {
        Arc::new(Int64Array::from(vals.to_vec()))
    }

    #[test]
    fn test_upper() {
        let f = UpperFunction;
        let result = f
            .evaluate(&[make_string_array(&[Some("hello"), None, Some("World")])])
            .unwrap();
        let arr = as_string_array(&result);
        assert_eq!(arr.value(0), "HELLO");
        assert!(arr.is_null(1));
        assert_eq!(arr.value(2), "WORLD");
    }

    #[test]
    fn test_lower() {
        let f = LowerFunction;
        let result = f
            .evaluate(&[make_string_array(&[Some("HELLO"), Some("")])])
            .unwrap();
        let arr = as_string_array(&result);
        assert_eq!(arr.value(0), "hello");
        assert_eq!(arr.value(1), "");
    }

    #[test]
    fn test_substring() {
        let f = SubstringFunction;
        let result = f
            .evaluate(&[
                make_string_array(&[Some("hello"), Some("world"), None]),
                make_int64_array(&[Some(2), Some(1), Some(1)]),
                make_int64_array(&[Some(3), Some(5), Some(1)]),
            ])
            .unwrap();
        let arr = as_string_array(&result);
        assert_eq!(arr.value(0), "ell"); // SUBSTRING('hello', 2, 3)
        assert_eq!(arr.value(1), "world"); // SUBSTRING('world', 1, 5)
        assert!(arr.is_null(2));
    }

    #[test]
    fn test_trim() {
        let f = TrimFunction;
        let result = f
            .evaluate(&[make_string_array(&[Some("  hello  "), Some(""), None])])
            .unwrap();
        let arr = as_string_array(&result);
        assert_eq!(arr.value(0), "hello");
        assert_eq!(arr.value(1), "");
        assert!(arr.is_null(2));
    }

    #[test]
    fn test_concat() {
        let f = ConcatFunction;
        let result = f
            .evaluate(&[
                make_string_array(&[Some("hello"), Some("a"), None]),
                make_string_array(&[Some(" world"), Some("b"), Some("c")]),
            ])
            .unwrap();
        let arr = as_string_array(&result);
        assert_eq!(arr.value(0), "hello world");
        assert_eq!(arr.value(1), "ab");
        assert!(arr.is_null(2));
    }

    #[test]
    fn test_length() {
        let f = LengthFunction;
        let result = f
            .evaluate(&[make_string_array(&[Some("hello"), Some(""), None])])
            .unwrap();
        let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(arr.value(0), 5);
        assert_eq!(arr.value(1), 0);
        assert!(arr.is_null(2));
    }

    #[test]
    fn test_replace() {
        let f = ReplaceFunction;
        let result = f
            .evaluate(&[
                make_string_array(&[Some("hello world"), Some("aaa")]),
                make_string_array(&[Some("world"), Some("a")]),
                make_string_array(&[Some("rust"), Some("b")]),
            ])
            .unwrap();
        let arr = as_string_array(&result);
        assert_eq!(arr.value(0), "hello rust");
        assert_eq!(arr.value(1), "bbb");
    }

    #[test]
    fn test_position() {
        let f = PositionFunction;
        let result = f
            .evaluate(&[
                make_string_array(&[Some("lo"), Some("xyz"), None]),
                make_string_array(&[Some("hello"), Some("hello"), Some("hello")]),
            ])
            .unwrap();
        let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(arr.value(0), 4); // "lo" in "hello" at position 4
        assert_eq!(arr.value(1), 0); // "xyz" not found
        assert!(arr.is_null(2));
    }
}
