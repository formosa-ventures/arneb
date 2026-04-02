//! Built-in date scalar functions.

use std::sync::Arc;

use arneb_common::error::ExecutionError;
use arneb_common::types::DataType;
use arrow::array::{as_string_array, Array, ArrayRef, Date32Array, Int64Array};

use super::registry::ScalarFunction;

/// Return all built-in date functions.
pub(crate) fn all_date_functions() -> Vec<Arc<dyn ScalarFunction>> {
    vec![
        Arc::new(ExtractFunction),
        Arc::new(CurrentDateFunction),
        Arc::new(DateTruncFunction),
    ]
}

/// Convert days-since-epoch (Date32) to (year, month, day).
///
/// Uses Howard Hinnant's `civil_from_days` algorithm.
/// Input: days since 1970-01-01 (Unix epoch).
fn days_to_ymd(days: i32) -> (i32, u32, u32) {
    // Shift epoch from 1970-01-01 to 0000-03-01
    let z = days + 719468;
    let era = (if z >= 0 { z } else { z - 146096 }) / 146097;
    let doe = (z - era * 146097) as u32; // day of era [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365; // year of era [0, 399]
    let y = yoe as i32 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // day of year [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    let year = if m <= 2 { y + 1 } else { y };
    (year, m, d)
}

/// Convert (year, month, day) to days-since-epoch (Date32).
///
/// Uses Howard Hinnant's `days_from_civil` algorithm.
/// Output: days since 1970-01-01 (Unix epoch).
fn ymd_to_days(year: i32, month: u32, day: u32) -> i32 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = (if y >= 0 { y } else { y - 399 }) / 400;
    let yoe = (y - era * 400) as u32; // [0, 399]
    let doy = (153 * (if month > 2 { month - 3 } else { month + 9 }) + 2) / 5 + day - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    era * 146097 + doe as i32 - 719468
}

// -- EXTRACT --

#[derive(Debug)]
struct ExtractFunction;

impl ScalarFunction for ExtractFunction {
    fn name(&self) -> &str {
        "EXTRACT"
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(DataType::Int64)
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        // EXTRACT(field, date) — field is a Utf8 ('YEAR', 'MONTH', 'DAY'), date is Date32
        if args.len() < 2 {
            return Err(ExecutionError::InvalidOperation(
                "EXTRACT requires 2 arguments (field, date)".to_string(),
            ));
        }
        let field_arr = as_string_array(&args[0]);
        let date_arr = args[1]
            .as_any()
            .downcast_ref::<Date32Array>()
            .ok_or_else(|| {
                ExecutionError::InvalidOperation("EXTRACT requires Date32 argument".to_string())
            })?;

        let result: Int64Array = (0..date_arr.len())
            .map(|i| {
                if date_arr.is_null(i) || field_arr.is_null(i) {
                    return None;
                }
                let days = date_arr.value(i);
                let (year, month, day) = days_to_ymd(days);
                let field = field_arr.value(i).to_uppercase();
                match field.as_str() {
                    "YEAR" => Some(year as i64),
                    "MONTH" => Some(month as i64),
                    "DAY" => Some(day as i64),
                    _ => None,
                }
            })
            .collect();
        Ok(Arc::new(result))
    }
}

// -- CURRENT_DATE --

#[derive(Debug)]
struct CurrentDateFunction;

impl ScalarFunction for CurrentDateFunction {
    fn name(&self) -> &str {
        "CURRENT_DATE"
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(DataType::Date32)
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        // CURRENT_DATE takes no args but needs to know array length
        // We use the first argument's length if provided, otherwise 1
        let len = if args.is_empty() { 1 } else { args[0].len() };

        // Calculate today's date as days since epoch
        use std::time::{SystemTime, UNIX_EPOCH};
        let secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let days = (secs / 86400) as i32;

        let result = Date32Array::from(vec![days; len]);
        Ok(Arc::new(result))
    }
}

// -- DATE_TRUNC --

#[derive(Debug)]
struct DateTruncFunction;

impl ScalarFunction for DateTruncFunction {
    fn name(&self) -> &str {
        "DATE_TRUNC"
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(DataType::Date32)
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        // DATE_TRUNC(field, date) — truncate to 'year', 'month', 'day'
        if args.len() < 2 {
            return Err(ExecutionError::InvalidOperation(
                "DATE_TRUNC requires 2 arguments (field, date)".to_string(),
            ));
        }
        let field_arr = as_string_array(&args[0]);
        let date_arr = args[1]
            .as_any()
            .downcast_ref::<Date32Array>()
            .ok_or_else(|| {
                ExecutionError::InvalidOperation("DATE_TRUNC requires Date32 argument".to_string())
            })?;

        let result: Date32Array = (0..date_arr.len())
            .map(|i| {
                if date_arr.is_null(i) || field_arr.is_null(i) {
                    return None;
                }
                let days = date_arr.value(i);
                let (year, month, day) = days_to_ymd(days);
                let field = field_arr.value(i).to_uppercase();
                let truncated = match field.as_str() {
                    "YEAR" => ymd_to_days(year, 1, 1),
                    "MONTH" => ymd_to_days(year, month, 1),
                    "DAY" => ymd_to_days(year, month, day),
                    _ => return None,
                };
                Some(truncated)
            })
            .collect();
        Ok(Arc::new(result))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_date_array(days: &[Option<i32>]) -> ArrayRef {
        Arc::new(Date32Array::from(days.to_vec()))
    }

    fn make_string_array(vals: &[Option<&str>]) -> ArrayRef {
        Arc::new(arrow::array::StringArray::from(vals.to_vec()))
    }

    #[test]
    fn test_days_to_ymd_epoch() {
        let (y, m, d) = days_to_ymd(0);
        assert_eq!((y, m, d), (1970, 1, 1));
    }

    #[test]
    fn test_days_to_ymd_known_date() {
        // 2024-03-15 = 19797 days since epoch
        let days = ymd_to_days(2024, 3, 15);
        let (y, m, d) = days_to_ymd(days);
        assert_eq!((y, m, d), (2024, 3, 15));
    }

    #[test]
    fn test_ymd_roundtrip() {
        for (y, m, d) in [(1970, 1, 1), (2000, 12, 31), (2024, 2, 29), (1969, 6, 15)] {
            let days = ymd_to_days(y, m, d);
            let (y2, m2, d2) = days_to_ymd(days);
            assert_eq!((y, m, d), (y2, m2, d2), "roundtrip failed for {y}-{m}-{d}");
        }
    }

    #[test]
    fn test_extract() {
        let f = ExtractFunction;
        // 2024-01-15
        let date = ymd_to_days(2024, 1, 15);
        let result = f
            .evaluate(&[
                make_string_array(&[Some("YEAR"), Some("MONTH"), Some("DAY")]),
                make_date_array(&[Some(date), Some(date), Some(date)]),
            ])
            .unwrap();
        let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(arr.value(0), 2024);
        assert_eq!(arr.value(1), 1);
        assert_eq!(arr.value(2), 15);
    }

    #[test]
    fn test_extract_null() {
        let f = ExtractFunction;
        let result = f
            .evaluate(&[make_string_array(&[Some("YEAR")]), make_date_array(&[None])])
            .unwrap();
        let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert!(arr.is_null(0));
    }

    #[test]
    fn test_date_trunc_year() {
        let f = DateTruncFunction;
        let date = ymd_to_days(2024, 6, 15);
        let result = f
            .evaluate(&[
                make_string_array(&[Some("year")]),
                make_date_array(&[Some(date)]),
            ])
            .unwrap();
        let arr = result.as_any().downcast_ref::<Date32Array>().unwrap();
        let (y, m, d) = days_to_ymd(arr.value(0));
        assert_eq!((y, m, d), (2024, 1, 1));
    }

    #[test]
    fn test_date_trunc_month() {
        let f = DateTruncFunction;
        let date = ymd_to_days(2024, 6, 15);
        let result = f
            .evaluate(&[
                make_string_array(&[Some("month")]),
                make_date_array(&[Some(date)]),
            ])
            .unwrap();
        let arr = result.as_any().downcast_ref::<Date32Array>().unwrap();
        let (y, m, d) = days_to_ymd(arr.value(0));
        assert_eq!((y, m, d), (2024, 6, 1));
    }
}
