//! Built-in date / time scalar functions.
//!
//! # Types
//!
//! DATE values are Arrow `Date32` (days since 1970-01-01). TIMESTAMP
//! values produced by these functions are `Timestamp(Microsecond, None)`
//! holding UTC wall-clock time. Timestamp inputs of any unit / zone are
//! accepted and normalised to microseconds.
//!
//! arneb has no session time zone yet, so every function behaves as if
//! the Trino session zone were UTC. In particular `now()` /
//! `current_timestamp` and `from_unixtime()` return a zone-less
//! TIMESTAMP in UTC where Trino returns `TIMESTAMP WITH TIME ZONE` in
//! the session zone.

use std::sync::Arc;

use arneb_common::error::ExecutionError;
use arneb_common::types::{DataType, TimeUnit};
use arrow::array::{
    Array, ArrayRef, Date32Array, Float64Array, Int64Array, StringBuilder,
    TimestampMicrosecondArray,
};
use arrow::compute::kernels::temporal::{date_part, DatePart};
use arrow::datatypes::{DataType as ArrowDataType, Date32Type, Float64Type, Int64Type};
use chrono::format::{Item, Parsed, StrftimeItems};
use chrono::{DateTime, Datelike, Months, NaiveDate, NaiveDateTime};

use super::args::{
    cast_result, check_arity, constant_str, float64, int64, invalid, timestamp_us,
    timestamp_us_type, utf8,
};
use super::registry::ScalarFunction;

/// Return all built-in date functions.
pub(crate) fn all_date_functions() -> Vec<Arc<dyn ScalarFunction>> {
    vec![
        Arc::new(ExtractFunction),
        Arc::new(CurrentDateFunction),
        Arc::new(DateTruncFunction),
        // Trino batch 1 (trino-functions-batch1)
        date_part_fn("YEAR", DatePart::Year),
        date_part_fn("QUARTER", DatePart::Quarter),
        date_part_fn("MONTH", DatePart::Month),
        date_part_fn("WEEK", DatePart::Week),
        date_part_fn("DAY", DatePart::Day),
        date_part_fn("DAY_OF_WEEK", DatePart::DayOfWeekMonday1),
        date_part_fn("DAY_OF_YEAR", DatePart::DayOfYear),
        date_part_fn("YEAR_OF_WEEK", DatePart::YearISO),
        date_part_fn("HOUR", DatePart::Hour),
        date_part_fn("MINUTE", DatePart::Minute),
        date_part_fn("SECOND", DatePart::Second),
        date_part_fn("MILLISECOND", DatePart::Millisecond),
        Arc::new(DateAddFunction),
        Arc::new(DateDiffFunction),
        Arc::new(LastDayOfMonthFunction),
        Arc::new(NowFunction),
        Arc::new(FromUnixtimeFunction),
        Arc::new(ToUnixtimeFunction),
        Arc::new(DateFunction),
        Arc::new(DateFormatFunction),
        Arc::new(DateParseFunction),
        Arc::new(FormatDatetimeFunction),
    ]
}

const MICROS_PER_MILLI: i64 = 1_000;
const MICROS_PER_SECOND: i64 = 1_000_000;
const MICROS_PER_MINUTE: i64 = 60 * MICROS_PER_SECOND;
const MICROS_PER_HOUR: i64 = 60 * MICROS_PER_MINUTE;
const MICROS_PER_DAY: i64 = 24 * MICROS_PER_HOUR;

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

fn micros_to_naive(us: i64) -> Result<NaiveDateTime, ExecutionError> {
    DateTime::from_timestamp_micros(us)
        .map(|d| d.naive_utc())
        .ok_or_else(|| invalid(format!("timestamp out of range: {us} microseconds")))
}

fn naive_to_micros(dt: NaiveDateTime) -> i64 {
    dt.and_utc().timestamp_micros()
}

fn now_micros() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

fn is_date_like(dt: &ArrowDataType) -> bool {
    matches!(
        dt,
        ArrowDataType::Date32
            | ArrowDataType::Utf8
            | ArrowDataType::LargeUtf8
            | ArrowDataType::Null
    )
}

/// Normalise a DATE-ish argument (DATE, TIMESTAMP, VARCHAR, NULL) to
/// `Date32`.
fn date32(arr: &ArrayRef, name: &str) -> Result<Date32Array, ExecutionError> {
    let casted = if arr.data_type() == &ArrowDataType::Date32 {
        arr.clone()
    } else {
        arrow::compute::cast(arr, &ArrowDataType::Date32)
            .map_err(|e| invalid(format!("{name}: cannot convert argument to DATE: {e}")))?
    };
    Ok(casted
        .as_any()
        .downcast_ref::<Date32Array>()
        .expect("cast to Date32 yields Date32Array")
        .clone())
}

/// Temporal field names accepted by `EXTRACT`, `DATE_TRUNC`,
/// `DATE_ADD` and `DATE_DIFF`, normalised to upper case.
fn normalize_unit(unit: &str) -> String {
    let u = unit.trim().to_ascii_uppercase();
    // sqlparser renders `EXTRACT(WEEK FROM x)` as `WEEK(NONE)`.
    if u.starts_with("WEEK(") {
        return "WEEK".to_string();
    }
    u
}

/// Map an `EXTRACT` field to the Arrow date part (Trino semantics).
fn extract_part(field: &str) -> Result<DatePart, ExecutionError> {
    Ok(match normalize_unit(field).as_str() {
        "YEAR" | "YEARS" => DatePart::Year,
        "QUARTER" => DatePart::Quarter,
        "MONTH" | "MONTHS" => DatePart::Month,
        "WEEK" | "WEEKS" | "WEEK_OF_YEAR" => DatePart::Week,
        "DAY" | "DAYS" | "DAY_OF_MONTH" => DatePart::Day,
        // Trino's DOW is ISO: Monday = 1 ... Sunday = 7.
        "DOW" | "DAYOFWEEK" | "DAY_OF_WEEK" => DatePart::DayOfWeekMonday1,
        "DOY" | "DAYOFYEAR" | "DAY_OF_YEAR" => DatePart::DayOfYear,
        "YOW" | "YEAR_OF_WEEK" | "ISOYEAR" => DatePart::YearISO,
        "HOUR" | "HOURS" => DatePart::Hour,
        "MINUTE" | "MINUTES" => DatePart::Minute,
        "SECOND" | "SECONDS" => DatePart::Second,
        "MILLISECOND" | "MILLISECONDS" => DatePart::Millisecond,
        other => return Err(invalid(format!("EXTRACT: unsupported field '{other}'"))),
    })
}

/// Evaluate an Arrow date part over a DATE or TIMESTAMP array as BIGINT.
fn date_part_i64(arr: &ArrayRef, part: DatePart, name: &str) -> Result<ArrayRef, ExecutionError> {
    let input: ArrayRef = match arr.data_type() {
        ArrowDataType::Date32 | ArrowDataType::Timestamp(_, _) => arr.clone(),
        _ => Arc::new(timestamp_us(arr, name)?),
    };
    let parts = date_part(input.as_ref(), part)?;
    Ok(arrow::compute::cast(&parts, &ArrowDataType::Int64)?)
}

// -- EXTRACT --

/// `EXTRACT(field FROM x)` (lowered by the parser to
/// `EXTRACT('FIELD', x)`) → BIGINT. `x` may be a DATE or TIMESTAMP.
///
/// Supported fields: YEAR, QUARTER, MONTH, WEEK, DAY, DAY_OF_MONTH,
/// DOW / DAY_OF_WEEK (ISO, Monday = 1), DOY / DAY_OF_YEAR,
/// YOW / YEAR_OF_WEEK (ISO week-year), HOUR, MINUTE, SECOND,
/// MILLISECOND. Time fields of a DATE are 0.
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
        check_arity("EXTRACT", args, 2, 2)?;
        let fields = utf8(&args[0], "EXTRACT")?;
        if let Some(field) = constant_str(&fields) {
            return date_part_i64(&args[1], extract_part(field)?, "EXTRACT");
        }
        // Per-row fields: evaluate each distinct field once, then pick.
        let mut cache: Vec<(String, Int64Array)> = Vec::new();
        let mut out = Vec::with_capacity(fields.len());
        for i in 0..fields.len() {
            if fields.is_null(i) {
                out.push(None);
                continue;
            }
            let f = fields.value(i);
            let idx = match cache.iter().position(|(k, _)| k == f) {
                Some(idx) => idx,
                None => {
                    let arr = date_part_i64(&args[1], extract_part(f)?, "EXTRACT")?;
                    let arr = arr.as_any().downcast_ref::<Int64Array>().unwrap().clone();
                    cache.push((f.to_string(), arr));
                    cache.len() - 1
                }
            };
            let arr = &cache[idx].1;
            out.push((!arr.is_null(i)).then(|| arr.value(i)));
        }
        Ok(Arc::new(Int64Array::from(out)))
    }
}

// -- YEAR / MONTH / DAY / ... --

/// `year(x)`, `quarter(x)`, `month(x)`, `week(x)` (ISO week),
/// `day(x)`, `day_of_week(x)` (ISO, Monday = 1), `day_of_year(x)`,
/// `year_of_week(x)` (ISO week-year), `hour(x)`, `minute(x)`,
/// `second(x)`, `millisecond(x)` → BIGINT, via Arrow's vectorised
/// `date_part` kernel. `x` is a DATE or TIMESTAMP; the time fields of a
/// DATE are 0 (Trino rejects `hour(date)` at analysis time).
#[derive(Debug)]
struct DatePartFunction {
    name: &'static str,
    part: DatePart,
}

fn date_part_fn(name: &'static str, part: DatePart) -> Arc<dyn ScalarFunction> {
    Arc::new(DatePartFunction { name, part })
}

impl ScalarFunction for DatePartFunction {
    fn name(&self) -> &str {
        self.name
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(DataType::Int64)
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        check_arity(self.name, args, 1, 1)?;
        date_part_i64(&args[0], self.part, self.name)
    }
}

// -- CURRENT_DATE / NOW --

/// `current_date` → DATE (UTC).
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
        let len = args.first().map_or(1, |a| a.len());
        self.invoke(&[], len)
    }
    fn invoke(&self, _args: &[ArrayRef], num_rows: usize) -> Result<ArrayRef, ExecutionError> {
        let days = now_micros().div_euclid(MICROS_PER_DAY) as i32;
        Ok(Arc::new(Date32Array::from_value(days, num_rows)))
    }
}

/// `now()` / `current_timestamp` / `localtimestamp` → TIMESTAMP (UTC,
/// microseconds). The value is read once per batch, so all rows of a
/// batch agree; Trino fixes it once per query.
#[derive(Debug)]
struct NowFunction;

impl ScalarFunction for NowFunction {
    fn name(&self) -> &str {
        "NOW"
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(timestamp_us_dt())
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        let len = args.first().map_or(1, |a| a.len());
        self.invoke(&[], len)
    }
    fn invoke(&self, args: &[ArrayRef], num_rows: usize) -> Result<ArrayRef, ExecutionError> {
        check_arity("NOW", args, 0, 0)?;
        Ok(Arc::new(TimestampMicrosecondArray::from_value(
            now_micros(),
            num_rows,
        )))
    }
}

fn timestamp_us_dt() -> DataType {
    DataType::Timestamp {
        unit: TimeUnit::Microsecond,
        timezone: None,
    }
}

// -- DATE_TRUNC --

/// Truncation / arithmetic units (Trino spelling, case-insensitive).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Unit {
    Millisecond,
    Second,
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Quarter,
    Year,
}

impl Unit {
    fn parse(s: &str, fn_name: &str) -> Result<Unit, ExecutionError> {
        Ok(match normalize_unit(s).as_str() {
            "MILLISECOND" | "MILLISECONDS" => Unit::Millisecond,
            "SECOND" | "SECONDS" => Unit::Second,
            "MINUTE" | "MINUTES" => Unit::Minute,
            "HOUR" | "HOURS" => Unit::Hour,
            "DAY" | "DAYS" => Unit::Day,
            "WEEK" | "WEEKS" => Unit::Week,
            "MONTH" | "MONTHS" => Unit::Month,
            "QUARTER" | "QUARTERS" => Unit::Quarter,
            "YEAR" | "YEARS" => Unit::Year,
            other => return Err(invalid(format!("{fn_name}: unsupported unit '{other}'"))),
        })
    }

    /// Fixed length in microseconds for sub-month units.
    fn fixed_micros(self) -> Option<i64> {
        match self {
            Unit::Millisecond => Some(MICROS_PER_MILLI),
            Unit::Second => Some(MICROS_PER_SECOND),
            Unit::Minute => Some(MICROS_PER_MINUTE),
            Unit::Hour => Some(MICROS_PER_HOUR),
            Unit::Day => Some(MICROS_PER_DAY),
            Unit::Week => Some(7 * MICROS_PER_DAY),
            Unit::Month | Unit::Quarter | Unit::Year => None,
        }
    }

    fn months(self) -> Option<i64> {
        match self {
            Unit::Month => Some(1),
            Unit::Quarter => Some(3),
            Unit::Year => Some(12),
            _ => None,
        }
    }

    fn is_date_unit(self) -> bool {
        matches!(
            self,
            Unit::Day | Unit::Week | Unit::Month | Unit::Quarter | Unit::Year
        )
    }
}

/// Resolve a unit argument that is (almost always) a literal.
fn per_row_units(arr: &ArrayRef, fn_name: &str) -> Result<Vec<Option<Unit>>, ExecutionError> {
    let units = utf8(arr, fn_name)?;
    if let Some(u) = constant_str(&units) {
        return Ok(vec![Some(Unit::parse(u, fn_name)?); units.len()]);
    }
    units
        .iter()
        .map(|u| u.map(|u| Unit::parse(u, fn_name)).transpose())
        .collect()
}

fn trunc_days(days: i32, unit: Unit) -> i32 {
    let (y, m, _) = days_to_ymd(days);
    match unit {
        Unit::Year => ymd_to_days(y, 1, 1),
        Unit::Quarter => ymd_to_days(y, (m - 1) / 3 * 3 + 1, 1),
        Unit::Month => ymd_to_days(y, m, 1),
        // ISO weeks start on Monday; 1970-01-01 was a Thursday.
        Unit::Week => days - (days + 3).rem_euclid(7),
        _ => days,
    }
}

fn trunc_micros(us: i64, unit: Unit) -> i64 {
    match unit {
        Unit::Millisecond | Unit::Second | Unit::Minute | Unit::Hour | Unit::Day => {
            let step = unit.fixed_micros().expect("fixed unit");
            us - us.rem_euclid(step)
        }
        _ => {
            let days = us.div_euclid(MICROS_PER_DAY) as i32;
            trunc_days(days, unit) as i64 * MICROS_PER_DAY
        }
    }
}

/// `date_trunc(unit, x)` → same type as `x`.
///
/// Units: `millisecond`, `second`, `minute`, `hour`, `day`, `week`
/// (ISO, Monday), `month`, `quarter`, `year`. DATE inputs accept only
/// the date units (`day` and above) and return a DATE; TIMESTAMP inputs
/// return a TIMESTAMP of the same unit / zone.
#[derive(Debug)]
struct DateTruncFunction;

impl ScalarFunction for DateTruncFunction {
    fn name(&self) -> &str {
        "DATE_TRUNC"
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(match arg_types.get(1) {
            Some(DataType::Null | DataType::Utf8 | DataType::LargeUtf8) | None => DataType::Date32,
            Some(t) => t.clone(),
        })
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        check_arity("DATE_TRUNC", args, 2, 2)?;
        let units = per_row_units(&args[0], "DATE_TRUNC")?;
        if is_date_like(args[1].data_type()) {
            let d = date32(&args[1], "DATE_TRUNC")?;
            let out: Date32Array = d
                .iter()
                .zip(units)
                .map(|(v, u)| match (v, u) {
                    (Some(v), Some(u)) => {
                        if !u.is_date_unit() {
                            return Err(invalid(format!(
                                "DATE_TRUNC: '{u:?}' is not a valid unit for DATE"
                            )));
                        }
                        Ok(Some(trunc_days(v, u)))
                    }
                    _ => Ok(None),
                })
                .collect::<Result<_, _>>()?;
            return Ok(Arc::new(out));
        }
        let ts = timestamp_us(&args[1], "DATE_TRUNC")?;
        let out: TimestampMicrosecondArray = ts
            .iter()
            .zip(units)
            .map(|(v, u)| Some(trunc_micros(v?, u?)))
            .collect();
        cast_result(Arc::new(out), args[1].data_type())
    }
}

// -- DATE_ADD --

fn add_months_days(days: i32, months: i64) -> Result<i32, ExecutionError> {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
    let d = epoch
        .checked_add_signed(chrono::Duration::days(days as i64))
        .ok_or_else(|| invalid("DATE_ADD: date out of range"))?;
    let shifted = shift_months(d.and_hms_opt(0, 0, 0).expect("midnight"), months)?;
    Ok((shifted.date() - epoch).num_days() as i32)
}

fn shift_months(dt: NaiveDateTime, months: i64) -> Result<NaiveDateTime, ExecutionError> {
    let m = Months::new(
        u32::try_from(months.unsigned_abs()).map_err(|_| invalid("month offset out of range"))?,
    );
    let r = if months >= 0 {
        dt.checked_add_months(m)
    } else {
        dt.checked_sub_months(m)
    };
    r.ok_or_else(|| invalid("date arithmetic out of range"))
}

/// `date_add(unit, value, x)` → same type as `x`.
///
/// Adds `value` units to `x`. Month-based units clamp to the last day
/// of the target month (`date_add('month', 1, DATE '2024-01-31')` is
/// `2024-02-29`), like Trino. DATE inputs accept only date units.
#[derive(Debug)]
struct DateAddFunction;

impl ScalarFunction for DateAddFunction {
    fn name(&self) -> &str {
        "DATE_ADD"
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(match arg_types.get(2) {
            Some(DataType::Null | DataType::Utf8 | DataType::LargeUtf8) | None => timestamp_us_dt(),
            Some(t) => t.clone(),
        })
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        check_arity("DATE_ADD", args, 3, 3)?;
        let units = per_row_units(&args[0], "DATE_ADD")?;
        let values = int64(&args[1], "DATE_ADD")?;
        if args[2].data_type() == &ArrowDataType::Date32 {
            let d = date32(&args[2], "DATE_ADD")?;
            let mut out = Vec::with_capacity(d.len());
            for (i, unit) in units.iter().enumerate() {
                let (Some(u), false, false) = (*unit, d.is_null(i), values.is_null(i)) else {
                    out.push(None);
                    continue;
                };
                let (days, n) = (d.value(i), values.value(i));
                let r = match u {
                    Unit::Day => days as i64 + n,
                    Unit::Week => days as i64 + 7 * n,
                    Unit::Month | Unit::Quarter | Unit::Year => {
                        add_months_days(days, n * u.months().expect("month unit"))? as i64
                    }
                    other => {
                        return Err(invalid(format!(
                            "DATE_ADD: '{other:?}' is not a valid unit for DATE"
                        )))
                    }
                };
                out.push(Some(
                    i32::try_from(r).map_err(|_| invalid("DATE_ADD: date out of range"))?,
                ));
            }
            return Ok(Arc::new(Date32Array::from(out)));
        }
        let ts = timestamp_us(&args[2], "DATE_ADD")?;
        let mut out = Vec::with_capacity(ts.len());
        for (i, unit) in units.iter().enumerate() {
            let (Some(u), false, false) = (*unit, ts.is_null(i), values.is_null(i)) else {
                out.push(None);
                continue;
            };
            let (us, n) = (ts.value(i), values.value(i));
            let r = match (u.fixed_micros(), u.months()) {
                (Some(step), _) => n
                    .checked_mul(step)
                    .and_then(|d| us.checked_add(d))
                    .ok_or_else(|| invalid("DATE_ADD: timestamp out of range"))?,
                (None, Some(m)) => naive_to_micros(shift_months(micros_to_naive(us)?, n * m)?),
                (None, None) => unreachable!("every unit is fixed or month-based"),
            };
            out.push(Some(r));
        }
        let out: ArrayRef = Arc::new(TimestampMicrosecondArray::from(out));
        let target = match args[2].data_type() {
            ArrowDataType::Timestamp(_, _) => args[2].data_type().clone(),
            _ => timestamp_us_type(),
        };
        cast_result(out, &target)
    }
}

// -- DATE_DIFF --

/// Whole months from `a` to `b`, truncated toward zero with Joda's
/// end-of-month rule (`2024-01-31 → 2024-02-29` counts as 1 month),
/// matching Trino's `date_diff`.
fn months_between(a: NaiveDateTime, b: NaiveDateTime) -> Result<i64, ExecutionError> {
    let mut diff =
        (b.year() as i64 * 12 + b.month0() as i64) - (a.year() as i64 * 12 + a.month0() as i64);
    if diff > 0 && shift_months(a, diff)? > b {
        diff -= 1;
    } else if diff < 0 && shift_months(a, diff)? < b {
        diff += 1;
    }
    Ok(diff)
}

/// `date_diff(unit, a, b)` → BIGINT: the number of whole `unit`s from
/// `a` to `b` (negative when `b < a`), truncated toward zero. DATE
/// arguments are treated as midnight UTC.
#[derive(Debug)]
struct DateDiffFunction;

impl ScalarFunction for DateDiffFunction {
    fn name(&self) -> &str {
        "DATE_DIFF"
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(DataType::Int64)
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        check_arity("DATE_DIFF", args, 3, 3)?;
        let units = per_row_units(&args[0], "DATE_DIFF")?;
        let a = timestamp_us(&args[1], "DATE_DIFF")?;
        let b = timestamp_us(&args[2], "DATE_DIFF")?;
        let mut out = Vec::with_capacity(a.len());
        for (i, unit) in units.iter().enumerate() {
            let (Some(u), false, false) = (*unit, a.is_null(i), b.is_null(i)) else {
                out.push(None);
                continue;
            };
            let (x, y) = (a.value(i), b.value(i));
            let r = match (u.fixed_micros(), u.months()) {
                (Some(step), _) => (y as i128 - x as i128) as i64 / step,
                (None, Some(m)) => months_between(micros_to_naive(x)?, micros_to_naive(y)?)? / m,
                (None, None) => unreachable!("every unit is fixed or month-based"),
            };
            out.push(Some(r));
        }
        Ok(Arc::new(Int64Array::from(out)))
    }
}

// -- LAST_DAY_OF_MONTH --

/// `last_day_of_month(x)` → DATE: the last day of the month of `x`.
#[derive(Debug)]
struct LastDayOfMonthFunction;

impl ScalarFunction for LastDayOfMonthFunction {
    fn name(&self) -> &str {
        "LAST_DAY_OF_MONTH"
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(DataType::Date32)
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        check_arity("LAST_DAY_OF_MONTH", args, 1, 1)?;
        let d = date32(&args[0], "LAST_DAY_OF_MONTH")?;
        let out = d.unary::<_, Date32Type>(|days| {
            let (y, m, _) = days_to_ymd(days);
            let (ny, nm) = if m == 12 { (y + 1, 1) } else { (y, m + 1) };
            ymd_to_days(ny, nm, 1) - 1
        });
        Ok(Arc::new(out))
    }
}

// -- FROM_UNIXTIME / TO_UNIXTIME / DATE --

/// `from_unixtime(seconds)` → TIMESTAMP (UTC, microseconds). Fractional
/// seconds are kept to microsecond precision. Trino returns
/// `TIMESTAMP WITH TIME ZONE` in the session zone; see the module docs.
#[derive(Debug)]
struct FromUnixtimeFunction;

impl ScalarFunction for FromUnixtimeFunction {
    fn name(&self) -> &str {
        "FROM_UNIXTIME"
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(timestamp_us_dt())
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        check_arity("FROM_UNIXTIME", args, 1, 1)?;
        let secs = float64(&args[0], "FROM_UNIXTIME")?;
        let micros = secs.unary::<_, Int64Type>(|s| (s * MICROS_PER_SECOND as f64).round() as i64);
        let out = TimestampMicrosecondArray::from(
            micros
                .to_data()
                .into_builder()
                .data_type(timestamp_us_type())
                .build()?,
        );
        Ok(Arc::new(out))
    }
}

/// `to_unixtime(x)` → DOUBLE seconds since the epoch (with fraction).
#[derive(Debug)]
struct ToUnixtimeFunction;

impl ScalarFunction for ToUnixtimeFunction {
    fn name(&self) -> &str {
        "TO_UNIXTIME"
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(DataType::Float64)
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        check_arity("TO_UNIXTIME", args, 1, 1)?;
        let ts = timestamp_us(&args[0], "TO_UNIXTIME")?;
        let out: Float64Array =
            ts.unary::<_, Float64Type>(|us| us as f64 / MICROS_PER_SECOND as f64);
        Ok(Arc::new(out))
    }
}

/// `date(x)` → DATE; equivalent to `CAST(x AS DATE)` (accepts VARCHAR
/// `'YYYY-MM-DD'`, TIMESTAMP, DATE).
#[derive(Debug)]
struct DateFunction;

impl ScalarFunction for DateFunction {
    fn name(&self) -> &str {
        "DATE"
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(DataType::Date32)
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        check_arity("DATE", args, 1, 1)?;
        Ok(Arc::new(date32(&args[0], "DATE")?))
    }
}

// -- DATE_FORMAT / DATE_PARSE (MySQL format specifiers) --

/// Translate a MySQL-style `date_format` / `date_parse` pattern into a
/// chrono strftime pattern. `for_parse` selects the parsing variant of
/// the few specifiers whose formatting and parsing forms differ.
///
/// Supported: `%a %b %c %d %e %f %H %h %I %i %j %k %l %M %m %p %r %S
/// %s %T %v %W %x %Y %y %%`, plus `%D` (formatting only). Trino rejects
/// `%U %u %V %w %X`; arneb reports them as unsupported too.
fn mysql_to_strftime(fmt: &str, for_parse: bool, fn_name: &str) -> Result<String, ExecutionError> {
    let mut out = String::with_capacity(fmt.len() * 2);
    let mut chars = fmt.chars();
    while let Some(c) = chars.next() {
        if c != '%' {
            out.push(c);
            continue;
        }
        let spec = chars
            .next()
            .ok_or_else(|| invalid(format!("{fn_name}: pattern ends with a lone '%'")))?;
        let mapped = match spec {
            'a' => "%a",
            'b' => "%b",
            'c' => {
                if for_parse {
                    "%m"
                } else {
                    "%-m"
                }
            }
            'd' => "%d",
            'e' => {
                if for_parse {
                    "%d"
                } else {
                    "%-d"
                }
            }
            'f' => "%6f",
            'H' => "%H",
            'h' | 'I' => "%I",
            'i' => "%M",
            'j' => "%j",
            'k' => {
                if for_parse {
                    "%H"
                } else {
                    "%-H"
                }
            }
            'l' => {
                if for_parse {
                    "%I"
                } else {
                    "%-I"
                }
            }
            'M' => "%B",
            'm' => "%m",
            'p' => "%p",
            'r' => "%I:%M:%S %p",
            'S' | 's' => "%S",
            'T' => "%H:%M:%S",
            'v' => "%V",
            'W' => "%A",
            'x' => "%G",
            'Y' => "%Y",
            'y' => "%y",
            '%' => "%%",
            'D' if !for_parse => "\u{0}D", // handled by the renderer
            other => {
                return Err(invalid(format!(
                    "{fn_name}: unsupported format specifier '%{other}'"
                )))
            }
        };
        out.push_str(mapped);
    }
    Ok(out)
}

fn ordinal_suffix(day: u32) -> &'static str {
    match (day % 10, day % 100) {
        (_, 11..=13) => "th",
        (1, _) => "st",
        (2, _) => "nd",
        (3, _) => "rd",
        _ => "th",
    }
}

/// A parsed, reusable output pattern.
struct CompiledFormat {
    /// Chunks of strftime text; `None` marks a `%D` (day with suffix).
    pieces: Vec<Option<Vec<Item<'static>>>>,
}

impl CompiledFormat {
    fn new(strftime: &str, fn_name: &str) -> Result<Self, ExecutionError> {
        let mut pieces = Vec::new();
        for (i, chunk) in strftime.split("\u{0}D").enumerate() {
            if i > 0 {
                pieces.push(None);
            }
            let items: Vec<Item<'static>> = StrftimeItems::new(chunk)
                .map(|it| to_owned_item(it))
                .collect();
            if items.iter().any(|it| matches!(it, Item::Error)) {
                return Err(invalid(format!("{fn_name}: invalid format pattern")));
            }
            pieces.push(Some(items));
        }
        Ok(Self { pieces })
    }

    fn render(&self, dt: &NaiveDateTime, buf: &mut String) {
        use std::fmt::Write;
        for p in &self.pieces {
            match p {
                Some(items) => {
                    let _ = write!(buf, "{}", dt.format_with_items(items.iter()));
                }
                None => {
                    let _ = write!(buf, "{}{}", dt.day(), ordinal_suffix(dt.day()));
                }
            }
        }
    }
}

fn to_owned_item(item: Item<'_>) -> Item<'static> {
    match item {
        Item::Literal(s) => Item::OwnedLiteral(s.into()),
        Item::Space(s) => Item::OwnedSpace(s.into()),
        Item::OwnedLiteral(s) => Item::OwnedLiteral(s),
        Item::OwnedSpace(s) => Item::OwnedSpace(s),
        Item::Numeric(n, p) => Item::Numeric(n, p),
        Item::Fixed(f) => Item::Fixed(f),
        Item::Error => Item::Error,
    }
}

/// Shared driver for `date_format` / `format_datetime`: format each
/// timestamp with a per-pattern compiled format (compiled once while
/// consecutive rows share a pattern).
fn format_timestamps(
    args: &[ArrayRef],
    fn_name: &str,
    compile: impl Fn(&str) -> Result<CompiledFormat, ExecutionError>,
) -> Result<ArrayRef, ExecutionError> {
    check_arity(fn_name, args, 2, 2)?;
    let ts = timestamp_us(&args[0], fn_name)?;
    let fmts = utf8(&args[1], fn_name)?;
    let mut compiled: Option<(String, CompiledFormat)> = None;
    let mut out = StringBuilder::with_capacity(ts.len(), ts.len() * 16);
    let mut buf = String::new();
    for i in 0..ts.len() {
        if ts.is_null(i) || fmts.is_null(i) {
            out.append_null();
            continue;
        }
        let f = fmts.value(i);
        if !matches!(&compiled, Some((k, _)) if k == f) {
            compiled = Some((f.to_string(), compile(f)?));
        }
        buf.clear();
        let dt = micros_to_naive(ts.value(i))?;
        compiled
            .as_ref()
            .expect("compiled above")
            .1
            .render(&dt, &mut buf);
        out.append_value(&buf);
    }
    Ok(Arc::new(out.finish()))
}

/// `date_format(timestamp, format)` → VARCHAR using MySQL format
/// specifiers (see [`mysql_to_strftime`]). DATE inputs are formatted as
/// midnight.
#[derive(Debug)]
struct DateFormatFunction;

impl ScalarFunction for DateFormatFunction {
    fn name(&self) -> &str {
        "DATE_FORMAT"
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(DataType::Utf8)
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        format_timestamps(args, "DATE_FORMAT", |f| {
            CompiledFormat::new(&mysql_to_strftime(f, false, "DATE_FORMAT")?, "DATE_FORMAT")
        })
    }
}

/// `date_parse(string, format)` → TIMESTAMP using MySQL format
/// specifiers. Fields absent from the pattern default to
/// `1970-01-01 00:00:00.000` (so `date_parse('2024', '%Y')` is
/// `2024-01-01 00:00:00`), like Trino. `%h`/`%I` without `%p` is read
/// as AM. Input that does not match the pattern is an error (wrap in
/// `TRY` to get NULL instead).
#[derive(Debug)]
struct DateParseFunction;

fn parse_with_defaults(
    input: &str,
    items: &[Item<'static>],
) -> Result<NaiveDateTime, chrono::format::ParseError> {
    let mut p = Parsed::new();
    chrono::format::parse(&mut p, input, items.iter())?;
    if p.year().is_none()
        && p.year_mod_100().is_none()
        && p.isoyear().is_none()
        && p.isoyear_mod_100().is_none()
    {
        p.set_year(1970)?;
    }
    if p.month().is_none() && p.ordinal().is_none() && p.isoweek().is_none() {
        p.set_month(1)?;
    }
    if p.day().is_none() && p.ordinal().is_none() && p.weekday().is_none() {
        p.set_day(1)?;
    }
    if p.hour_mod_12().is_none() {
        p.set_hour(0)?;
    } else if p.hour_div_12().is_none() {
        p.set_ampm(false)?;
    }
    if p.minute().is_none() {
        p.set_minute(0)?;
    }
    if p.second().is_none() {
        p.set_second(0)?;
    }
    p.to_naive_datetime_with_offset(0)
}

impl ScalarFunction for DateParseFunction {
    fn name(&self) -> &str {
        "DATE_PARSE"
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(timestamp_us_dt())
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        check_arity("DATE_PARSE", args, 2, 2)?;
        let s = utf8(&args[0], "DATE_PARSE")?;
        let fmts = utf8(&args[1], "DATE_PARSE")?;
        let mut compiled: Option<(String, Vec<Item<'static>>)> = None;
        let mut out = Vec::with_capacity(s.len());
        for i in 0..s.len() {
            if s.is_null(i) || fmts.is_null(i) {
                out.push(None);
                continue;
            }
            let f = fmts.value(i);
            if !matches!(&compiled, Some((k, _)) if k == f) {
                let strf = mysql_to_strftime(f, true, "DATE_PARSE")?;
                let items: Vec<Item<'static>> =
                    StrftimeItems::new(&strf).map(to_owned_item).collect();
                compiled = Some((f.to_string(), items));
            }
            let items = &compiled.as_ref().expect("compiled above").1;
            let dt = parse_with_defaults(s.value(i), items).map_err(|e| {
                invalid(format!(
                    "DATE_PARSE: invalid value '{}' for format '{f}': {e}",
                    s.value(i)
                ))
            })?;
            out.push(Some(naive_to_micros(dt)));
        }
        Ok(Arc::new(TimestampMicrosecondArray::from(out)))
    }
}

// -- FORMAT_DATETIME (Joda-Time patterns) --

/// Translate a Joda-Time pattern (as used by Trino's
/// `format_datetime`) into a chrono strftime pattern.
///
/// Supported letters: `y`/`Y` (`yy` → 2-digit, otherwise 4-digit year),
/// `M` (`M`, `MM`, `MMM` short name, `MMMM` full name), `d`, `D` (day of
/// year), `H`, `h`, `m`, `s`, `S` (fraction, 1–9 digits),
/// `a`, `E` (`E`–`EEE` short weekday, `EEEE` full), `e` (ISO day of
/// week), `w` (ISO week), `x` (ISO week-year), `Z` (`+0000`), `z`
/// (`UTC`), and quoted literals (`'T'`, `''` for a quote). Other
/// letters are rejected.
fn joda_to_strftime(pattern: &str) -> Result<String, ExecutionError> {
    let err = |m: String| invalid(format!("FORMAT_DATETIME: {m}"));
    let chars: Vec<char> = pattern.chars().collect();
    let mut out = String::with_capacity(pattern.len() * 2);
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c == '\'' {
            // Quoted literal; '' is an escaped quote.
            if chars.get(i + 1) == Some(&'\'') {
                out.push('\'');
                i += 2;
                continue;
            }
            i += 1;
            while i < chars.len() {
                if chars[i] == '\'' {
                    if chars.get(i + 1) == Some(&'\'') {
                        out.push('\'');
                        i += 2;
                        continue;
                    }
                    break;
                }
                if chars[i] == '%' {
                    out.push_str("%%");
                } else {
                    out.push(chars[i]);
                }
                i += 1;
            }
            i += 1; // closing quote
            continue;
        }
        if !c.is_ascii_alphabetic() {
            if c == '%' {
                out.push_str("%%");
            } else {
                out.push(c);
            }
            i += 1;
            continue;
        }
        let mut n = 1;
        while i + n < chars.len() && chars[i + n] == c {
            n += 1;
        }
        i += n;
        let spec: String = match (c, n) {
            ('y' | 'Y', 2) => "%y".into(),
            ('y' | 'Y', _) => "%Y".into(),
            ('x', 2) => "%g".into(),
            ('x', _) => "%G".into(),
            ('M', 1) => "%-m".into(),
            ('M', 2) => "%m".into(),
            ('M', 3) => "%b".into(),
            ('M', _) => "%B".into(),
            ('d', 1) => "%-d".into(),
            ('d', _) => "%d".into(),
            ('D', 1 | 2) => "%-j".into(),
            ('D', _) => "%j".into(),
            ('H', 1) => "%-H".into(),
            ('H', _) => "%H".into(),
            ('h', 1) => "%-I".into(),
            ('h', _) => "%I".into(),
            ('m', 1) => "%-M".into(),
            ('m', _) => "%M".into(),
            ('s', 1) => "%-S".into(),
            ('s', _) => "%S".into(),
            ('S', 1..=9) => return_fraction(n),
            ('a', _) => "%p".into(),
            ('E', 1..=3) => "%a".into(),
            ('E', _) => "%A".into(),
            ('e', _) => "%u".into(),
            ('w', 1) => "%-V".into(),
            ('w', _) => "%V".into(),
            ('Z', _) => "+0000".into(),
            ('z', _) => "UTC".into(),
            (other, n) => {
                return Err(err(format!(
                    "unsupported pattern letter '{}'",
                    other.to_string().repeat(n)
                )))
            }
        };
        out.push_str(&spec);
    }
    Ok(out)
}

/// Joda `S`×n = first n digits of the fraction of second. chrono only
/// offers 3/6/9-digit fixed fractions, so other widths are rendered at
/// the next supported width.
fn return_fraction(n: usize) -> String {
    match n {
        1..=3 => "%3f".into(),
        4..=6 => "%6f".into(),
        _ => "%9f".into(),
    }
}

/// `format_datetime(timestamp, format)` → VARCHAR using Joda-Time
/// pattern letters (see [`joda_to_strftime`] for the supported subset).
/// DATE inputs are formatted as midnight. Fraction widths other than
/// 3, 6 or 9 `S` letters round up to the next of those widths.
#[derive(Debug)]
struct FormatDatetimeFunction;

impl ScalarFunction for FormatDatetimeFunction {
    fn name(&self) -> &str {
        "FORMAT_DATETIME"
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(DataType::Utf8)
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        format_timestamps(args, "FORMAT_DATETIME", |f| {
            CompiledFormat::new(&joda_to_strftime(f)?, "FORMAT_DATETIME")
        })
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

    // -- Trino batch 1 --

    fn by_name(name: &str) -> Arc<dyn ScalarFunction> {
        all_date_functions()
            .into_iter()
            .find(|f| f.name() == name)
            .unwrap_or_else(|| panic!("no date function {name}"))
    }

    fn day(y: i32, m: u32, d: u32) -> i32 {
        ymd_to_days(y, m, d)
    }

    fn micros(y: i32, m: u32, d: u32, h: u32, mi: u32, s: u32, ms: u32) -> i64 {
        naive_to_micros(
            NaiveDate::from_ymd_opt(y, m, d)
                .unwrap()
                .and_hms_milli_opt(h, mi, s, ms)
                .unwrap(),
        )
    }

    fn ts_array(v: &[Option<i64>]) -> ArrayRef {
        Arc::new(TimestampMicrosecondArray::from(v.to_vec()))
    }

    fn i64s(a: &ArrayRef) -> Vec<Option<i64>> {
        a.as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect()
    }

    fn dates(a: &ArrayRef) -> Vec<Option<i32>> {
        a.as_any()
            .downcast_ref::<Date32Array>()
            .unwrap()
            .iter()
            .collect()
    }

    fn tss(a: &ArrayRef) -> Vec<Option<i64>> {
        a.as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap()
            .iter()
            .collect()
    }

    fn strs_out(a: &ArrayRef) -> Vec<Option<String>> {
        a.as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap()
            .iter()
            .map(|v| v.map(str::to_string))
            .collect()
    }

    fn lit(v: &str, n: usize) -> ArrayRef {
        make_string_array(&vec![Some(v); n])
    }

    #[test]
    fn date_part_functions_on_dates() {
        // 2024-01-07 is a Sunday; 2021-01-01 is in ISO week 53 of 2020.
        let d = make_date_array(&[Some(day(2024, 1, 7)), Some(day(2021, 1, 1)), None]);
        let expect: [(&str, [Option<i64>; 3]); 8] = [
            ("YEAR", [Some(2024), Some(2021), None]),
            ("QUARTER", [Some(1), Some(1), None]),
            ("MONTH", [Some(1), Some(1), None]),
            ("DAY", [Some(7), Some(1), None]),
            ("DAY_OF_WEEK", [Some(7), Some(5), None]),
            ("DAY_OF_YEAR", [Some(7), Some(1), None]),
            ("WEEK", [Some(1), Some(53), None]),
            ("YEAR_OF_WEEK", [Some(2024), Some(2020), None]),
        ];
        for (name, want) in expect {
            let out = by_name(name).evaluate(std::slice::from_ref(&d)).unwrap();
            assert_eq!(i64s(&out), want.to_vec(), "{name}");
        }
    }

    #[test]
    fn time_part_functions_on_timestamps() {
        let t = ts_array(&[Some(micros(2024, 3, 15, 10, 20, 30, 456)), None]);
        for (name, want) in [
            ("HOUR", 10),
            ("MINUTE", 20),
            ("SECOND", 30),
            ("MILLISECOND", 456),
            ("YEAR", 2024),
            ("MONTH", 3),
        ] {
            let out = by_name(name).evaluate(std::slice::from_ref(&t)).unwrap();
            assert_eq!(i64s(&out), vec![Some(want), None], "{name}");
        }
    }

    #[test]
    fn extract_more_fields_and_timestamps() {
        let t = ts_array(&[Some(micros(2024, 8, 18, 23, 5, 0, 0))]); // a Sunday
        for (field, want) in [
            ("DOW", 7),
            ("QUARTER", 3),
            ("HOUR", 23),
            ("DAYOFYEAR", 231),
            ("WEEK(NONE)", 33),
        ] {
            let out = ExtractFunction
                .evaluate(&[lit(field, 1), t.clone()])
                .unwrap();
            assert_eq!(i64s(&out), vec![Some(want)], "EXTRACT({field})");
        }
        assert!(ExtractFunction
            .evaluate(&[lit("CENTURY", 1), t.clone()])
            .is_err());
    }

    #[test]
    fn date_trunc_timestamp_and_week_quarter() {
        let t = ts_array(&[Some(micros(2024, 5, 15, 10, 20, 30, 456))]);
        for (unit, want) in [
            ("second", micros(2024, 5, 15, 10, 20, 30, 0)),
            ("minute", micros(2024, 5, 15, 10, 20, 0, 0)),
            ("hour", micros(2024, 5, 15, 10, 0, 0, 0)),
            ("day", micros(2024, 5, 15, 0, 0, 0, 0)),
            ("week", micros(2024, 5, 13, 0, 0, 0, 0)), // Monday
            ("quarter", micros(2024, 4, 1, 0, 0, 0, 0)),
        ] {
            let out = DateTruncFunction
                .evaluate(&[lit(unit, 1), t.clone()])
                .unwrap();
            assert_eq!(tss(&out), vec![Some(want)], "date_trunc('{unit}')");
        }
        let d = make_date_array(&[Some(day(2024, 5, 15))]);
        let out = DateTruncFunction
            .evaluate(&[lit("week", 1), d.clone()])
            .unwrap();
        assert_eq!(dates(&out), vec![Some(day(2024, 5, 13))]);
        assert!(DateTruncFunction.evaluate(&[lit("hour", 1), d]).is_err());
        // Pre-epoch values truncate downward, not toward zero.
        let t = ts_array(&[Some(micros(1969, 12, 31, 23, 30, 0, 0))]);
        let out = DateTruncFunction.evaluate(&[lit("hour", 1), t]).unwrap();
        assert_eq!(tss(&out), vec![Some(micros(1969, 12, 31, 23, 0, 0, 0))]);
    }

    #[test]
    fn date_add_dates_clamp_month_end() {
        let d = make_date_array(&[Some(day(2024, 1, 31)), Some(day(2024, 3, 1)), None]);
        let n = |v: i64| -> ArrayRef { Arc::new(Int64Array::from(vec![v; 3])) };
        let out = DateAddFunction
            .evaluate(&[lit("month", 3), n(1), d.clone()])
            .unwrap();
        assert_eq!(
            dates(&out),
            vec![Some(day(2024, 2, 29)), Some(day(2024, 4, 1)), None]
        );
        let out = DateAddFunction
            .evaluate(&[lit("day", 3), n(-1), d.clone()])
            .unwrap();
        assert_eq!(
            dates(&out),
            vec![Some(day(2024, 1, 30)), Some(day(2024, 2, 29)), None]
        );
        let out = DateAddFunction
            .evaluate(&[lit("year", 3), n(1), d.clone()])
            .unwrap();
        assert_eq!(dates(&out)[0], Some(day(2025, 1, 31)));
        assert!(DateAddFunction
            .evaluate(&[lit("hour", 3), n(1), d])
            .is_err());
    }

    #[test]
    fn date_add_timestamps() {
        let t = ts_array(&[Some(micros(2024, 1, 31, 22, 0, 0, 0))]);
        let one: ArrayRef = Arc::new(Int64Array::from(vec![3]));
        let out = DateAddFunction
            .evaluate(&[lit("hour", 1), one.clone(), t.clone()])
            .unwrap();
        assert_eq!(tss(&out), vec![Some(micros(2024, 2, 1, 1, 0, 0, 0))]);
        let out = DateAddFunction
            .evaluate(&[lit("month", 1), one, t])
            .unwrap();
        assert_eq!(tss(&out), vec![Some(micros(2024, 4, 30, 22, 0, 0, 0))]);
    }

    #[test]
    fn date_diff_semantics() {
        let d = |y, m, dd| Some(day(y, m, dd));
        let a = make_date_array(&[
            d(2024, 1, 31),
            d(2024, 1, 15),
            d(2024, 3, 1),
            d(2020, 6, 1),
            None,
        ]);
        let b = make_date_array(&[
            d(2024, 2, 29),
            d(2024, 2, 14),
            d(2024, 1, 1),
            d(2024, 5, 31),
            d(2024, 1, 1),
        ]);
        let run = |unit: &str| {
            i64s(
                &DateDiffFunction
                    .evaluate(&[lit(unit, 5), a.clone(), b.clone()])
                    .unwrap(),
            )
        };
        assert_eq!(
            run("day"),
            vec![Some(29), Some(30), Some(-60), Some(1460), None]
        );
        assert_eq!(
            run("month"),
            vec![Some(1), Some(0), Some(-2), Some(47), None]
        );
        assert_eq!(run("year"), vec![Some(0), Some(0), Some(0), Some(3), None]);
        assert_eq!(
            run("week"),
            vec![Some(4), Some(4), Some(-8), Some(208), None]
        );

        let t1 = ts_array(&[Some(micros(2024, 1, 1, 10, 0, 0, 0))]);
        let t2 = ts_array(&[Some(micros(2024, 1, 2, 9, 30, 0, 0))]);
        let out = DateDiffFunction
            .evaluate(&[lit("hour", 1), t1.clone(), t2.clone()])
            .unwrap();
        assert_eq!(i64s(&out), vec![Some(23)]);
        let out = DateDiffFunction.evaluate(&[lit("day", 1), t1, t2]).unwrap();
        assert_eq!(i64s(&out), vec![Some(0)], "truncates toward zero");
    }

    #[test]
    fn last_day_of_month_and_date() {
        let d = make_date_array(&[Some(day(2024, 2, 10)), Some(day(2023, 12, 5)), None]);
        let out = LastDayOfMonthFunction.evaluate(&[d]).unwrap();
        assert_eq!(
            dates(&out),
            vec![Some(day(2024, 2, 29)), Some(day(2023, 12, 31)), None]
        );
        let out = DateFunction
            .evaluate(&[make_string_array(&[Some("2024-03-15"), None])])
            .unwrap();
        assert_eq!(dates(&out), vec![Some(day(2024, 3, 15)), None]);
        let out = DateFunction
            .evaluate(&[ts_array(&[Some(micros(2024, 3, 15, 23, 59, 0, 0))])])
            .unwrap();
        assert_eq!(dates(&out), vec![Some(day(2024, 3, 15))]);
    }

    #[test]
    fn now_and_current_date_fill_the_batch() {
        let out = NowFunction.invoke(&[], 4).unwrap();
        let v = tss(&out);
        assert_eq!(v.len(), 4);
        assert!(v.iter().all(|x| *x == v[0]));
        assert!(v[0].unwrap() > micros(2020, 1, 1, 0, 0, 0, 0));
        let out = CurrentDateFunction.invoke(&[], 3).unwrap();
        assert_eq!(out.len(), 3);
    }

    #[test]
    fn unixtime_roundtrip() {
        let secs: ArrayRef = Arc::new(Float64Array::from(vec![Some(1_700_000_000.5), None]));
        let out = FromUnixtimeFunction.evaluate(&[secs]).unwrap();
        assert_eq!(tss(&out), vec![Some(1_700_000_000_500_000), None]);
        let back = ToUnixtimeFunction.evaluate(&[out]).unwrap();
        let back = back.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(back.value(0), 1_700_000_000.5);
        assert!(back.is_null(1));
    }

    #[test]
    fn date_format_mysql_specifiers() {
        let t = ts_array(&[Some(micros(2024, 3, 1, 15, 4, 5, 123)), None]);
        let run = |fmt: &str| {
            strs_out(
                &DateFormatFunction
                    .evaluate(&[t.clone(), lit(fmt, 2)])
                    .unwrap(),
            )
        };
        assert_eq!(
            run("%Y-%m-%d %H:%i:%s"),
            vec![Some("2024-03-01 15:04:05".into()), None]
        );
        assert_eq!(
            run("%W, %M %D %Y")[0].as_deref(),
            Some("Friday, March 1st 2024")
        );
        assert_eq!(
            run("%a %b %e %h:%i %p")[0].as_deref(),
            Some("Fri Mar 1 03:04 PM")
        );
        assert_eq!(run("%T.%f")[0].as_deref(), Some("15:04:05.123000"));
        assert_eq!(run("%j %y %c 100%%")[0].as_deref(), Some("061 24 3 100%"));
        assert!(DateFormatFunction.evaluate(&[t, lit("%U", 2)]).is_err());
    }

    #[test]
    fn date_parse_mysql_specifiers() {
        let run = |s: &str, fmt: &str| {
            DateParseFunction
                .evaluate(&[make_string_array(&[Some(s)]), lit(fmt, 1)])
                .map(|a| tss(&a)[0])
        };
        assert_eq!(
            run("2024-03-15 10:20:30", "%Y-%m-%d %H:%i:%s").unwrap(),
            Some(micros(2024, 3, 15, 10, 20, 30, 0))
        );
        assert_eq!(
            run("2024", "%Y").unwrap(),
            Some(micros(2024, 1, 1, 0, 0, 0, 0))
        );
        assert_eq!(
            run("03/15/2024 07:05 PM", "%m/%d/%Y %h:%i %p").unwrap(),
            Some(micros(2024, 3, 15, 19, 5, 0, 0))
        );
        assert!(run("not a date", "%Y-%m-%d").is_err());
        let out = DateParseFunction
            .evaluate(&[make_string_array(&[None]), lit("%Y", 1)])
            .unwrap();
        assert_eq!(tss(&out), vec![None]);
    }

    #[test]
    fn format_datetime_joda_patterns() {
        let t = ts_array(&[Some(micros(2024, 3, 1, 15, 4, 5, 123))]);
        let run = |fmt: &str| {
            FormatDatetimeFunction
                .evaluate(&[t.clone(), lit(fmt, 1)])
                .map(|a| strs_out(&a)[0].clone().unwrap())
        };
        assert_eq!(run("yyyy-MM-dd HH:mm:ss").unwrap(), "2024-03-01 15:04:05");
        assert_eq!(
            run("yyyy-MM-dd'T'HH:mm:ss.SSS").unwrap(),
            "2024-03-01T15:04:05.123"
        );
        assert_eq!(run("EEE, d MMM yy").unwrap(), "Fri, 1 Mar 24");
        assert_eq!(run("EEEE MMMM h a").unwrap(), "Friday March 3 PM");
        assert_eq!(run("'it''s' D").unwrap(), "it's 61");
        assert!(run("G").is_err());
    }
}
