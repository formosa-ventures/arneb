use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes as arrow_types;
use arrow::record_batch::RecordBatch;
use bytes::BytesMut;
use pgwire::api::portal::Format;
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo};
use pgwire::api::Type;
use pgwire::error::{PgWireError, PgWireResult};
use postgres_protocol::types as wire;
use postgres_protocol::IsNull;
use postgres_types::Kind;

use arneb_common::types::{ColumnInfo, DataType};

/// Maps a trino `DataType` to a PostgreSQL `Type` (OID).
pub fn datatype_to_pg_type(dt: &DataType) -> Type {
    match dt {
        DataType::Boolean => Type::BOOL,
        DataType::Int8 | DataType::Int16 => Type::INT2,
        DataType::Int32 => Type::INT4,
        DataType::Int64 => Type::INT8,
        DataType::Float32 => Type::FLOAT4,
        DataType::Float64 => Type::FLOAT8,
        DataType::Decimal128 { .. } => Type::NUMERIC,
        DataType::Utf8 | DataType::LargeUtf8 => Type::VARCHAR,
        DataType::Char(_) => Type::BPCHAR,
        DataType::Binary => Type::BYTEA,
        DataType::Date32 => Type::DATE,
        DataType::Timestamp { .. } => Type::TIMESTAMP,
        DataType::List(element) => array_pg_type(element),
        _ => Type::TEXT,
    }
}

/// The PostgreSQL array type over `element`'s type (`int8[]` for `Int64`),
/// or `text[]` when the element type has no array type of its own.
pub fn array_pg_type(element: &DataType) -> Type {
    let oid = i64::from(datatype_to_pg_type(element).oid());
    arneb_common::pg_oid::PG_TYPES
        .iter()
        .find(|t| t.oid == oid)
        .and_then(|t| u32::try_from(t.array_oid).ok())
        .and_then(Type::from_oid)
        .unwrap_or(Type::TEXT_ARRAY)
}

/// Converts a slice of `ColumnInfo` into pgwire `FieldInfo` entries for RowDescription.
pub fn column_info_to_field_info(columns: &[ColumnInfo]) -> Vec<FieldInfo> {
    columns
        .iter()
        .map(|col| {
            let pg_type = datatype_to_pg_type(&col.data_type);
            FieldInfo::new(col.name.clone(), None, None, pg_type, FieldFormat::Text)
        })
        .collect()
}

/// `fields` with each column's format set from the client's Bind result
/// format codes: none means all text, one code applies to every column, and
/// otherwise there is one code per column (`08P01` when the count differs,
/// as in PostgreSQL).
pub fn with_result_formats(
    fields: Vec<FieldInfo>,
    format: &Format,
) -> PgWireResult<Vec<FieldInfo>> {
    if let Format::Individual(codes) = format {
        if codes.len() != fields.len() {
            return Err(PgWireError::UserError(Box::new(
                pgwire::error::ErrorInfo::new(
                    "ERROR".to_string(),
                    "08P01".to_string(),
                    format!(
                        "bind message has {} result formats but query has {} columns",
                        codes.len(),
                        fields.len()
                    ),
                ),
            )));
        }
    }
    Ok(fields
        .into_iter()
        .enumerate()
        .map(|(i, f)| {
            let wanted = format.format_for(i);
            if wanted == f.format() {
                return f;
            }
            let datatype = f.datatype().clone();
            rebuild_field(&f, datatype, wanted)
        })
        .collect())
}

fn rebuild_field(f: &FieldInfo, datatype: Type, format: FieldFormat) -> FieldInfo {
    FieldInfo::new(
        f.name().to_string(),
        f.table_id(),
        f.column_id(),
        datatype,
        format,
    )
    .with_format_options(f.format_options().clone())
    .with_type_size(f.type_size())
    .with_type_modifier(f.type_modifier())
}

/// `fields` with each binary column typed as `described` (the statement's
/// Describe) says, where the two differ.
///
/// A statement with `$N` placeholders is described before its parameters
/// are bound, and Arneb plans the bound SQL again, so a column such as
/// `$1 + 1` is described as `text` but executes as `int8`. In text format
/// the client reads either; in binary it decodes by the described type, so
/// the values must be encoded as that type: a `text` column carries the
/// value's text form, and a type with no lossless encoding from the
/// executed values is refused (`0A000`) rather than sent misread.
pub fn with_described_types(fields: Vec<FieldInfo>, described: &[FieldInfo]) -> Vec<FieldInfo> {
    if described.len() != fields.len() {
        return fields;
    }
    fields
        .into_iter()
        .zip(described)
        .map(|(f, d)| {
            if f.format() == FieldFormat::Binary && f.datatype() != d.datatype() {
                rebuild_field(&f, d.datatype().clone(), FieldFormat::Binary)
            } else {
                f
            }
        })
        .collect()
}

/// Encodes a single value from an Arrow array at the given row index as an `Option<String>`.
/// Returns `None` for NULL values.
fn encode_value(array: &dyn Array, row: usize) -> Option<String> {
    if array.is_null(row) {
        return None;
    }

    let dt = array.data_type();
    match dt {
        // A NullArray has no validity buffer, so `is_null` reports false.
        arrow_types::DataType::Null => None,
        arrow_types::DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            Some(if arr.value(row) { "t" } else { "f" }.to_string())
        }
        arrow_types::DataType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>().unwrap();
            Some(arr.value(row).to_string())
        }
        arrow_types::DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
            Some(arr.value(row).to_string())
        }
        arrow_types::DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            Some(arr.value(row).to_string())
        }
        arrow_types::DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            Some(arr.value(row).to_string())
        }
        arrow_types::DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            Some(arr.value(row).to_string())
        }
        arrow_types::DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            Some(arr.value(row).to_string())
        }
        arrow_types::DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            Some(arr.value(row).to_string())
        }
        arrow_types::DataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            Some(arr.value(row).to_string())
        }
        arrow_types::DataType::Binary => {
            let arr = array.as_any().downcast_ref::<BinaryArray>().unwrap();
            Some(hex::encode(arr.value(row)))
        }
        arrow_types::DataType::Date32 => {
            let arr = array.as_any().downcast_ref::<Date32Array>().unwrap();
            let days = arr.value(row);
            Some(
                days.checked_add(719_163)
                    .and_then(chrono::NaiveDate::from_num_days_from_ce_opt)
                    .map(|date| with_era(date, date.format("%m-%d").to_string()))
                    // Past chrono's ±262 143 years: no calendar form, but never
                    // a made-up date.
                    .unwrap_or_else(|| days.to_string()),
            )
        }
        arrow_types::DataType::Timestamp(unit, _) => {
            // `(value, units per second)`. Split into whole seconds and a
            // remainder in the array's own unit; scaling to nanoseconds
            // first would overflow i64 outside 1677..2262.
            let (value, per_sec) = match unit {
                arrow_types::TimeUnit::Second => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampSecondArray>()
                        .unwrap();
                    (arr.value(row), 1)
                }
                arrow_types::TimeUnit::Millisecond => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .unwrap();
                    (arr.value(row), 1_000)
                }
                arrow_types::TimeUnit::Microsecond => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .unwrap();
                    (arr.value(row), 1_000_000)
                }
                arrow_types::TimeUnit::Nanosecond => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .unwrap();
                    (arr.value(row), 1_000_000_000)
                }
            };
            Some(format_timestamp(value, per_sec))
        }
        arrow_types::DataType::Decimal128(precision, scale) => {
            let arr = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
            let raw = arr.value(row);
            format_decimal128(raw, *precision, *scale)
        }
        arrow_types::DataType::Dictionary(_, value_type) => {
            let materialized = arrow::compute::cast(array, value_type.as_ref()).ok()?;
            encode_value(materialized.as_ref(), row)
        }
        arrow_types::DataType::List(_) | arrow_types::DataType::LargeList(_) => {
            Some(encode_pg_array_text(array, row))
        }
        _ => arrow::util::display::array_value_to_string(array, row).ok(),
    }
}

/// PostgreSQL text form (`{a,b,"c d",NULL}`) of the list at `row`.
///
/// An element is double-quoted when it is empty, equals `NULL` in any case,
/// or contains a comma, double quote, backslash, brace or whitespace;
/// inside the quotes only `"` and `\` are backslash-escaped. A NULL element
/// prints bare `NULL`, and a nested list prints as a nested `{...}` without
/// quotes. A NULL list prints as `NULL`; a non-list array prints the value
/// itself.
pub fn encode_pg_array_text(array: &dyn Array, row: usize) -> String {
    if array.is_null(row) {
        return "NULL".to_string();
    }
    let elements: ArrayRef = match array.data_type() {
        arrow_types::DataType::List(_) => array.as_list::<i32>().value(row),
        arrow_types::DataType::LargeList(_) => array.as_list::<i64>().value(row),
        _ => return encode_value(array, row).unwrap_or_else(|| "NULL".to_string()),
    };
    let nested = matches!(
        elements.data_type(),
        arrow_types::DataType::List(_) | arrow_types::DataType::LargeList(_)
    );
    let mut out = String::from("{");
    for i in 0..elements.len() {
        if i > 0 {
            out.push(',');
        }
        if elements.is_null(i) {
            out.push_str("NULL");
        } else if nested {
            out.push_str(&encode_pg_array_text(elements.as_ref(), i));
        } else {
            match encode_value(elements.as_ref(), i) {
                Some(text) => push_array_element(&mut out, &text),
                None => out.push_str("NULL"),
            }
        }
    }
    out.push('}');
    out
}

fn push_array_element(out: &mut String, text: &str) {
    let needs_quotes = text.is_empty()
        || text.eq_ignore_ascii_case("null")
        || text
            .chars()
            .any(|c| matches!(c, ',' | '"' | '\\' | '{' | '}') || c.is_whitespace());
    if !needs_quotes {
        out.push_str(text);
        return;
    }
    out.push('"');
    for c in text.chars() {
        if c == '"' || c == '\\' {
            out.push('\\');
        }
        out.push(c);
    }
    out.push('"');
}

/// PostgreSQL text form of a timestamp `value` counted in `1 / per_sec`
/// seconds from the epoch: fractional seconds with trailing zeros trimmed,
/// and `BC` for years before 1 AD.
fn format_timestamp(value: i64, per_sec: i64) -> String {
    // Floor division: a pre-1970 instant has a negative remainder.
    let secs = value.div_euclid(per_sec);
    let nsecs = (value.rem_euclid(per_sec) * (1_000_000_000 / per_sec)) as u32;
    let Some(dt) = chrono::DateTime::from_timestamp(secs, nsecs).map(|d| d.naive_utc()) else {
        // Past chrono's ±262 143 years: no calendar form, but never a NULL.
        return value.to_string();
    };
    let mut rest = dt.format("%m-%d %H:%M:%S").to_string();
    if nsecs > 0 {
        let frac = format!("{nsecs:09}");
        rest.push('.');
        rest.push_str(frac.trim_end_matches('0'));
    }
    with_era(dt.date(), rest)
}

/// `YYYY-<rest>` the way PostgreSQL prints a year: 1 BC is year 0 in chrono,
/// so years ≤ 0 print as `1 - year` with a trailing ` BC`.
fn with_era(date: chrono::NaiveDate, rest: String) -> String {
    use chrono::Datelike;

    let year = date.year();
    if year <= 0 {
        format!("{:04}-{rest} BC", 1 - year)
    } else {
        format!("{year:04}-{rest}")
    }
}

fn format_decimal128(value: i128, _precision: u8, scale: i8) -> Option<String> {
    if scale <= 0 {
        let multiplier = 10i128.pow((-scale) as u32);
        return Some((value * multiplier).to_string());
    }
    let scale = scale as u32;
    let divisor = 10u128.pow(scale);
    let sign = if value < 0 { "-" } else { "" };
    let integer_part = value.unsigned_abs() / divisor;
    let fractional_part = value.unsigned_abs() % divisor;
    Some(format!(
        "{sign}{}.{:0>width$}",
        integer_part,
        fractional_part,
        width = scale as usize
    ))
}

/// The error for a struct (composite) column requested in binary: there is
/// no binary composite encoding here. In text, a struct goes as Arrow
/// displays it (`{x: 1, n: a}`).
fn composite_binary_error(field: &FieldInfo) -> PgWireError {
    PgWireError::UserError(Box::new(pgwire::error::ErrorInfo::new(
        "ERROR".to_string(),
        "0A000".to_string(),
        format!(
            "binary result format is not supported for composite values (column \"{}\")",
            field.name()
        ),
    )))
}

fn has_arrow_struct(dt: &arrow_types::DataType) -> bool {
    match dt {
        arrow_types::DataType::Struct(_) => true,
        arrow_types::DataType::List(f) | arrow_types::DataType::LargeList(f) => {
            has_arrow_struct(f.data_type())
        }
        _ => false,
    }
}

/// Encodes `Vec<RecordBatch>` into a vector of pgwire `PgWireResult<DataRow>` entries
/// suitable for streaming. Returns the encoded rows and total row count.
pub fn encode_record_batches(
    schema: &Arc<Vec<FieldInfo>>,
    batches: &[RecordBatch],
) -> PgWireResult<(Vec<PgWireResult<pgwire::messages::data::DataRow>>, usize)> {
    let mut rows = Vec::new();
    let mut total_count = 0usize;

    for batch in batches {
        let num_rows = batch.num_rows();
        total_count += num_rows;
        let columns: Vec<ArrayRef> = batch
            .columns()
            .iter()
            .map(|array| match array.data_type() {
                arrow_types::DataType::Dictionary(_, value_type) => {
                    arrow::compute::cast(array, value_type.as_ref())
                }
                _ => Ok(array.clone()),
            })
            .collect::<Result<_, _>>()
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

        let is_array_col: Vec<bool> = schema
            .iter()
            .map(|f| {
                let oid = i64::from(f.datatype().oid());
                arneb_common::pg_oid::PG_TYPES
                    .iter()
                    .any(|t| t.array_oid == oid)
            })
            .collect();
        for (field, array) in schema.iter().zip(&columns) {
            if field.format() == FieldFormat::Binary {
                if has_arrow_struct(array.data_type()) {
                    return Err(composite_binary_error(field));
                }
                check_binary(field, array.data_type())?;
            }
        }
        let mut encoder = DataRowEncoder::new(schema.clone());
        let mut scratch = BytesMut::new();
        for row_idx in 0..num_rows {
            for (col_idx, array) in columns.iter().enumerate() {
                let field = &schema[col_idx];
                if field.format() == FieldFormat::Binary {
                    scratch.clear();
                    let value =
                        match write_binary(array.as_ref(), row_idx, field.datatype(), &mut scratch)
                            .map_err(PgWireError::ApiError)?
                        {
                            IsNull::No => Some(&scratch[..]),
                            IsNull::Yes => None,
                        };
                    // The bytes are already in the type's binary form;
                    // `bytea` copies them through verbatim.
                    encoder.encode_field_with_type_and_format(
                        &value,
                        &Type::BYTEA,
                        FieldFormat::Binary,
                        field.format_options().as_ref(),
                    )?;
                    continue;
                }
                let value = encode_value(array.as_ref(), row_idx);
                if is_array_col[col_idx] {
                    // `encode_value` already produced the `{...}` literal;
                    // typed as an array, pgwire would quote it again as a
                    // single element.
                    encoder.encode_field_with_type_and_format(
                        &value,
                        &Type::TEXT,
                        field.format(),
                        field.format_options().as_ref(),
                    )?;
                } else {
                    encoder.encode_field(&value)?;
                }
            }
            rows.push(Ok(encoder.take_row()));
        }
    }

    Ok((rows, total_count))
}

/// Microseconds from 1970-01-01 to PostgreSQL's epoch, 2000-01-01.
const PG_EPOCH_MICROS: i64 = 946_684_800_000_000;
/// Days from 1970-01-01 to 2000-01-01.
const PG_EPOCH_DAYS: i32 = 10_957;

fn unsupported_binary(field: &FieldInfo, dt: &arrow_types::DataType) -> PgWireError {
    PgWireError::UserError(Box::new(pgwire::error::ErrorInfo::new(
        "ERROR".to_string(),
        "0A000".to_string(),
        format!(
            "binary result format is not supported for type {} (column \"{}\", Arrow {dt})",
            field.datatype(),
            field.name()
        ),
    )))
}

/// Whether values of Arrow type `dt` have a binary encoding as PostgreSQL
/// type `ty`: the exact type or a lossless widening of it.
fn binary_supported(ty: &Type, dt: &arrow_types::DataType) -> bool {
    use arrow_types::DataType as A;
    if matches!(dt, A::Null) {
        return true;
    }
    if let Kind::Array(member) = ty.kind() {
        return match dt {
            A::List(f) | A::LargeList(f) => {
                !matches!(f.data_type(), A::List(_) | A::LargeList(_))
                    && binary_supported(member, f.data_type())
            }
            _ => false,
        };
    }
    // OIDs: 16 bool, 21 int2, 23 int4, 20 int8, 700 float4, 701 float8,
    // 1700 numeric, 25 text, 1043 varchar, 1042 bpchar, 19 name, 17 bytea,
    // 1082 date, 1114 timestamp, 1184 timestamptz.
    match ty.oid() {
        16 => matches!(dt, A::Boolean),
        21 => matches!(dt, A::Int8 | A::Int16),
        23 => matches!(dt, A::Int8 | A::Int16 | A::Int32),
        20 => matches!(dt, A::Int8 | A::Int16 | A::Int32 | A::Int64),
        700 => matches!(dt, A::Float32),
        701 => matches!(dt, A::Float32 | A::Float64),
        1700 => matches!(
            dt,
            A::Decimal32(..)
                | A::Decimal64(..)
                | A::Decimal128(..)
                | A::Int8
                | A::Int16
                | A::Int32
                | A::Int64
        ),
        // Text types: the binary form is the text form's UTF-8 bytes (an
        // array value's `{...}` literal included).
        25 | 1043 | 1042 | 19 => true,
        17 => matches!(dt, A::Binary | A::LargeBinary),
        1082 => matches!(dt, A::Date32),
        1114 | 1184 => matches!(dt, A::Timestamp(..)),
        _ => false,
    }
}

/// Refuses a binary result column whose type has no binary encoding here
/// (`0A000`), instead of sending text the client would misread.
fn check_binary(field: &FieldInfo, dt: &arrow_types::DataType) -> PgWireResult<()> {
    if binary_supported(field.datatype(), dt) {
        Ok(())
    } else {
        Err(unsupported_binary(field, dt))
    }
}

type WireResult<T> = Result<T, Box<dyn std::error::Error + Sync + Send>>;

/// Appends the binary form of `array[row]` as PostgreSQL type `ty` to `out`.
/// The pair passed [`binary_supported`].
fn write_binary(
    array: &dyn Array,
    row: usize,
    ty: &Type,
    out: &mut BytesMut,
) -> WireResult<IsNull> {
    use arrow_types::DataType as A;
    if array.is_null(row) || matches!(array.data_type(), A::Null) {
        return Ok(IsNull::Yes);
    }
    if let Kind::Array(member) = ty.kind() {
        let elements = match array.data_type() {
            A::List(_) => array.as_list::<i32>().value(row),
            _ => array.as_list::<i64>().value(row),
        };
        let len = i32::try_from(elements.len())?;
        // PostgreSQL sends an empty array with no dimensions.
        let dimension = (len > 0).then_some(wire::ArrayDimension {
            len,
            lower_bound: 1,
        });
        wire::array_to_sql(
            dimension,
            member.oid(),
            0..elements.len(),
            |i, buf| write_binary(elements.as_ref(), i, member, buf),
            out,
        )?;
        return Ok(IsNull::No);
    }
    match ty.oid() {
        16 => wire::bool_to_sql(array.as_boolean().value(row), out),
        21 => wire::int2_to_sql(int_value(array, row)?.try_into()?, out),
        23 => wire::int4_to_sql(int_value(array, row)?.try_into()?, out),
        20 => wire::int8_to_sql(int_value(array, row)?, out),
        700 => wire::float4_to_sql(
            array.as_primitive::<arrow_types::Float32Type>().value(row),
            out,
        ),
        701 => {
            let v = match array.data_type() {
                A::Float32 => {
                    f64::from(array.as_primitive::<arrow_types::Float32Type>().value(row))
                }
                _ => array.as_primitive::<arrow_types::Float64Type>().value(row),
            };
            wire::float8_to_sql(v, out)
        }
        1700 => {
            let (value, scale) = match array.data_type() {
                A::Decimal32(_, s) => (
                    i128::from(
                        array
                            .as_primitive::<arrow_types::Decimal32Type>()
                            .value(row),
                    ),
                    *s,
                ),
                A::Decimal64(_, s) => (
                    i128::from(
                        array
                            .as_primitive::<arrow_types::Decimal64Type>()
                            .value(row),
                    ),
                    *s,
                ),
                A::Decimal128(_, s) => (
                    array
                        .as_primitive::<arrow_types::Decimal128Type>()
                        .value(row),
                    *s,
                ),
                _ => (i128::from(int_value(array, row)?), 0),
            };
            numeric_to_sql(value, scale, out)
        }
        17 => match array.data_type() {
            A::LargeBinary => wire::bytea_to_sql(array.as_binary::<i64>().value(row), out),
            _ => wire::bytea_to_sql(array.as_binary::<i32>().value(row), out),
        },
        1082 => {
            let days = array.as_primitive::<arrow_types::Date32Type>().value(row);
            wire::date_to_sql(
                days.checked_sub(PG_EPOCH_DAYS).ok_or("date out of range")?,
                out,
            )
        }
        1114 | 1184 => {
            let A::Timestamp(unit, _) = array.data_type() else {
                return Err("timestamp column holds no timestamps".into());
            };
            use arrow_types::TimeUnit as U;
            let micros = match unit {
                U::Second => array
                    .as_primitive::<arrow_types::TimestampSecondType>()
                    .value(row)
                    .checked_mul(1_000_000),
                U::Millisecond => array
                    .as_primitive::<arrow_types::TimestampMillisecondType>()
                    .value(row)
                    .checked_mul(1_000),
                U::Microsecond => Some(
                    array
                        .as_primitive::<arrow_types::TimestampMicrosecondType>()
                        .value(row),
                ),
                // PostgreSQL keeps microseconds; truncate toward the past.
                U::Nanosecond => Some(
                    array
                        .as_primitive::<arrow_types::TimestampNanosecondType>()
                        .value(row)
                        .div_euclid(1_000),
                ),
            };
            let pg = micros
                .and_then(|m| m.checked_sub(PG_EPOCH_MICROS))
                .ok_or("timestamp out of range")?;
            wire::timestamp_to_sql(pg, out)
        }
        // Text types: the same bytes the text format sends.
        _ => match encode_value(array, row) {
            Some(text) => wire::text_to_sql(&text, out),
            None => return Ok(IsNull::Yes),
        },
    }
    Ok(IsNull::No)
}

fn int_value(array: &dyn Array, row: usize) -> WireResult<i64> {
    use arrow_types::DataType as A;
    Ok(match array.data_type() {
        A::Int8 => i64::from(array.as_primitive::<arrow_types::Int8Type>().value(row)),
        A::Int16 => i64::from(array.as_primitive::<arrow_types::Int16Type>().value(row)),
        A::Int32 => i64::from(array.as_primitive::<arrow_types::Int32Type>().value(row)),
        A::Int64 => array.as_primitive::<arrow_types::Int64Type>().value(row),
        other => return Err(format!("not an integer column: {other}").into()),
    })
}

/// PostgreSQL's binary `numeric` form of `value × 10^-scale`: `ndigits`,
/// `weight` (of the first base-10000 digit), `sign`, `dscale` (decimal
/// digits after the point), then the base-10000 digits, without leading or
/// trailing zero digits. Neither `postgres-types` nor `rust_decimal` (28
/// digits) covers a 38-digit `Decimal128`.
fn numeric_to_sql(value: i128, scale: i8, out: &mut BytesMut) {
    use bytes::BufMut;

    let mut digits = value.unsigned_abs().to_string();
    // A negative scale multiplies by a power of ten.
    if scale < 0 {
        digits.extend(std::iter::repeat_n('0', usize::from(scale.unsigned_abs())));
    }
    let scale = usize::try_from(scale.max(0)).unwrap_or(0);
    if digits.len() <= scale {
        digits.insert_str(0, &"0".repeat(scale + 1 - digits.len()));
    }
    let (int_part, frac_part) = digits.split_at(digits.len() - scale);
    let to_group = |chunk: &[u8]| -> i16 {
        chunk
            .iter()
            .fold(0i16, |acc, d| acc * 10 + i16::from(d - b'0'))
    };
    // Integer digits group from the point leftwards, fraction digits
    // rightwards; zero-pad each to whole groups.
    let int_padded = format!("{}{int_part}", "0".repeat((4 - int_part.len() % 4) % 4));
    let frac_padded = format!("{frac_part}{}", "0".repeat((4 - frac_part.len() % 4) % 4));
    let int_groups: Vec<i16> = int_padded.as_bytes().chunks(4).map(to_group).collect();
    let mut groups: Vec<i16> = int_groups.clone();
    groups.extend(frac_padded.as_bytes().chunks(4).map(to_group));
    let mut weight = int_groups.len() as i16 - 1;
    let leading = groups.iter().take_while(|g| **g == 0).count();
    groups.drain(..leading);
    weight -= leading as i16;
    while groups.last() == Some(&0) {
        groups.pop();
    }
    if groups.is_empty() {
        weight = 0;
    }
    out.put_i16(groups.len() as i16);
    out.put_i16(weight);
    out.put_u16(if value < 0 && !groups.is_empty() {
        0x4000
    } else {
        0
    });
    out.put_u16(scale as u16);
    for g in groups {
        out.put_i16(g);
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{ListArray, StringArray};
    use arrow::datatypes::Int32Type;

    #[test]
    fn pg_array_text_quotes_and_nests() {
        let strings = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), None, Some(3)]),
            None,
            Some(vec![]),
        ]);
        assert_eq!(encode_pg_array_text(&strings, 0), "{1,NULL,3}");
        assert_eq!(encode_pg_array_text(&strings, 1), "NULL");
        assert_eq!(encode_pg_array_text(&strings, 2), "{}");

        let values: ArrayRef = Arc::new(StringArray::from(vec![
            Some("a"),
            Some(""),
            Some("x y"),
            Some("Null"),
            None,
        ]));
        let field = Arc::new(arrow::datatypes::Field::new(
            "item",
            arrow_types::DataType::Utf8,
            true,
        ));
        let offsets = arrow::buffer::OffsetBuffer::from_lengths([5]);
        let list = ListArray::new(field, offsets, values, None);
        assert_eq!(
            encode_pg_array_text(&list, 0),
            r#"{a,"","x y","Null",NULL}"#
        );

        // A list of lists nests without quoting the inner braces.
        let inner: ArrayRef = Arc::new(strings);
        let outer_field = Arc::new(arrow::datatypes::Field::new(
            "item",
            inner.data_type().clone(),
            true,
        ));
        let outer = ListArray::new(
            outer_field,
            arrow::buffer::OffsetBuffer::from_lengths([2]),
            inner.slice(0, 2),
            None,
        );
        assert_eq!(encode_pg_array_text(&outer, 0), "{{1,NULL,3},NULL}");
    }

    #[test]
    fn pg_array_text_renders_untyped_null_elements_as_null() {
        let values: ArrayRef = Arc::new(NullArray::new(2));
        let field = Arc::new(arrow::datatypes::Field::new(
            "item",
            arrow_types::DataType::Null,
            true,
        ));
        let list = ListArray::new(
            field,
            arrow::buffer::OffsetBuffer::from_lengths([2]),
            values,
            None,
        );
        assert_eq!(encode_pg_array_text(&list, 0), "{NULL,NULL}");
    }

    #[test]
    fn array_type_oids() {
        use arneb_common::types::DataType as DT;
        assert_eq!(
            datatype_to_pg_type(&DT::List(Box::new(DT::Int32))).oid(),
            1007
        );
        assert_eq!(
            datatype_to_pg_type(&DT::List(Box::new(DT::Int64))).oid(),
            1016
        );
        assert_eq!(
            datatype_to_pg_type(&DT::List(Box::new(DT::Utf8))).oid(),
            1015
        );
        // A struct element maps to text, whose array type is text[].
        assert_eq!(
            datatype_to_pg_type(&DT::List(Box::new(DT::Struct(vec![])))).oid(),
            1009
        );
    }
    use super::*;
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    #[test]
    fn test_datatype_to_pg_type_int32() {
        assert_eq!(datatype_to_pg_type(&DataType::Int32), Type::INT4);
    }

    #[test]
    fn test_datatype_to_pg_type_utf8() {
        assert_eq!(datatype_to_pg_type(&DataType::Utf8), Type::VARCHAR);
    }

    #[test]
    fn test_datatype_to_pg_type_char() {
        assert_eq!(datatype_to_pg_type(&DataType::Char(50)), Type::BPCHAR);
    }

    #[test]
    fn test_datatype_to_pg_type_boolean() {
        assert_eq!(datatype_to_pg_type(&DataType::Boolean), Type::BOOL);
    }

    #[test]
    fn test_datatype_to_pg_type_float64() {
        assert_eq!(datatype_to_pg_type(&DataType::Float64), Type::FLOAT8);
    }

    #[test]
    fn test_datatype_to_pg_type_unsupported_fallback() {
        assert_eq!(datatype_to_pg_type(&DataType::Null), Type::TEXT);
    }

    #[test]
    fn test_column_info_to_field_info() {
        let cols = vec![
            ColumnInfo {
                name: "id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            },
            ColumnInfo {
                name: "name".to_string(),
                data_type: DataType::Utf8,
                nullable: true,
            },
        ];
        let fields = column_info_to_field_info(&cols);
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].name(), "id");
        assert_eq!(*fields[0].datatype(), Type::INT4);
        assert_eq!(fields[1].name(), "name");
        assert_eq!(*fields[1].datatype(), Type::VARCHAR);
    }

    #[test]
    fn test_encode_int32_values() {
        let arr: ArrayRef = Arc::new(Int32Array::from(vec![1, -7, 0]));
        assert_eq!(encode_value(arr.as_ref(), 0), Some("1".to_string()));
        assert_eq!(encode_value(arr.as_ref(), 1), Some("-7".to_string()));
        assert_eq!(encode_value(arr.as_ref(), 2), Some("0".to_string()));
    }

    #[test]
    fn test_encode_boolean_values() {
        let arr: ArrayRef = Arc::new(BooleanArray::from(vec![true, false]));
        assert_eq!(encode_value(arr.as_ref(), 0), Some("t".to_string()));
        assert_eq!(encode_value(arr.as_ref(), 1), Some("f".to_string()));
    }

    #[test]
    #[allow(clippy::approx_constant)]
    fn test_encode_float64_values() {
        let arr: ArrayRef = Arc::new(Float64Array::from(vec![3.14, -0.5]));
        assert_eq!(encode_value(arr.as_ref(), 0), Some("3.14".to_string()));
        assert_eq!(encode_value(arr.as_ref(), 1), Some("-0.5".to_string()));
    }

    #[test]
    fn test_encode_utf8_values() {
        let arr: ArrayRef = Arc::new(StringArray::from(vec!["hello", "world"]));
        assert_eq!(encode_value(arr.as_ref(), 0), Some("hello".to_string()));
        assert_eq!(encode_value(arr.as_ref(), 1), Some("world".to_string()));
    }

    #[test]
    fn test_encode_null_values() {
        let arr: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), None, Some(3)]));
        assert_eq!(encode_value(arr.as_ref(), 0), Some("1".to_string()));
        assert_eq!(encode_value(arr.as_ref(), 1), None);
        assert_eq!(encode_value(arr.as_ref(), 2), Some("3".to_string()));
    }

    #[test]
    fn test_encode_null_typed_array() {
        // NullArray has no validity buffer: `is_null` is false for every row.
        let arr: ArrayRef = Arc::new(NullArray::new(3));
        assert_eq!(encode_value(arr.as_ref(), 0), None);
        assert_eq!(encode_value(arr.as_ref(), 2), None);
    }

    #[test]
    fn test_encode_unhandled_type_formats_one_cell() {
        // Types without a dedicated arm (here a TIME32) render the single
        // cell, not the debug dump of the whole array.
        let arr: ArrayRef = Arc::new(Time32SecondArray::from(vec![3661, 60]));
        assert_eq!(encode_value(arr.as_ref(), 0), Some("01:01:01".to_string()));
        assert_eq!(encode_value(arr.as_ref(), 1), Some("00:01:00".to_string()));
    }

    #[test]
    fn test_encode_date32_values() {
        // 2024-01-15 is day 19737 from epoch (1970-01-01)
        let arr: ArrayRef = Arc::new(Date32Array::from(vec![19737]));
        let val = encode_value(arr.as_ref(), 0).unwrap();
        assert_eq!(val, "2024-01-15");

        let enc = |days: i32| {
            let arr: ArrayRef = Arc::new(Date32Array::from(vec![days]));
            encode_value(arr.as_ref(), 0).unwrap()
        };
        assert_eq!(enc(2_932_896), "9999-12-31");
        assert_eq!(enc(-719_162), "0001-01-01");
        assert_eq!(enc(-719_163), "0001-12-31 BC");
        assert_eq!(enc(-735_160), "0044-03-15 BC");
    }

    #[test]
    fn test_encode_timestamp_values() {
        // 2024-01-15 10:30:00 UTC as microseconds from epoch
        let micros = 1705314600_i64 * 1_000_000;
        let arr: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![micros]));
        let val = encode_value(arr.as_ref(), 0).unwrap();
        assert_eq!(val, "2024-01-15 10:30:00");
    }

    #[test]
    fn test_encode_timestamp_fraction_and_pre_epoch() {
        let enc = |micros: i64| {
            let arr: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![micros]));
            encode_value(arr.as_ref(), 0).unwrap()
        };
        assert_eq!(enc(981_173_106_789_012), "2001-02-03 04:05:06.789012");
        assert_eq!(enc(1_500_000), "1970-01-01 00:00:01.5");
        assert_eq!(enc(-1), "1969-12-31 23:59:59.999999");
        assert_eq!(enc(-1_000_000), "1969-12-31 23:59:59");
    }

    #[test]
    fn test_encode_timestamp_outside_the_nanosecond_range() {
        use arrow::array::TimestampMillisecondArray;
        // Microseconds from the epoch, as PostgreSQL's COPY delivers them.
        let enc = |micros: i64| {
            let arr: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![micros]));
            encode_value(arr.as_ref(), 0).unwrap()
        };
        assert_eq!(enc(253_402_300_799_000_000), "9999-12-31 23:59:59");
        assert_eq!(enc(-62_135_596_800_000_000), "0001-01-01 00:00:00");
        assert_eq!(enc(-14_817_470_400_000_000), "1500-06-15 12:00:00");
        assert_eq!(
            enc(-62_135_596_800_000_001),
            "0001-12-31 23:59:59.999999 BC"
        );

        let secs: ArrayRef = Arc::new(TimestampSecondArray::from(vec![253_402_300_799]));
        assert_eq!(
            encode_value(secs.as_ref(), 0).unwrap(),
            "9999-12-31 23:59:59"
        );
        let millis: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![-62_135_596_799_500]));
        assert_eq!(
            encode_value(millis.as_ref(), 0).unwrap(),
            "0001-01-01 00:00:00.5"
        );
    }

    #[test]
    fn test_encode_record_batches_produces_correct_rows() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", arrow_types::DataType::Int32, false),
            Field::new("name", arrow_types::DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["Alice", "Bob"])),
            ],
        )
        .unwrap();

        let field_info = Arc::new(vec![
            FieldInfo::new("id".to_string(), None, None, Type::INT4, FieldFormat::Text),
            FieldInfo::new(
                "name".to_string(),
                None,
                None,
                Type::VARCHAR,
                FieldFormat::Text,
            ),
        ]);

        let (rows, count) = encode_record_batches(&field_info, &[batch]).unwrap();
        assert_eq!(count, 2);
        assert_eq!(rows.len(), 2);
    }

    fn numeric(value: i128, scale: i8) -> Vec<u8> {
        let mut out = BytesMut::new();
        numeric_to_sql(value, scale, &mut out);
        out.to_vec()
    }

    /// `ndigits, weight, sign, dscale, digits...` as big-endian 16-bit words.
    fn words(w: &[i32]) -> Vec<u8> {
        w.iter().flat_map(|v| (*v as u16).to_be_bytes()).collect()
    }

    #[test]
    fn numeric_binary_form() {
        assert_eq!(numeric(0, 0), words(&[0, 0, 0, 0]));
        assert_eq!(numeric(0, 2), words(&[0, 0, 0, 2]));
        // -123.45: 123 | 4500, weight 0.
        assert_eq!(numeric(-12_345, 2), words(&[2, 0, 0x4000, 2, 123, 4500]));
        // 12345.6 at scale 2: 1 | 2345 | 6000.
        assert_eq!(numeric(1_234_560, 2), words(&[3, 1, 0, 2, 1, 2345, 6000]));
        // 0.05: one digit 500 at weight -1.
        assert_eq!(numeric(5, 2), words(&[1, -1, 0, 2, 500]));
        // 0.00001234 at scale 8: digits 0 | 1234 → weight -2.
        assert_eq!(numeric(1_234, 8), words(&[1, -2, 0, 8, 1234]));
        // 10000 at scale 0: trailing zero digits are dropped.
        assert_eq!(numeric(10_000, 0), words(&[1, 1, 0, 0, 1]));
        // 38 nines.
        let big = 10i128.pow(38) - 1;
        let mut expected = vec![10, 9, 0, 0, 99];
        expected.extend([9999; 9]);
        assert_eq!(numeric(big, 0), words(&expected));
        // Negative scale: 12 × 10^3.
        assert_eq!(numeric(12, -3), words(&[2, 1, 0, 0, 1, 2000]));
    }

    fn binary_cells(field: FieldInfo, array: ArrayRef) -> PgWireResult<Vec<Option<Vec<u8>>>> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "c",
            array.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
        let (rows, _) = encode_record_batches(&Arc::new(vec![field]), &[batch])?;
        Ok(rows
            .into_iter()
            .map(|r| {
                let data = r.unwrap().data;
                let len = i32::from_be_bytes(data[..4].try_into().unwrap());
                (len >= 0).then(|| data[4..].to_vec())
            })
            .collect())
    }

    fn binary_field(ty: Type) -> FieldInfo {
        FieldInfo::new("c".to_string(), None, None, ty, FieldFormat::Binary)
    }

    #[test]
    fn binary_numeric_null_and_decimal64() {
        let d128: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(5), None])
                .with_precision_and_scale(10, 2)
                .unwrap(),
        );
        assert_eq!(
            binary_cells(binary_field(Type::NUMERIC), d128).unwrap(),
            vec![Some(words(&[1, -1, 0, 2, 500])), None]
        );
        let d64: ArrayRef = Arc::new(
            Decimal64Array::from(vec![Some(-12_345)])
                .with_precision_and_scale(15, 2)
                .unwrap(),
        );
        assert_eq!(
            binary_cells(binary_field(Type::NUMERIC), d64).unwrap(),
            vec![Some(words(&[2, 0, 0x4000, 2, 123, 4500]))]
        );
    }

    #[test]
    fn binary_refuses_types_without_an_encoding() {
        let floats: ArrayRef = Arc::new(Float64Array::from(vec![1.5]));
        let err = binary_cells(binary_field(Type::INT4), floats).unwrap_err();
        let PgWireError::UserError(info) = err else {
            panic!("{err:?}")
        };
        assert_eq!(info.code, "0A000");
        assert!(info.message.contains("int4"), "{}", info.message);
        // A type Arneb never declares, even with matching data.
        let ints: ArrayRef = Arc::new(Int64Array::from(vec![Some(1)]));
        assert!(binary_cells(binary_field(Type::INTERVAL), ints).is_err());
        // Nested arrays have no binary form here.
        let inner = ListArray::from_iter_primitive::<arrow_types::Int64Type, _, _>(vec![Some(
            vec![Some(1)],
        )]);
        let nested: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new("item", inner.data_type().clone(), true)),
            arrow::buffer::OffsetBuffer::from_lengths([1]),
            Arc::new(inner),
            None,
        ));
        assert!(binary_cells(binary_field(Type::INT8_ARRAY), nested).is_err());
    }

    #[test]
    fn binary_empty_array_has_no_dimensions() {
        let empty =
            ListArray::from_iter_primitive::<arrow_types::Int64Type, _, _>(vec![Some(Vec::<
                Option<i64>,
            >::new(
            ))]);
        let cells = binary_cells(binary_field(Type::INT8_ARRAY), Arc::new(empty)).unwrap();
        // ndim 0, no nulls, element oid 20.
        assert_eq!(
            cells,
            vec![Some([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 20].to_vec())]
        );
    }

    #[test]
    fn text_results_stay_byte_identical() {
        // A text column keeps the bytes the text path produced before
        // binary support: the value, not a binary form.
        let field = FieldInfo::new("c".to_string(), None, None, Type::INT8, FieldFormat::Text);
        let ints: ArrayRef = Arc::new(Int64Array::from(vec![Some(42), None]));
        assert_eq!(
            binary_cells(field, ints).unwrap(),
            vec![Some(b"42".to_vec()), None]
        );
    }

    #[test]
    fn result_format_codes() {
        let fields = vec![
            FieldInfo::new("a".to_string(), None, None, Type::INT4, FieldFormat::Text),
            FieldInfo::new("b".to_string(), None, None, Type::INT4, FieldFormat::Text),
        ];
        let formats = |f: &Format| {
            with_result_formats(fields.clone(), f)
                .map(|v| v.iter().map(|f| f.format()).collect::<Vec<_>>())
        };
        use FieldFormat::{Binary, Text};
        assert_eq!(formats(&Format::UnifiedText).unwrap(), vec![Text, Text]);
        assert_eq!(
            formats(&Format::UnifiedBinary).unwrap(),
            vec![Binary, Binary]
        );
        assert_eq!(
            formats(&Format::Individual(vec![1, 0])).unwrap(),
            vec![Binary, Text]
        );
        assert!(formats(&Format::Individual(vec![1, 0, 1])).is_err());
    }
}
