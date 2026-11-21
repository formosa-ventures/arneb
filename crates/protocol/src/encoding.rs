use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes as arrow_types;
use arrow::record_batch::RecordBatch;
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo};
use pgwire::api::Type;
use pgwire::error::{PgWireError, PgWireResult};

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

/// The error for a struct (composite) value reaching the client: composites
/// stay inside the plan (`(expr).field` reads them).
fn composite_error() -> PgWireError {
    PgWireError::UserError(Box::new(pgwire::error::ErrorInfo::new(
        "ERROR".to_string(),
        "0A000".to_string(),
        "composite values are not supported over pgwire".to_string(),
    )))
}

fn has_struct(dt: &DataType) -> bool {
    match dt {
        DataType::Struct(_) => true,
        DataType::List(element) => has_struct(element),
        _ => false,
    }
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

/// Refuses result columns of a struct type (or an array of one): there is no
/// pgwire encoding for composite values.
pub fn refuse_composite_columns(columns: &[ColumnInfo]) -> PgWireResult<()> {
    if columns.iter().any(|c| has_struct(&c.data_type)) {
        return Err(composite_error());
    }
    Ok(())
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
        if batch
            .schema()
            .fields()
            .iter()
            .any(|f| has_arrow_struct(f.data_type()))
        {
            return Err(composite_error());
        }
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
        let mut encoder = DataRowEncoder::new(schema.clone());
        for row_idx in 0..num_rows {
            for (col_idx, array) in columns.iter().enumerate() {
                let value = encode_value(array.as_ref(), row_idx);
                let field = &schema[col_idx];
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
}
