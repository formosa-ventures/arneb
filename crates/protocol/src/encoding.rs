use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes as arrow_types;
use arrow::record_batch::RecordBatch;
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo};
use pgwire::api::Type;
use pgwire::error::PgWireResult;

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
        DataType::Binary => Type::BYTEA,
        DataType::Date32 => Type::DATE,
        DataType::Timestamp { .. } => Type::TIMESTAMP,
        _ => Type::TEXT,
    }
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
            let date = chrono::NaiveDate::from_num_days_from_ce_opt(days + 719_163)
                .unwrap_or(chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
            Some(date.format("%Y-%m-%d").to_string())
        }
        arrow_types::DataType::Timestamp(unit, _) => {
            let value = match unit {
                arrow_types::TimeUnit::Second => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampSecondArray>()
                        .unwrap();
                    arr.value(row) * 1_000_000_000
                }
                arrow_types::TimeUnit::Millisecond => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .unwrap();
                    arr.value(row) * 1_000_000
                }
                arrow_types::TimeUnit::Microsecond => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .unwrap();
                    arr.value(row) * 1_000
                }
                arrow_types::TimeUnit::Nanosecond => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .unwrap();
                    arr.value(row)
                }
            };
            let secs = value / 1_000_000_000;
            let nsecs = (value % 1_000_000_000) as u32;
            let dt = chrono::DateTime::from_timestamp(secs, nsecs)
                .unwrap_or_default()
                .naive_utc();
            Some(dt.format("%Y-%m-%d %H:%M:%S").to_string())
        }
        arrow_types::DataType::Decimal128(precision, scale) => {
            let arr = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
            let raw = arr.value(row);
            format_decimal128(raw, *precision, *scale)
        }
        _ => Some(format!("{array:?}")),
    }
}

fn format_decimal128(value: i128, _precision: u8, scale: i8) -> Option<String> {
    if scale <= 0 {
        let multiplier = 10i128.pow((-scale) as u32);
        return Some((value * multiplier).to_string());
    }
    let scale = scale as u32;
    let divisor = 10i128.pow(scale);
    let integer_part = value / divisor;
    let fractional_part = (value % divisor).abs();
    Some(format!(
        "{}.{:0>width$}",
        integer_part,
        fractional_part,
        width = scale as usize
    ))
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
        let num_cols = batch.num_columns();
        total_count += num_rows;

        for row_idx in 0..num_rows {
            let mut encoder = DataRowEncoder::new(schema.clone());
            for col_idx in 0..num_cols {
                let array = batch.column(col_idx);
                let value = encode_value(array.as_ref(), row_idx);
                encoder.encode_field(&value)?;
            }
            rows.push(encoder.finish());
        }
    }

    Ok((rows, total_count))
}

#[cfg(test)]
mod tests {
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
    fn test_encode_date32_values() {
        // 2024-01-15 is day 19737 from epoch (1970-01-01)
        let arr: ArrayRef = Arc::new(Date32Array::from(vec![19737]));
        let val = encode_value(arr.as_ref(), 0).unwrap();
        assert_eq!(val, "2024-01-15");
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
