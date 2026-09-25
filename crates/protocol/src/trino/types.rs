//! Arrow → Trino type mapping and Trino client-protocol JSON value encoding.
//!
//! Trino clients decode each row cell according to the column's type
//! string (python client) or `typeSignature` (JDBC). The encodings here
//! follow what a real Trino coordinator emits:
//!
//! | Trino type                    | JSON encoding                          |
//! |-------------------------------|----------------------------------------|
//! | tinyint/smallint/integer/bigint | number                               |
//! | real/double                   | number; `"NaN"`, `"Infinity"`, `"-Infinity"` as strings |
//! | decimal(p,s)                  | string (`"123.45"`)                    |
//! | varchar                       | string                                 |
//! | varbinary                     | base64 string                          |
//! | boolean                       | `true` / `false`                       |
//! | date                          | `"YYYY-MM-DD"`                         |
//! | time(p)                       | `"HH:MM:SS[.fff]"`                     |
//! | timestamp(p)                  | `"YYYY-MM-DD HH:MM:SS[.fff]"`          |
//! | timestamp(p) with time zone   | `"YYYY-MM-DD HH:MM:SS[.fff] UTC"`      |
//! | array(T)                      | JSON array                             |
//! | map(K,V)                      | JSON object keyed by the key's string form |
//! | row(...)                      | JSON array of field values             |

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Date32Array, Date64Array, Float16Array, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, LargeListArray, ListArray,
    MapArray, StructArray, Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray,
    Time64NanosecondArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array,
    UInt8Array,
};
use arrow::datatypes::{DataType as ArrowType, Field, TimeUnit};
use base64::Engine as _;
use serde_json::{json, Map, Value};

/// Unbounded varchar length Trino reports in the `varchar` type signature.
const UNBOUNDED_VARCHAR: i64 = 2_147_483_647;

/// A Trino client-visible type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TrinoType {
    Unknown,
    Boolean,
    TinyInt,
    SmallInt,
    Integer,
    BigInt,
    Real,
    Double,
    Decimal(u8, i8),
    Varchar,
    Varbinary,
    Date,
    Time(u8),
    Timestamp(u8),
    TimestampTz(u8),
    Array(Box<TrinoType>),
    Map(Box<TrinoType>, Box<TrinoType>),
    Row(Vec<(String, TrinoType)>),
}

fn unit_precision(unit: &TimeUnit) -> u8 {
    match unit {
        TimeUnit::Second => 0,
        TimeUnit::Millisecond => 3,
        TimeUnit::Microsecond => 6,
        TimeUnit::Nanosecond => 9,
    }
}

impl TrinoType {
    /// Maps an Arrow data type to the Trino type a client should see.
    pub(crate) fn from_arrow(dt: &ArrowType) -> Self {
        match dt {
            ArrowType::Null => Self::Unknown,
            ArrowType::Boolean => Self::Boolean,
            ArrowType::Int8 => Self::TinyInt,
            ArrowType::Int16 | ArrowType::UInt8 => Self::SmallInt,
            ArrowType::Int32 | ArrowType::UInt16 => Self::Integer,
            ArrowType::Int64 | ArrowType::UInt32 | ArrowType::UInt64 => Self::BigInt,
            ArrowType::Float16 | ArrowType::Float32 => Self::Real,
            ArrowType::Float64 => Self::Double,
            ArrowType::Decimal32(p, s)
            | ArrowType::Decimal64(p, s)
            | ArrowType::Decimal128(p, s)
            | ArrowType::Decimal256(p, s) => Self::Decimal(*p, *s),
            ArrowType::Utf8 | ArrowType::LargeUtf8 | ArrowType::Utf8View => Self::Varchar,
            ArrowType::Binary
            | ArrowType::LargeBinary
            | ArrowType::BinaryView
            | ArrowType::FixedSizeBinary(_) => Self::Varbinary,
            ArrowType::Date32 | ArrowType::Date64 => Self::Date,
            ArrowType::Time32(unit) | ArrowType::Time64(unit) => Self::Time(unit_precision(unit)),
            ArrowType::Timestamp(unit, None) => Self::Timestamp(unit_precision(unit)),
            ArrowType::Timestamp(unit, Some(_)) => Self::TimestampTz(unit_precision(unit)),
            ArrowType::List(f) | ArrowType::LargeList(f) | ArrowType::FixedSizeList(f, _) => {
                Self::Array(Box::new(Self::from_arrow(f.data_type())))
            }
            ArrowType::Map(entries, _) => match entries.data_type() {
                ArrowType::Struct(fields) if fields.len() == 2 => Self::Map(
                    Box::new(Self::from_arrow(fields[0].data_type())),
                    Box::new(Self::from_arrow(fields[1].data_type())),
                ),
                _ => Self::Varchar,
            },
            ArrowType::Struct(fields) => Self::Row(
                fields
                    .iter()
                    .map(|f| (f.name().clone(), Self::from_arrow(f.data_type())))
                    .collect(),
            ),
            ArrowType::Dictionary(_, value) => Self::from_arrow(value),
            // Intervals, durations, unions, …: rendered as strings.
            _ => Self::Varchar,
        }
    }

    /// The `rawType` of the type signature.
    fn raw_type(&self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Boolean => "boolean",
            Self::TinyInt => "tinyint",
            Self::SmallInt => "smallint",
            Self::Integer => "integer",
            Self::BigInt => "bigint",
            Self::Real => "real",
            Self::Double => "double",
            Self::Decimal(..) => "decimal",
            Self::Varchar => "varchar",
            Self::Varbinary => "varbinary",
            Self::Date => "date",
            Self::Time(_) => "time",
            Self::Timestamp(_) => "timestamp",
            Self::TimestampTz(_) => "timestamp with time zone",
            Self::Array(_) => "array",
            Self::Map(..) => "map",
            Self::Row(_) => "row",
        }
    }

    /// The Trino type display string, e.g. `decimal(12,2)` or `timestamp(3)`.
    pub(crate) fn display(&self) -> String {
        match self {
            Self::Decimal(p, s) => format!("decimal({p},{s})"),
            Self::Time(p) => format!("time({p})"),
            Self::Timestamp(p) => format!("timestamp({p})"),
            Self::TimestampTz(p) => format!("timestamp({p}) with time zone"),
            Self::Array(t) => format!("array({})", t.display()),
            Self::Map(k, v) => format!("map({}, {})", k.display(), v.display()),
            Self::Row(fields) => {
                let inner: Vec<String> = fields
                    .iter()
                    .map(|(n, t)| format!("\"{}\" {}", n.replace('"', "\"\""), t.display()))
                    .collect();
                format!("row({})", inner.join(", "))
            }
            other => other.raw_type().to_string(),
        }
    }

    /// The JSON `typeSignature` object (`ClientTypeSignature` in Trino).
    pub(crate) fn signature(&self) -> Value {
        let long = |v: i64| json!({"kind": "LONG", "value": v});
        let ty = |t: &TrinoType| json!({"kind": "TYPE", "value": t.signature()});
        let arguments: Vec<Value> = match self {
            Self::Varchar => vec![long(UNBOUNDED_VARCHAR)],
            Self::Decimal(p, s) => vec![long(*p as i64), long(*s as i64)],
            Self::Time(p) | Self::Timestamp(p) | Self::TimestampTz(p) => vec![long(*p as i64)],
            Self::Array(t) => vec![ty(t)],
            Self::Map(k, v) => vec![ty(k), ty(v)],
            Self::Row(fields) => fields
                .iter()
                .map(|(n, t)| {
                    json!({
                        "kind": "NAMED_TYPE",
                        "value": {"fieldName": {"name": n}, "typeSignature": t.signature()}
                    })
                })
                .collect(),
            _ => vec![],
        };
        json!({"rawType": self.raw_type(), "arguments": arguments})
    }

    /// The `java.sql.Types` code the Trino JDBC driver reports for this type.
    pub(crate) fn jdbc_type_code(&self) -> i64 {
        match self {
            Self::Unknown => 0,
            Self::Boolean => 16,
            Self::TinyInt => -6,
            Self::SmallInt => 5,
            Self::Integer => 4,
            Self::BigInt => -5,
            Self::Real => 7,
            Self::Double => 8,
            Self::Decimal(..) => 3,
            Self::Varchar => 12,
            Self::Varbinary => -3,
            Self::Date => 91,
            Self::Time(_) => 92,
            Self::Timestamp(_) => 93,
            Self::TimestampTz(_) => 2014,
            Self::Array(_) => 2003,
            Self::Map(..) | Self::Row(_) => 2000,
        }
    }
}

/// Maps an Arneb logical type to the Trino type a client should see.
pub(crate) fn trino_type_of(dt: &arneb_common::types::DataType) -> TrinoType {
    TrinoType::from_arrow(&ArrowType::from(dt.clone()))
}

/// Builds the JSON `columns` entry for one output column.
pub(crate) fn column_json(name: &str, ty: &TrinoType) -> Value {
    json!({
        "name": name,
        "type": ty.display(),
        "typeSignature": ty.signature(),
    })
}

/// Unwraps dictionary-encoded arrays so the encoder only sees value types.
pub(crate) fn materialize(array: &ArrayRef) -> ArrayRef {
    match array.data_type() {
        ArrowType::Dictionary(_, value) => {
            arrow::compute::cast(array, value).unwrap_or_else(|_| array.clone())
        }
        _ => array.clone(),
    }
}

fn float_json(v: f64) -> Value {
    if v.is_nan() {
        Value::String("NaN".into())
    } else if v.is_infinite() {
        Value::String(if v > 0.0 { "Infinity" } else { "-Infinity" }.into())
    } else {
        serde_json::Number::from_f64(v)
            .map(Value::Number)
            .unwrap_or(Value::Null)
    }
}

fn format_datetime(nanos_since_epoch: i128, precision: u8, with_date: bool) -> String {
    let secs = nanos_since_epoch.div_euclid(1_000_000_000) as i64;
    let nsec = nanos_since_epoch.rem_euclid(1_000_000_000) as u32;
    let dt = chrono::DateTime::from_timestamp(secs, nsec)
        .unwrap_or_default()
        .naive_utc();
    let base = if with_date {
        dt.format("%Y-%m-%d %H:%M:%S").to_string()
    } else {
        dt.format("%H:%M:%S").to_string()
    };
    if precision == 0 {
        return base;
    }
    let frac = format!("{nsec:09}");
    format!("{base}.{}", &frac[..precision as usize])
}

fn timestamp_nanos(array: &dyn Array, unit: &TimeUnit, row: usize) -> i128 {
    let any = array.as_any();
    match unit {
        TimeUnit::Second => {
            any.downcast_ref::<TimestampSecondArray>()
                .map(|a| a.value(row))
                .unwrap_or_default() as i128
                * 1_000_000_000
        }
        TimeUnit::Millisecond => {
            any.downcast_ref::<TimestampMillisecondArray>()
                .map(|a| a.value(row))
                .unwrap_or_default() as i128
                * 1_000_000
        }
        TimeUnit::Microsecond => {
            any.downcast_ref::<TimestampMicrosecondArray>()
                .map(|a| a.value(row))
                .unwrap_or_default() as i128
                * 1_000
        }
        TimeUnit::Nanosecond => any
            .downcast_ref::<TimestampNanosecondArray>()
            .map(|a| a.value(row))
            .unwrap_or_default() as i128,
    }
}

fn time_nanos(array: &dyn Array, row: usize) -> i128 {
    let any = array.as_any();
    if let Some(a) = any.downcast_ref::<Time32SecondArray>() {
        a.value(row) as i128 * 1_000_000_000
    } else if let Some(a) = any.downcast_ref::<Time32MillisecondArray>() {
        a.value(row) as i128 * 1_000_000
    } else if let Some(a) = any.downcast_ref::<Time64MicrosecondArray>() {
        a.value(row) as i128 * 1_000
    } else if let Some(a) = any.downcast_ref::<Time64NanosecondArray>() {
        a.value(row) as i128
    } else {
        0
    }
}

macro_rules! num {
    ($array:expr, $ty:ty, $row:expr) => {
        $array
            .as_any()
            .downcast_ref::<$ty>()
            .map(|a| json!(a.value($row)))
            .unwrap_or(Value::Null)
    };
}

fn display_string(array: &dyn Array, row: usize) -> Value {
    arrow::util::display::array_value_to_string(array, row)
        .map(Value::String)
        .unwrap_or(Value::Null)
}

/// Encodes one cell as its Trino client-protocol JSON value.
pub(crate) fn encode_cell(array: &dyn Array, row: usize) -> Value {
    if array.is_null(row) {
        return Value::Null;
    }
    match array.data_type() {
        ArrowType::Null => Value::Null,
        ArrowType::Boolean => array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .map(|a| Value::Bool(a.value(row)))
            .unwrap_or(Value::Null),
        ArrowType::Int8 => num!(array, Int8Array, row),
        ArrowType::Int16 => num!(array, Int16Array, row),
        ArrowType::Int32 => num!(array, Int32Array, row),
        ArrowType::Int64 => num!(array, Int64Array, row),
        ArrowType::UInt8 => num!(array, UInt8Array, row),
        ArrowType::UInt16 => num!(array, UInt16Array, row),
        ArrowType::UInt32 => num!(array, UInt32Array, row),
        ArrowType::UInt64 => num!(array, UInt64Array, row),
        ArrowType::Float16 => array
            .as_any()
            .downcast_ref::<Float16Array>()
            .map(|a| float_json(a.value(row).to_f64()))
            .unwrap_or(Value::Null),
        ArrowType::Float32 => array
            .as_any()
            .downcast_ref::<Float32Array>()
            .map(|a| float_json(a.value(row) as f64))
            .unwrap_or(Value::Null),
        ArrowType::Float64 => array
            .as_any()
            .downcast_ref::<Float64Array>()
            .map(|a| float_json(a.value(row)))
            .unwrap_or(Value::Null),
        ArrowType::Utf8 => Value::String(array.as_string::<i32>().value(row).to_string()),
        ArrowType::LargeUtf8 => Value::String(array.as_string::<i64>().value(row).to_string()),
        ArrowType::Utf8View => Value::String(array.as_string_view().value(row).to_string()),
        ArrowType::Binary => Value::String(
            base64::engine::general_purpose::STANDARD.encode(array.as_binary::<i32>().value(row)),
        ),
        ArrowType::LargeBinary => Value::String(
            base64::engine::general_purpose::STANDARD.encode(array.as_binary::<i64>().value(row)),
        ),
        ArrowType::BinaryView => Value::String(
            base64::engine::general_purpose::STANDARD.encode(array.as_binary_view().value(row)),
        ),
        ArrowType::FixedSizeBinary(_) => Value::String(
            base64::engine::general_purpose::STANDARD
                .encode(array.as_fixed_size_binary().value(row)),
        ),
        ArrowType::Date32 => array
            .as_any()
            .downcast_ref::<Date32Array>()
            .and_then(|a| a.value_as_date(row))
            .map(|d| Value::String(d.format("%Y-%m-%d").to_string()))
            .unwrap_or(Value::Null),
        ArrowType::Date64 => array
            .as_any()
            .downcast_ref::<Date64Array>()
            .and_then(|a| a.value_as_date(row))
            .map(|d| Value::String(d.format("%Y-%m-%d").to_string()))
            .unwrap_or(Value::Null),
        ArrowType::Time32(unit) | ArrowType::Time64(unit) => Value::String(format_datetime(
            time_nanos(array, row),
            unit_precision(unit),
            false,
        )),
        ArrowType::Timestamp(unit, tz) => {
            let text = format_datetime(
                timestamp_nanos(array, unit, row),
                unit_precision(unit),
                true,
            );
            // Arrow stores the UTC instant; render it in UTC and label it.
            Value::String(match tz {
                Some(_) => format!("{text} UTC"),
                None => text,
            })
        }
        ArrowType::Decimal32(..)
        | ArrowType::Decimal64(..)
        | ArrowType::Decimal128(..)
        | ArrowType::Decimal256(..) => display_string(array, row),
        ArrowType::List(_) => {
            let list = array.as_any().downcast_ref::<ListArray>();
            list.map(|l| encode_list(&l.value(row)))
                .unwrap_or(Value::Null)
        }
        ArrowType::LargeList(_) => {
            let list = array.as_any().downcast_ref::<LargeListArray>();
            list.map(|l| encode_list(&l.value(row)))
                .unwrap_or(Value::Null)
        }
        ArrowType::FixedSizeList(..) => encode_list(&array.as_fixed_size_list().value(row)),
        ArrowType::Map(..) => {
            let Some(map) = array.as_any().downcast_ref::<MapArray>() else {
                return Value::Null;
            };
            let entries = map.value(row);
            let keys = materialize(entries.column(0));
            let values = materialize(entries.column(1));
            let mut out = Map::new();
            for i in 0..entries.len() {
                let key = match encode_cell(keys.as_ref(), i) {
                    Value::String(s) => s,
                    other => other.to_string(),
                };
                out.insert(key, encode_cell(values.as_ref(), i));
            }
            Value::Object(out)
        }
        ArrowType::Struct(_) => {
            let Some(s) = array.as_any().downcast_ref::<StructArray>() else {
                return Value::Null;
            };
            Value::Array(
                s.columns()
                    .iter()
                    .map(|c| encode_cell(materialize(c).as_ref(), row))
                    .collect(),
            )
        }
        ArrowType::Dictionary(..) => {
            let single = array.slice(row, 1);
            let value = materialize(&single);
            encode_cell(value.as_ref(), 0)
        }
        _ => display_string(array, row),
    }
}

fn encode_list(values: &ArrayRef) -> Value {
    let values = materialize(values);
    Value::Array(
        (0..values.len())
            .map(|i| encode_cell(values.as_ref(), i))
            .collect(),
    )
}

/// Output column of a statement: name plus the Trino type clients see.
#[derive(Debug, Clone)]
pub(crate) struct OutputColumn {
    pub(crate) name: String,
    pub(crate) ty: TrinoType,
}

impl OutputColumn {
    pub(crate) fn from_field(field: &Field) -> Self {
        Self {
            name: field.name().clone(),
            ty: TrinoType::from_arrow(field.data_type()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Decimal128Array, StringArray};
    use std::sync::Arc;

    #[test]
    fn type_strings_and_signatures() {
        assert_eq!(TrinoType::from_arrow(&ArrowType::Int64).display(), "bigint");
        assert_eq!(
            TrinoType::from_arrow(&ArrowType::Decimal128(12, 2)).display(),
            "decimal(12,2)"
        );
        assert_eq!(
            TrinoType::from_arrow(&ArrowType::Timestamp(TimeUnit::Millisecond, None)).display(),
            "timestamp(3)"
        );
        assert_eq!(
            TrinoType::from_arrow(&ArrowType::Timestamp(
                TimeUnit::Microsecond,
                Some("UTC".into())
            ))
            .display(),
            "timestamp(6) with time zone"
        );
        let sig = TrinoType::Varchar.signature();
        assert_eq!(sig["rawType"], "varchar");
        assert_eq!(sig["arguments"][0]["kind"], "LONG");
        assert_eq!(sig["arguments"][0]["value"], UNBOUNDED_VARCHAR);
        let sig = TrinoType::Decimal(15, 2).signature();
        assert_eq!(sig["arguments"][1]["value"], 2);
        let arr = TrinoType::Array(Box::new(TrinoType::BigInt));
        assert_eq!(arr.display(), "array(bigint)");
        assert_eq!(arr.signature()["arguments"][0]["kind"], "TYPE");
    }

    #[test]
    fn encodes_scalars_like_trino() {
        let ints: ArrayRef = Arc::new(Int64Array::from(vec![Some(42), None]));
        assert_eq!(encode_cell(ints.as_ref(), 0), json!(42));
        assert_eq!(encode_cell(ints.as_ref(), 1), Value::Null);

        let dec: ArrayRef = Arc::new(
            Decimal128Array::from(vec![12345_i128, -5])
                .with_precision_and_scale(12, 2)
                .unwrap(),
        );
        assert_eq!(encode_cell(dec.as_ref(), 0), json!("123.45"));
        assert_eq!(encode_cell(dec.as_ref(), 1), json!("-0.05"));

        let dates: ArrayRef = Arc::new(Date32Array::from(vec![9_131]));
        assert_eq!(encode_cell(dates.as_ref(), 0), json!("1995-01-01"));

        let ts: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![1_500]));
        assert_eq!(
            encode_cell(ts.as_ref(), 0),
            json!("1970-01-01 00:00:01.500")
        );
        let ts: ArrayRef = Arc::new(TimestampSecondArray::from(vec![-1]));
        assert_eq!(encode_cell(ts.as_ref(), 0), json!("1969-12-31 23:59:59"));

        let floats: ArrayRef = Arc::new(Float64Array::from(vec![1.5, f64::NAN, f64::INFINITY]));
        assert_eq!(encode_cell(floats.as_ref(), 0), json!(1.5));
        assert_eq!(encode_cell(floats.as_ref(), 1), json!("NaN"));
        assert_eq!(encode_cell(floats.as_ref(), 2), json!("Infinity"));

        let strings: ArrayRef = Arc::new(StringArray::from(vec!["héllo"]));
        assert_eq!(encode_cell(strings.as_ref(), 0), json!("héllo"));

        let bools: ArrayRef = Arc::new(BooleanArray::from(vec![true]));
        assert_eq!(encode_cell(bools.as_ref(), 0), json!(true));

        let bin: ArrayRef = Arc::new(arrow::array::BinaryArray::from(vec![b"ab".as_ref()]));
        assert_eq!(encode_cell(bin.as_ref(), 0), json!("YWI="));
    }
}
