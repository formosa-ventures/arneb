//! Shared data types for the trino-alt query engine.

use std::fmt;

use thiserror::Error;

/// Time unit for timestamp types, matching Arrow's TimeUnit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TimeUnit {
    /// Seconds.
    Second,
    /// Milliseconds.
    Millisecond,
    /// Microseconds.
    Microsecond,
    /// Nanoseconds.
    Nanosecond,
}

impl From<TimeUnit> for arrow::datatypes::TimeUnit {
    fn from(unit: TimeUnit) -> Self {
        match unit {
            TimeUnit::Second => arrow::datatypes::TimeUnit::Second,
            TimeUnit::Millisecond => arrow::datatypes::TimeUnit::Millisecond,
            TimeUnit::Microsecond => arrow::datatypes::TimeUnit::Microsecond,
            TimeUnit::Nanosecond => arrow::datatypes::TimeUnit::Nanosecond,
        }
    }
}

impl From<arrow::datatypes::TimeUnit> for TimeUnit {
    fn from(unit: arrow::datatypes::TimeUnit) -> Self {
        match unit {
            arrow::datatypes::TimeUnit::Second => TimeUnit::Second,
            arrow::datatypes::TimeUnit::Millisecond => TimeUnit::Millisecond,
            arrow::datatypes::TimeUnit::Microsecond => TimeUnit::Microsecond,
            arrow::datatypes::TimeUnit::Nanosecond => TimeUnit::Nanosecond,
        }
    }
}

impl fmt::Display for TimeUnit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TimeUnit::Second => write!(f, "Second"),
            TimeUnit::Millisecond => write!(f, "Millisecond"),
            TimeUnit::Microsecond => write!(f, "Microsecond"),
            TimeUnit::Nanosecond => write!(f, "Nanosecond"),
        }
    }
}

/// SQL data type system. Maintains SQL semantics (precision, scale, etc.)
/// while providing conversion to/from Arrow types.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum DataType {
    /// SQL NULL type.
    Null,
    /// SQL BOOLEAN.
    Boolean,
    /// SQL TINYINT (8-bit signed integer).
    Int8,
    /// SQL SMALLINT (16-bit signed integer).
    Int16,
    /// SQL INTEGER (32-bit signed integer).
    Int32,
    /// SQL BIGINT (64-bit signed integer).
    Int64,
    /// SQL REAL (32-bit floating point).
    Float32,
    /// SQL DOUBLE (64-bit floating point).
    Float64,
    /// SQL DECIMAL with precision and scale.
    Decimal128 {
        /// Total number of digits.
        precision: u8,
        /// Number of digits after the decimal point.
        scale: i8,
    },
    /// SQL VARCHAR / TEXT.
    Utf8,
    /// SQL VARCHAR for large strings.
    LargeUtf8,
    /// SQL VARBINARY.
    Binary,
    /// SQL DATE (days since epoch).
    Date32,
    /// SQL TIMESTAMP with optional timezone.
    Timestamp {
        /// Time resolution.
        unit: TimeUnit,
        /// Optional timezone identifier.
        timezone: Option<String>,
    },
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataType::Null => write!(f, "Null"),
            DataType::Boolean => write!(f, "Boolean"),
            DataType::Int8 => write!(f, "Int8"),
            DataType::Int16 => write!(f, "Int16"),
            DataType::Int32 => write!(f, "Int32"),
            DataType::Int64 => write!(f, "Int64"),
            DataType::Float32 => write!(f, "Float32"),
            DataType::Float64 => write!(f, "Float64"),
            DataType::Decimal128 { precision, scale } => {
                write!(f, "Decimal128({precision}, {scale})")
            }
            DataType::Utf8 => write!(f, "Utf8"),
            DataType::LargeUtf8 => write!(f, "LargeUtf8"),
            DataType::Binary => write!(f, "Binary"),
            DataType::Date32 => write!(f, "Date32"),
            DataType::Timestamp { unit, timezone } => match timezone {
                Some(tz) => write!(f, "Timestamp({unit}, {tz})"),
                None => write!(f, "Timestamp({unit})"),
            },
        }
    }
}

impl From<DataType> for arrow::datatypes::DataType {
    fn from(dt: DataType) -> Self {
        match dt {
            DataType::Null => arrow::datatypes::DataType::Null,
            DataType::Boolean => arrow::datatypes::DataType::Boolean,
            DataType::Int8 => arrow::datatypes::DataType::Int8,
            DataType::Int16 => arrow::datatypes::DataType::Int16,
            DataType::Int32 => arrow::datatypes::DataType::Int32,
            DataType::Int64 => arrow::datatypes::DataType::Int64,
            DataType::Float32 => arrow::datatypes::DataType::Float32,
            DataType::Float64 => arrow::datatypes::DataType::Float64,
            DataType::Decimal128 { precision, scale } => {
                arrow::datatypes::DataType::Decimal128(precision, scale)
            }
            DataType::Utf8 => arrow::datatypes::DataType::Utf8,
            DataType::LargeUtf8 => arrow::datatypes::DataType::LargeUtf8,
            DataType::Binary => arrow::datatypes::DataType::Binary,
            DataType::Date32 => arrow::datatypes::DataType::Date32,
            DataType::Timestamp { unit, timezone } => {
                arrow::datatypes::DataType::Timestamp(unit.into(), timezone.map(|tz| tz.into()))
            }
        }
    }
}

/// Error returned when an Arrow DataType cannot be converted to our DataType.
#[derive(Debug, Clone, Error)]
#[error("unsupported Arrow type: {0:?}")]
pub struct UnsupportedArrowType(pub arrow::datatypes::DataType);

impl TryFrom<arrow::datatypes::DataType> for DataType {
    type Error = UnsupportedArrowType;

    fn try_from(dt: arrow::datatypes::DataType) -> std::result::Result<Self, Self::Error> {
        match dt {
            arrow::datatypes::DataType::Null => Ok(DataType::Null),
            arrow::datatypes::DataType::Boolean => Ok(DataType::Boolean),
            arrow::datatypes::DataType::Int8 => Ok(DataType::Int8),
            arrow::datatypes::DataType::Int16 => Ok(DataType::Int16),
            arrow::datatypes::DataType::Int32 => Ok(DataType::Int32),
            arrow::datatypes::DataType::Int64 => Ok(DataType::Int64),
            arrow::datatypes::DataType::Float32 => Ok(DataType::Float32),
            arrow::datatypes::DataType::Float64 => Ok(DataType::Float64),
            arrow::datatypes::DataType::Decimal128(precision, scale) => {
                Ok(DataType::Decimal128 { precision, scale })
            }
            arrow::datatypes::DataType::Utf8 => Ok(DataType::Utf8),
            arrow::datatypes::DataType::LargeUtf8 => Ok(DataType::LargeUtf8),
            arrow::datatypes::DataType::Binary => Ok(DataType::Binary),
            arrow::datatypes::DataType::Date32 => Ok(DataType::Date32),
            arrow::datatypes::DataType::Timestamp(unit, tz) => Ok(DataType::Timestamp {
                unit: unit.into(),
                timezone: tz.map(|s| s.to_string()),
            }),
            other => Err(UnsupportedArrowType(other)),
        }
    }
}

/// Error returned when a table reference string cannot be parsed.
#[derive(Debug, Clone, Error)]
#[error("invalid table reference: {reason}")]
pub struct InvalidTableReference {
    /// Description of why parsing failed.
    pub reason: String,
}

/// Reference to a table using up to three-part naming: catalog.schema.table.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableReference {
    /// Optional catalog name (first part of `catalog.schema.table`).
    pub catalog: Option<String>,
    /// Optional schema name (second part of `catalog.schema.table`).
    pub schema: Option<String>,
    /// Table name (required).
    pub table: String,
}

impl TableReference {
    /// Parse a dotted table reference string.
    ///
    /// Supports:
    /// - `"table"` → one-part
    /// - `"schema.table"` → two-part
    /// - `"catalog.schema.table"` → three-part
    ///
    /// Returns an error for empty strings, empty parts, or more than three parts.
    pub fn parse(s: &str) -> Result<Self, InvalidTableReference> {
        if s.is_empty() {
            return Err(InvalidTableReference {
                reason: "empty table reference".to_string(),
            });
        }

        let parts: Vec<&str> = s.split('.').collect();

        if parts.iter().any(|p| p.is_empty()) {
            return Err(InvalidTableReference {
                reason: format!("empty part in table reference: '{s}'"),
            });
        }

        match parts.len() {
            1 => Ok(TableReference {
                catalog: None,
                schema: None,
                table: parts[0].to_string(),
            }),
            2 => Ok(TableReference {
                catalog: None,
                schema: Some(parts[0].to_string()),
                table: parts[1].to_string(),
            }),
            3 => Ok(TableReference {
                catalog: Some(parts[0].to_string()),
                schema: Some(parts[1].to_string()),
                table: parts[2].to_string(),
            }),
            n => Err(InvalidTableReference {
                reason: format!("too many parts ({n}), expected at most 3: '{s}'"),
            }),
        }
    }

    /// Create a simple one-part table reference.
    pub fn table(name: impl Into<String>) -> Self {
        TableReference {
            catalog: None,
            schema: None,
            table: name.into(),
        }
    }
}

impl fmt::Display for TableReference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(catalog) = &self.catalog {
            write!(f, "{catalog}.")?;
        }
        if let Some(schema) = &self.schema {
            write!(f, "{schema}.")?;
        }
        write!(f, "{}", self.table)
    }
}

/// Metadata for a single column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnInfo {
    /// Column name.
    pub name: String,
    /// Column data type.
    pub data_type: DataType,
    /// Whether the column allows NULL values.
    pub nullable: bool,
}

impl From<ColumnInfo> for arrow::datatypes::Field {
    fn from(col: ColumnInfo) -> Self {
        arrow::datatypes::Field::new(col.name, col.data_type.into(), col.nullable)
    }
}

/// Representation of a scalar/literal value in query plans.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum ScalarValue {
    /// SQL NULL.
    Null,
    /// Boolean value.
    Boolean(bool),
    /// 32-bit signed integer.
    Int32(i32),
    /// 64-bit signed integer.
    Int64(i64),
    /// 32-bit floating point.
    Float32(f32),
    /// 64-bit floating point.
    Float64(f64),
    /// UTF-8 string.
    Utf8(String),
    /// Binary data.
    Binary(Vec<u8>),
    /// Decimal value with precision and scale.
    Decimal128 {
        /// The decimal value stored as i128.
        value: i128,
        /// Total number of digits.
        precision: u8,
        /// Number of digits after the decimal point.
        scale: i8,
    },
    /// Date (days since epoch).
    Date32(i32),
    /// Timestamp with unit and optional timezone.
    Timestamp {
        /// Timestamp value in the given time unit.
        value: i64,
        /// Time resolution.
        unit: TimeUnit,
        /// Optional timezone identifier.
        timezone: Option<String>,
    },
}

impl ScalarValue {
    /// Returns the `DataType` corresponding to this scalar value.
    pub fn data_type(&self) -> DataType {
        match self {
            ScalarValue::Null => DataType::Null,
            ScalarValue::Boolean(_) => DataType::Boolean,
            ScalarValue::Int32(_) => DataType::Int32,
            ScalarValue::Int64(_) => DataType::Int64,
            ScalarValue::Float32(_) => DataType::Float32,
            ScalarValue::Float64(_) => DataType::Float64,
            ScalarValue::Utf8(_) => DataType::Utf8,
            ScalarValue::Binary(_) => DataType::Binary,
            ScalarValue::Decimal128 {
                precision, scale, ..
            } => DataType::Decimal128 {
                precision: *precision,
                scale: *scale,
            },
            ScalarValue::Date32(_) => DataType::Date32,
            ScalarValue::Timestamp { unit, timezone, .. } => DataType::Timestamp {
                unit: *unit,
                timezone: timezone.clone(),
            },
        }
    }
}

impl fmt::Display for ScalarValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ScalarValue::Null => write!(f, "NULL"),
            ScalarValue::Boolean(v) => write!(f, "{v}"),
            ScalarValue::Int32(v) => write!(f, "{v}"),
            ScalarValue::Int64(v) => write!(f, "{v}"),
            ScalarValue::Float32(v) => write!(f, "{v}"),
            ScalarValue::Float64(v) => write!(f, "{v}"),
            ScalarValue::Utf8(v) => write!(f, "'{v}'"),
            ScalarValue::Binary(v) => write!(f, "<binary({} bytes)>", v.len()),
            ScalarValue::Decimal128 {
                value,
                precision,
                scale,
            } => write!(f, "{value} (DECIMAL({precision},{scale}))"),
            ScalarValue::Date32(v) => write!(f, "DATE({v})"),
            ScalarValue::Timestamp {
                value,
                unit,
                timezone,
            } => {
                write!(f, "TIMESTAMP({value}, {unit}")?;
                if let Some(tz) = timezone {
                    write!(f, ", {tz}")?;
                }
                write!(f, ")")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- TimeUnit tests --

    #[test]
    fn time_unit_to_arrow_roundtrip() {
        let units = [
            TimeUnit::Second,
            TimeUnit::Millisecond,
            TimeUnit::Microsecond,
            TimeUnit::Nanosecond,
        ];
        for unit in units {
            let arrow_unit: arrow::datatypes::TimeUnit = unit.into();
            let back: TimeUnit = arrow_unit.into();
            assert_eq!(unit, back);
        }
    }

    // -- DataType ↔ Arrow conversion tests --

    #[test]
    fn data_type_to_arrow_simple_types() {
        let cases: Vec<(DataType, arrow::datatypes::DataType)> = vec![
            (DataType::Null, arrow::datatypes::DataType::Null),
            (DataType::Boolean, arrow::datatypes::DataType::Boolean),
            (DataType::Int8, arrow::datatypes::DataType::Int8),
            (DataType::Int16, arrow::datatypes::DataType::Int16),
            (DataType::Int32, arrow::datatypes::DataType::Int32),
            (DataType::Int64, arrow::datatypes::DataType::Int64),
            (DataType::Float32, arrow::datatypes::DataType::Float32),
            (DataType::Float64, arrow::datatypes::DataType::Float64),
            (DataType::Utf8, arrow::datatypes::DataType::Utf8),
            (DataType::LargeUtf8, arrow::datatypes::DataType::LargeUtf8),
            (DataType::Binary, arrow::datatypes::DataType::Binary),
            (DataType::Date32, arrow::datatypes::DataType::Date32),
        ];
        for (sql_type, expected_arrow) in cases {
            let arrow_type: arrow::datatypes::DataType = sql_type.into();
            assert_eq!(arrow_type, expected_arrow);
        }
    }

    #[test]
    fn data_type_decimal_to_arrow() {
        let dt = DataType::Decimal128 {
            precision: 10,
            scale: 2,
        };
        let arrow_dt: arrow::datatypes::DataType = dt.into();
        assert_eq!(arrow_dt, arrow::datatypes::DataType::Decimal128(10, 2));
    }

    #[test]
    fn data_type_timestamp_to_arrow() {
        let dt = DataType::Timestamp {
            unit: TimeUnit::Microsecond,
            timezone: Some("UTC".to_string()),
        };
        let arrow_dt: arrow::datatypes::DataType = dt.into();
        assert_eq!(
            arrow_dt,
            arrow::datatypes::DataType::Timestamp(
                arrow::datatypes::TimeUnit::Microsecond,
                Some("UTC".into())
            )
        );
    }

    #[test]
    fn arrow_to_data_type_roundtrip() {
        let types = vec![
            DataType::Null,
            DataType::Boolean,
            DataType::Int32,
            DataType::Int64,
            DataType::Float64,
            DataType::Utf8,
            DataType::Binary,
            DataType::Date32,
            DataType::Decimal128 {
                precision: 18,
                scale: 4,
            },
            DataType::Timestamp {
                unit: TimeUnit::Nanosecond,
                timezone: None,
            },
        ];
        for dt in types {
            let arrow_dt: arrow::datatypes::DataType = dt.clone().into();
            let back = DataType::try_from(arrow_dt).unwrap();
            assert_eq!(dt, back);
        }
    }

    #[test]
    fn arrow_unsupported_type() {
        let arrow_dt = arrow::datatypes::DataType::Duration(arrow::datatypes::TimeUnit::Second);
        let result = DataType::try_from(arrow_dt);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("unsupported"));
    }

    // -- TableReference tests --

    #[test]
    fn table_reference_parse_one_part() {
        let tr = TableReference::parse("users").unwrap();
        assert_eq!(tr.catalog, None);
        assert_eq!(tr.schema, None);
        assert_eq!(tr.table, "users");
    }

    #[test]
    fn table_reference_parse_two_part() {
        let tr = TableReference::parse("public.users").unwrap();
        assert_eq!(tr.catalog, None);
        assert_eq!(tr.schema, Some("public".to_string()));
        assert_eq!(tr.table, "users");
    }

    #[test]
    fn table_reference_parse_three_part() {
        let tr = TableReference::parse("my_catalog.my_schema.my_table").unwrap();
        assert_eq!(tr.catalog, Some("my_catalog".to_string()));
        assert_eq!(tr.schema, Some("my_schema".to_string()));
        assert_eq!(tr.table, "my_table");
    }

    #[test]
    fn table_reference_parse_empty() {
        assert!(TableReference::parse("").is_err());
    }

    #[test]
    fn table_reference_parse_too_many_parts() {
        assert!(TableReference::parse("a.b.c.d").is_err());
    }

    #[test]
    fn table_reference_parse_empty_part() {
        assert!(TableReference::parse("a..b").is_err());
        assert!(TableReference::parse(".table").is_err());
        assert!(TableReference::parse("table.").is_err());
    }

    #[test]
    fn table_reference_display_one_part() {
        let tr = TableReference::table("users");
        assert_eq!(tr.to_string(), "users");
    }

    #[test]
    fn table_reference_display_three_part() {
        let tr = TableReference {
            catalog: Some("c".to_string()),
            schema: Some("s".to_string()),
            table: "t".to_string(),
        };
        assert_eq!(tr.to_string(), "c.s.t");
    }

    // -- ColumnInfo tests --

    #[test]
    fn column_info_to_arrow_field() {
        let col = ColumnInfo {
            name: "id".to_string(),
            data_type: DataType::Int32,
            nullable: false,
        };
        let field: arrow::datatypes::Field = col.into();
        assert_eq!(field.name(), "id");
        assert_eq!(field.data_type(), &arrow::datatypes::DataType::Int32);
        assert!(!field.is_nullable());
    }

    #[test]
    fn column_info_nullable() {
        let col = ColumnInfo {
            name: "email".to_string(),
            data_type: DataType::Utf8,
            nullable: true,
        };
        let field: arrow::datatypes::Field = col.into();
        assert!(field.is_nullable());
    }

    // -- ScalarValue tests --

    #[test]
    fn scalar_value_data_type_int64() {
        assert_eq!(ScalarValue::Int64(42).data_type(), DataType::Int64);
    }

    #[test]
    fn scalar_value_data_type_utf8() {
        assert_eq!(
            ScalarValue::Utf8("hello".to_string()).data_type(),
            DataType::Utf8
        );
    }

    #[test]
    fn scalar_value_data_type_null() {
        assert_eq!(ScalarValue::Null.data_type(), DataType::Null);
    }

    #[test]
    fn scalar_value_data_type_decimal() {
        let sv = ScalarValue::Decimal128 {
            value: 12345,
            precision: 10,
            scale: 2,
        };
        assert_eq!(
            sv.data_type(),
            DataType::Decimal128 {
                precision: 10,
                scale: 2
            }
        );
    }

    #[test]
    fn scalar_value_data_type_timestamp() {
        let sv = ScalarValue::Timestamp {
            value: 1000,
            unit: TimeUnit::Microsecond,
            timezone: Some("UTC".to_string()),
        };
        assert_eq!(
            sv.data_type(),
            DataType::Timestamp {
                unit: TimeUnit::Microsecond,
                timezone: Some("UTC".to_string())
            }
        );
    }

    #[test]
    fn scalar_value_display() {
        assert_eq!(ScalarValue::Null.to_string(), "NULL");
        assert_eq!(ScalarValue::Boolean(true).to_string(), "true");
        assert_eq!(ScalarValue::Int64(42).to_string(), "42");
        assert_eq!(ScalarValue::Utf8("hi".to_string()).to_string(), "'hi'");
    }
}
