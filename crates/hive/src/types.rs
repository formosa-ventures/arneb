//! Hive type string to Arrow DataType mapping.

use arrow::datatypes::{DataType as ArrowDataType, TimeUnit};

use arneb_common::error::ConnectorError;

/// Convert a Hive type string to an Arrow DataType.
///
/// Supports common Hive primitive types. Returns an error for
/// unsupported complex types (MAP, ARRAY, STRUCT).
pub fn hive_type_to_arrow(hive_type: &str) -> Result<ArrowDataType, ConnectorError> {
    let normalized = hive_type.trim().to_uppercase();

    // Handle parameterized types first
    if normalized.starts_with("DECIMAL") || normalized.starts_with("NUMERIC") {
        return parse_decimal(&normalized);
    }
    if normalized.starts_with("VARCHAR") || normalized.starts_with("CHAR") {
        return Ok(ArrowDataType::Utf8);
    }

    match normalized.as_str() {
        // Integer types
        "TINYINT" => Ok(ArrowDataType::Int8),
        "SMALLINT" | "SHORT" => Ok(ArrowDataType::Int16),
        "INT" | "INTEGER" => Ok(ArrowDataType::Int32),
        "BIGINT" | "LONG" => Ok(ArrowDataType::Int64),

        // Floating point types
        "FLOAT" => Ok(ArrowDataType::Float32),
        "DOUBLE" | "DOUBLE PRECISION" => Ok(ArrowDataType::Float64),

        // Boolean
        "BOOLEAN" => Ok(ArrowDataType::Boolean),

        // String types
        "STRING" => Ok(ArrowDataType::Utf8),

        // Binary
        "BINARY" => Ok(ArrowDataType::Binary),

        // Date/time types
        "DATE" => Ok(ArrowDataType::Date32),
        "TIMESTAMP" => Ok(ArrowDataType::Timestamp(TimeUnit::Microsecond, None)),

        // Unsupported complex types
        t if t.starts_with("MAP") => Err(ConnectorError::UnsupportedOperation(format!(
            "unsupported Hive type: {hive_type} (complex types not yet supported)"
        ))),
        t if t.starts_with("ARRAY") => Err(ConnectorError::UnsupportedOperation(format!(
            "unsupported Hive type: {hive_type} (complex types not yet supported)"
        ))),
        t if t.starts_with("STRUCT") => Err(ConnectorError::UnsupportedOperation(format!(
            "unsupported Hive type: {hive_type} (complex types not yet supported)"
        ))),
        t if t.starts_with("UNIONTYPE") => Err(ConnectorError::UnsupportedOperation(format!(
            "unsupported Hive type: {hive_type} (complex types not yet supported)"
        ))),

        _ => Err(ConnectorError::UnsupportedOperation(format!(
            "unknown Hive type: {hive_type}"
        ))),
    }
}

fn parse_decimal(normalized: &str) -> Result<ArrowDataType, ConnectorError> {
    // DECIMAL or DECIMAL(p) or DECIMAL(p,s)
    if normalized == "DECIMAL" || normalized == "NUMERIC" {
        return Ok(ArrowDataType::Decimal128(38, 18)); // Hive default
    }

    let inner = normalized
        .trim_start_matches("DECIMAL")
        .trim_start_matches("NUMERIC")
        .trim_start_matches('(')
        .trim_end_matches(')');

    let parts: Vec<&str> = inner.split(',').map(|s| s.trim()).collect();
    match parts.len() {
        1 => {
            let precision: u8 = parts[0].parse().map_err(|_| {
                ConnectorError::UnsupportedOperation(format!(
                    "invalid DECIMAL precision: {normalized}"
                ))
            })?;
            Ok(ArrowDataType::Decimal128(precision, 0))
        }
        2 => {
            let precision: u8 = parts[0].parse().map_err(|_| {
                ConnectorError::UnsupportedOperation(format!(
                    "invalid DECIMAL precision: {normalized}"
                ))
            })?;
            let scale: i8 = parts[1].parse().map_err(|_| {
                ConnectorError::UnsupportedOperation(format!("invalid DECIMAL scale: {normalized}"))
            })?;
            Ok(ArrowDataType::Decimal128(precision, scale))
        }
        _ => Err(ConnectorError::UnsupportedOperation(format!(
            "invalid DECIMAL format: {normalized}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_integer_types() {
        assert_eq!(hive_type_to_arrow("tinyint").unwrap(), ArrowDataType::Int8);
        assert_eq!(
            hive_type_to_arrow("smallint").unwrap(),
            ArrowDataType::Int16
        );
        assert_eq!(hive_type_to_arrow("int").unwrap(), ArrowDataType::Int32);
        assert_eq!(hive_type_to_arrow("integer").unwrap(), ArrowDataType::Int32);
        assert_eq!(hive_type_to_arrow("bigint").unwrap(), ArrowDataType::Int64);
    }

    #[test]
    fn test_float_types() {
        assert_eq!(hive_type_to_arrow("float").unwrap(), ArrowDataType::Float32);
        assert_eq!(
            hive_type_to_arrow("double").unwrap(),
            ArrowDataType::Float64
        );
    }

    #[test]
    fn test_boolean() {
        assert_eq!(
            hive_type_to_arrow("boolean").unwrap(),
            ArrowDataType::Boolean
        );
    }

    #[test]
    fn test_string_types() {
        assert_eq!(hive_type_to_arrow("string").unwrap(), ArrowDataType::Utf8);
        assert_eq!(
            hive_type_to_arrow("varchar(255)").unwrap(),
            ArrowDataType::Utf8
        );
        assert_eq!(hive_type_to_arrow("char(10)").unwrap(), ArrowDataType::Utf8);
    }

    #[test]
    fn test_date_time_types() {
        assert_eq!(hive_type_to_arrow("date").unwrap(), ArrowDataType::Date32);
        assert_eq!(
            hive_type_to_arrow("timestamp").unwrap(),
            ArrowDataType::Timestamp(TimeUnit::Microsecond, None)
        );
    }

    #[test]
    fn test_decimal() {
        assert_eq!(
            hive_type_to_arrow("decimal").unwrap(),
            ArrowDataType::Decimal128(38, 18)
        );
        assert_eq!(
            hive_type_to_arrow("decimal(10,2)").unwrap(),
            ArrowDataType::Decimal128(10, 2)
        );
        assert_eq!(
            hive_type_to_arrow("DECIMAL(18)").unwrap(),
            ArrowDataType::Decimal128(18, 0)
        );
    }

    #[test]
    fn test_binary() {
        assert_eq!(hive_type_to_arrow("binary").unwrap(), ArrowDataType::Binary);
    }

    #[test]
    fn test_unsupported_complex_types() {
        assert!(hive_type_to_arrow("map<string,int>").is_err());
        assert!(hive_type_to_arrow("array<string>").is_err());
        assert!(hive_type_to_arrow("struct<name:string,age:int>").is_err());
    }

    #[test]
    fn test_unknown_type() {
        let err = hive_type_to_arrow("foobar").unwrap_err();
        assert!(err.to_string().contains("unknown Hive type"));
    }

    #[test]
    fn test_case_insensitive() {
        assert_eq!(hive_type_to_arrow("INT").unwrap(), ArrowDataType::Int32);
        assert_eq!(hive_type_to_arrow("Int").unwrap(), ArrowDataType::Int32);
        assert_eq!(hive_type_to_arrow("STRING").unwrap(), ArrowDataType::Utf8);
    }
}
