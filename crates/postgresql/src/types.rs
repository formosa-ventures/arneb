//! PostgreSQL type → Arneb type mapping.
//!
//! The mapped [`RemoteColumn::remote_type`](arneb_rdbms::RemoteColumn) is
//! the PostgreSQL type name the COPY decoder dispatches on. A type that
//! cannot be read is recorded as `!<description>`: the column still
//! appears in the table schema (as VARCHAR) so other columns stay
//! queryable, and reading it fails with an error naming the type.

use arneb_common::types::{DataType, TimeUnit};

/// Marker prefix for an unsupported remote type.
pub(crate) const UNSUPPORTED_PREFIX: char = '!';

/// The largest decimal precision Arneb carries (Decimal128).
const MAX_PRECISION: i32 = 38;

/// Map a column's `pg_type.typname` and `pg_attribute.atttypmod` to
/// (Arneb type, remote type tag).
pub(crate) fn map_type(
    typname: &str,
    typmod: i32,
    unconstrained_numeric_scale: i8,
) -> (DataType, String) {
    let supported = |t: DataType| (t, typname.to_string());
    match typname {
        "bool" => supported(DataType::Boolean),
        "int2" => supported(DataType::Int16),
        "int4" => supported(DataType::Int32),
        "int8" => supported(DataType::Int64),
        "float4" => supported(DataType::Float32),
        "float8" => supported(DataType::Float64),
        "text" | "varchar" | "bpchar" | "name" => supported(DataType::Utf8),
        "date" => supported(DataType::Date32),
        "timestamp" => supported(DataType::Timestamp {
            unit: TimeUnit::Microsecond,
            timezone: None,
        }),
        "timestamptz" => supported(DataType::Timestamp {
            unit: TimeUnit::Microsecond,
            timezone: Some("UTC".to_string()),
        }),
        "uuid" => supported(DataType::Utf8),
        "bytea" => supported(DataType::Binary),
        "numeric" => {
            if typmod < 0 {
                return supported(DataType::Decimal128 {
                    precision: MAX_PRECISION as u8,
                    scale: unconstrained_numeric_scale,
                });
            }
            let precision = ((typmod - 4) >> 16) & 0xffff;
            // PostgreSQL 15+ allows a negative scale, stored as an 11-bit
            // two's-complement field.
            let raw_scale = (typmod - 4) & 0x7ff;
            let scale = if raw_scale >= 1024 {
                raw_scale - 2048
            } else {
                raw_scale
            };
            if precision > MAX_PRECISION || !(0..=precision).contains(&scale) {
                return unsupported(&format!("numeric({precision},{scale})"));
            }
            supported(DataType::Decimal128 {
                precision: precision as u8,
                scale: scale as i8,
            })
        }
        other => unsupported(other),
    }
}

fn unsupported(description: &str) -> (DataType, String) {
    (DataType::Utf8, format!("{UNSUPPORTED_PREFIX}{description}"))
}

/// The unsupported type's description, if `remote_type` is one.
pub(crate) fn unsupported_type(remote_type: &str) -> Option<&str> {
    remote_type.strip_prefix(UNSUPPORTED_PREFIX)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `atttypmod` for `numeric(p, s)`.
    fn typmod(p: i32, s: i32) -> i32 {
        ((p << 16) | (s & 0x7ff)) + 4
    }

    #[test]
    fn scalar_types() {
        assert_eq!(map_type("int4", -1, 10).0, DataType::Int32);
        assert_eq!(map_type("bpchar", 5, 10), (DataType::Utf8, "bpchar".into()));
        assert_eq!(map_type("uuid", -1, 10), (DataType::Utf8, "uuid".into()));
        assert_eq!(
            map_type("timestamptz", -1, 10).0,
            DataType::Timestamp {
                unit: TimeUnit::Microsecond,
                timezone: Some("UTC".into())
            }
        );
    }

    #[test]
    fn numeric_precision_and_scale() {
        assert_eq!(
            map_type("numeric", typmod(15, 2), 10).0,
            DataType::Decimal128 {
                precision: 15,
                scale: 2
            }
        );
        assert_eq!(
            map_type("numeric", -1, 10).0,
            DataType::Decimal128 {
                precision: 38,
                scale: 10
            }
        );
        assert_eq!(map_type("numeric", typmod(50, 2), 10).1, "!numeric(50,2)");
        assert_eq!(map_type("numeric", typmod(10, -2), 10).1, "!numeric(10,-2)");
    }

    #[test]
    fn unknown_types_are_marked() {
        let (t, tag) = map_type("jsonb", -1, 10);
        assert_eq!(t, DataType::Utf8);
        assert_eq!(unsupported_type(&tag), Some("jsonb"));
        assert_eq!(unsupported_type("int4"), None);
    }
}
