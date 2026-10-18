use arneb_common::types::{ScalarValue, TimeUnit};
use arneb_rdbms::{Dialect, RemoteColumn};

use crate::time::{civil_from_days, format_timestamp_micros};

/// PostgreSQL SQL rendering.
#[derive(Debug, Default, Clone, Copy)]
pub struct PostgresDialect;

impl Dialect for PostgresDialect {
    fn quote_identifier(&self, ident: &str) -> String {
        format!("\"{}\"", ident.replace('"', "\"\""))
    }

    fn render_literal(&self, value: &ScalarValue) -> Option<String> {
        Some(match value {
            ScalarValue::Boolean(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
            ScalarValue::Int32(v) => v.to_string(),
            ScalarValue::Int64(v) => v.to_string(),
            ScalarValue::Decimal128 { value, scale, .. } => render_decimal(*value, *scale)?,
            // Backslashes are refused rather than escaped: their meaning
            // depends on the server's `standard_conforming_strings`, and in
            // LIKE patterns on the escape character. NUL cannot be stored.
            ScalarValue::Utf8(s) if !s.contains(['\\', '\0']) => {
                format!("'{}'", s.replace('\'', "''"))
            }
            ScalarValue::Date32(days) => {
                let (y, m, d) = civil_from_days(*days as i64);
                if !(1..=9999).contains(&y) {
                    return None;
                }
                format!("DATE '{y:04}-{m:02}-{d:02}'")
            }
            ScalarValue::Timestamp {
                value,
                unit,
                timezone,
            } => {
                let micros = to_micros(*value, *unit)?;
                let text = format_timestamp_micros(micros)?;
                match timezone {
                    None => format!("TIMESTAMP '{text}'"),
                    Some(_) => format!("TIMESTAMPTZ '{text}+00'"),
                }
            }
            _ => return None,
        })
    }

    fn pushable_column(&self, column: &RemoteColumn) -> bool {
        // `char(n)` compares ignoring trailing spaces in PostgreSQL but not
        // in Arneb (which sees the padded value), so `<>` would drop rows.
        !matches!(column.remote_type.as_str(), "bpchar") && !column.remote_type.starts_with('!')
    }

    fn empty_select_list(&self) -> &'static str {
        // PostgreSQL accepts a zero-column SELECT list.
        ""
    }
}

fn to_micros(value: i64, unit: TimeUnit) -> Option<i64> {
    match unit {
        TimeUnit::Second => value.checked_mul(1_000_000),
        TimeUnit::Millisecond => value.checked_mul(1_000),
        TimeUnit::Microsecond => Some(value),
        TimeUnit::Nanosecond => (value % 1_000 == 0).then_some(value / 1_000),
    }
}

/// Exact decimal text for `value × 10^-scale`.
fn render_decimal(value: i128, scale: i8) -> Option<String> {
    if scale < 0 {
        return None;
    }
    let digits = value.unsigned_abs().to_string();
    let scale = scale as usize;
    let sign = if value < 0 { "-" } else { "" };
    if scale == 0 {
        return Some(format!("{sign}{digits}"));
    }
    let padded = format!("{digits:0>width$}", width = scale + 1);
    let (int, frac) = padded.split_at(padded.len() - scale);
    Some(format!("{sign}{int}.{frac}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn render(v: ScalarValue) -> Option<String> {
        PostgresDialect.render_literal(&v)
    }

    #[test]
    fn identifiers_are_quoted_and_escaped() {
        assert_eq!(PostgresDialect.quote_identifier("select"), "\"select\"");
        assert_eq!(
            PostgresDialect.quote_identifier("Order \"Items\""),
            "\"Order \"\"Items\"\"\""
        );
    }

    #[test]
    fn strings_are_escaped_or_refused() {
        assert_eq!(
            render(ScalarValue::Utf8("O'Brien".into())).unwrap(),
            "'O''Brien'"
        );
        assert!(render(ScalarValue::Utf8("a\\b".into())).is_none());
        assert!(render(ScalarValue::Utf8("a\0b".into())).is_none());
    }

    #[test]
    fn decimals_render_exactly() {
        let d = |value, scale| ScalarValue::Decimal128 {
            value,
            precision: 38,
            scale,
        };
        assert_eq!(render(d(12345, 2)).unwrap(), "123.45");
        assert_eq!(render(d(-5, 2)).unwrap(), "-0.05");
        assert_eq!(render(d(-50, 0)).unwrap(), "-50");
        assert_eq!(render(d(7, 3)).unwrap(), "0.007");
        assert!(render(d(7, -1)).is_none());
    }

    #[test]
    fn dates_and_timestamps() {
        // 1998-09-02 = day 10471.
        assert_eq!(
            render(ScalarValue::Date32(10471)).unwrap(),
            "DATE '1998-09-02'"
        );
        assert_eq!(
            render(ScalarValue::Date32(-1)).unwrap(),
            "DATE '1969-12-31'"
        );
        let ts = ScalarValue::Timestamp {
            value: 1_500_000,
            unit: TimeUnit::Microsecond,
            timezone: None,
        };
        assert_eq!(
            render(ts).unwrap(),
            "TIMESTAMP '1970-01-01 00:00:01.500000'"
        );
        let tz = ScalarValue::Timestamp {
            value: -1,
            unit: TimeUnit::Second,
            timezone: Some("UTC".into()),
        };
        assert_eq!(
            render(tz).unwrap(),
            "TIMESTAMPTZ '1969-12-31 23:59:59.000000+00'"
        );
        let nanos = ScalarValue::Timestamp {
            value: 1,
            unit: TimeUnit::Nanosecond,
            timezone: None,
        };
        assert!(render(nanos).is_none());
    }

    #[test]
    fn other_literals() {
        assert_eq!(render(ScalarValue::Boolean(true)).unwrap(), "TRUE");
        assert_eq!(render(ScalarValue::Int64(-7)).unwrap(), "-7");
        assert!(render(ScalarValue::Float64(1.5)).is_none());
        assert!(render(ScalarValue::Binary(vec![1])).is_none());
    }

    #[test]
    fn bpchar_and_unsupported_columns_are_not_pushable() {
        let c = |t: &str| RemoteColumn {
            name: "c".into(),
            remote_type: t.into(),
        };
        assert!(PostgresDialect.pushable_column(&c("varchar")));
        assert!(!PostgresDialect.pushable_column(&c("bpchar")));
        assert!(!PostgresDialect.pushable_column(&c("!jsonb")));
    }
}
