//! Parsing the time limits used by admission control (`statement_timeout`,
//! `query_max_execution_time`, `query_max_run_time`).
//!
//! Both spellings are accepted:
//!
//! - PostgreSQL `statement_timeout`: a bare integer is milliseconds; units
//!   `us`, `ms`, `s`, `min`, `h`, `d`.
//! - Trino durations: a decimal number with a unit, e.g. `30s`, `5m`,
//!   `1.5h`, `500ms`, `1d`.
//!
//! `0` (any unit) means no limit.

use std::time::Duration;

/// Parse a time limit; `Ok(None)` means `0` (no limit).
pub fn parse_time_limit(input: &str) -> Result<Option<Duration>, String> {
    let s = input.trim().trim_matches('\'').trim();
    if s.is_empty() {
        return Err("empty duration".to_owned());
    }
    let split = s
        .find(|c: char| !(c.is_ascii_digit() || c == '.'))
        .unwrap_or(s.len());
    let (number, unit) = s.split_at(split);
    let value: f64 = number
        .parse()
        .map_err(|_| format!("invalid duration '{input}'"))?;
    if !value.is_finite() || value < 0.0 {
        return Err(format!("invalid duration '{input}'"));
    }
    let millis_per_unit = match unit.trim().to_ascii_lowercase().as_str() {
        "" | "ms" => 1.0,
        "us" => 0.001,
        "s" => 1_000.0,
        "m" | "min" => 60_000.0,
        "h" => 3_600_000.0,
        "d" => 86_400_000.0,
        other => {
            return Err(format!(
                "invalid duration unit '{other}' in '{input}' (expected us, ms, s, m/min, h or d)"
            ))
        }
    };
    let millis = value * millis_per_unit;
    if millis == 0.0 {
        return Ok(None);
    }
    Duration::try_from_secs_f64(millis / 1_000.0)
        .map(Some)
        .map_err(|_| format!("invalid duration '{input}' (out of range)"))
}

/// PostgreSQL's display form for `SHOW statement_timeout`: `0`, or the
/// largest unit that divides the value exactly (`500ms`, `5s`, `2min`,
/// `1h`, `1d`).
pub fn format_pg_time_limit(limit: Option<Duration>) -> String {
    let Some(d) = limit else {
        return "0".to_owned();
    };
    let ms = d.as_millis();
    for (unit, size) in [
        ("d", 86_400_000u128),
        ("h", 3_600_000),
        ("min", 60_000),
        ("s", 1_000),
    ] {
        if ms % size == 0 {
            return format!("{}{unit}", ms / size);
        }
    }
    format!("{ms}ms")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ms(n: u64) -> Option<Duration> {
        Some(Duration::from_millis(n))
    }

    #[test]
    fn postgres_forms() {
        assert_eq!(parse_time_limit("5000").unwrap(), ms(5000));
        assert_eq!(parse_time_limit("'5s'").unwrap(), ms(5000));
        assert_eq!(parse_time_limit("2min").unwrap(), ms(120_000));
        assert_eq!(parse_time_limit("500 ms").unwrap(), ms(500));
        assert_eq!(parse_time_limit("0").unwrap(), None);
    }

    #[test]
    fn trino_forms() {
        assert_eq!(parse_time_limit("30s").unwrap(), ms(30_000));
        assert_eq!(parse_time_limit("5m").unwrap(), ms(300_000));
        assert_eq!(parse_time_limit("1.5h").unwrap(), ms(5_400_000));
        assert_eq!(parse_time_limit("1d").unwrap(), ms(86_400_000));
        assert_eq!(parse_time_limit("0s").unwrap(), None);
    }

    #[test]
    fn invalid_values() {
        for bad in [
            "",
            "abc",
            "-5s",
            "5 weeks",
            "1.2.3s",
            "99999999999999999999999d",
            "'99999999999999999999999d'",
        ] {
            assert!(parse_time_limit(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn display() {
        assert_eq!(format_pg_time_limit(None), "0");
        assert_eq!(format_pg_time_limit(ms(500)), "500ms");
        assert_eq!(format_pg_time_limit(ms(5000)), "5s");
        assert_eq!(format_pg_time_limit(ms(120_000)), "2min");
        assert_eq!(format_pg_time_limit(ms(3_600_000)), "1h");
    }
}
