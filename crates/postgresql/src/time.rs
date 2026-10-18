//! Calendar arithmetic shared by literal rendering and the COPY decoder.

/// Days between 1970-01-01 (Arrow's epoch) and 2000-01-01 (PostgreSQL's).
pub(crate) const PG_EPOCH_DAYS: i32 = 10_957;
/// Microseconds between the two epochs.
pub(crate) const PG_EPOCH_MICROS: i64 = PG_EPOCH_DAYS as i64 * 86_400_000_000;

/// (year, month, day) of a day count since 1970-01-01 (proleptic
/// Gregorian; Howard Hinnant's `civil_from_days`).
pub(crate) fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let y = yoe + era * 400 + i64::from(m <= 2);
    (y, m, d)
}

/// `YYYY-MM-DD HH:MM:SS.ffffff` for microseconds since 1970-01-01, or
/// `None` outside years 1–9999.
pub(crate) fn format_timestamp_micros(micros: i64) -> Option<String> {
    let days = micros.div_euclid(86_400_000_000);
    let rem = micros.rem_euclid(86_400_000_000);
    let (y, m, d) = civil_from_days(days);
    if !(1..=9999).contains(&y) {
        return None;
    }
    let secs = rem / 1_000_000;
    let frac = rem % 1_000_000;
    Some(format!(
        "{y:04}-{m:02}-{d:02} {:02}:{:02}:{:02}.{frac:06}",
        secs / 3600,
        secs / 60 % 60,
        secs % 60
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_dates() {
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(PG_EPOCH_DAYS as i64), (2000, 1, 1));
        assert_eq!(civil_from_days(-719_162), (1, 1, 1));
        assert_eq!(civil_from_days(11_016), (2000, 2, 29));
    }

    #[test]
    fn timestamps_format() {
        assert_eq!(
            format_timestamp_micros(PG_EPOCH_MICROS + 3_723_000_042).unwrap(),
            "2000-01-01 01:02:03.000042"
        );
        assert!(format_timestamp_micros(i64::MIN).is_none());
    }
}
