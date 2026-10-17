//! pgwire `statement_timeout` (admission-control D7): `SET` / `SHOW` /
//! `RESET statement_timeout`, stored per connection next to `search_path`.
//!
//! The value uses PostgreSQL's syntax (a bare integer is milliseconds;
//! `'5s'`, `'2min'`; `0` is off). A session that never sets it, or resets
//! it, uses the server default (`[admission] statement_timeout`).

use std::time::Duration;

use arneb_common::duration::{format_pg_time_limit, parse_time_limit};

use crate::metadata::MetadataResult;
use crate::session::keyword;

/// A connection's `statement_timeout`. `explicit: None` means "not set in
/// this session": the server default applies.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct SessionTimeout {
    pub(crate) explicit: Option<Option<Duration>>,
}

impl SessionTimeout {
    /// The timeout in effect, given the server default.
    pub(crate) fn effective(&self, server_default: Option<Duration>) -> Option<Duration> {
        self.explicit.unwrap_or(server_default)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Command {
    Set(Option<Duration>),
    Reset,
    Show,
}

/// Parse a `statement_timeout` command. `None`: not one. `Some(Err)`: an
/// invalid value (SQLSTATE `22023`).
pub(crate) fn parse(sql: &str) -> Option<Result<Command, String>> {
    let sql = sql.trim().trim_end_matches(';').trim_end();
    if let Some(rest) = keyword(sql, "show") {
        return keyword(rest, "statement_timeout")
            .filter(|r| r.trim().is_empty())
            .map(|_| Ok(Command::Show));
    }
    if let Some(rest) = keyword(sql, "reset") {
        return keyword(rest, "statement_timeout")
            .filter(|r| r.trim().is_empty())
            .map(|_| Ok(Command::Reset));
    }
    let rest = keyword(sql, "set")?;
    let rest = keyword(rest, "session")
        .or_else(|| keyword(rest, "local"))
        .unwrap_or(rest);
    let rest = keyword(rest, "statement_timeout")?.trim_start();
    let value = rest
        .strip_prefix('=')
        .or_else(|| keyword(rest, "to"))?
        .trim();
    if value.eq_ignore_ascii_case("default") {
        return Some(Ok(Command::Reset));
    }
    Some(
        parse_time_limit(value)
            .map(Command::Set)
            .map_err(|e| format!("invalid value for parameter \"statement_timeout\": {e}")),
    )
}

/// Whether `sql` is `RESET ALL` (which also resets `statement_timeout`).
pub(crate) fn is_reset_all(sql: &str) -> bool {
    let sql = sql.trim().trim_end_matches(';').trim_end();
    keyword(sql, "reset")
        .and_then(|r| keyword(r, "all"))
        .is_some_and(|r| r.trim().is_empty())
}

/// `SHOW statement_timeout` result, in PostgreSQL's display form.
pub(crate) fn show_result(effective: Option<Duration>) -> MetadataResult {
    crate::session::show_result("statement_timeout", &format_pg_time_limit(effective))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ms(n: u64) -> Option<Duration> {
        Some(Duration::from_millis(n))
    }

    #[test]
    fn parses_set_show_reset() {
        assert_eq!(
            parse("SET statement_timeout = 5000"),
            Some(Ok(Command::Set(ms(5000))))
        );
        assert_eq!(
            parse("set statement_timeout to '5s';"),
            Some(Ok(Command::Set(ms(5000))))
        );
        assert_eq!(
            parse("SET SESSION statement_timeout = '2min'"),
            Some(Ok(Command::Set(ms(120_000))))
        );
        assert_eq!(
            parse("SET statement_timeout = 0"),
            Some(Ok(Command::Set(None)))
        );
        assert_eq!(
            parse("SET statement_timeout TO DEFAULT"),
            Some(Ok(Command::Reset))
        );
        assert_eq!(parse("RESET statement_timeout"), Some(Ok(Command::Reset)));
        assert_eq!(parse("SHOW statement_timeout"), Some(Ok(Command::Show)));
        assert!(matches!(
            parse("SET statement_timeout = 'soon'"),
            Some(Err(_))
        ));
        assert_eq!(parse("SET search_path = x"), None);
        assert_eq!(parse("SHOW statement_timeouts"), None);
        assert!(is_reset_all("RESET ALL;"));
        assert!(!is_reset_all("RESET statement_timeout"));
    }

    #[test]
    fn session_value_overrides_the_default() {
        let unset = SessionTimeout::default();
        assert_eq!(unset.effective(ms(30_000)), ms(30_000));
        let off = SessionTimeout {
            explicit: Some(None),
        };
        assert_eq!(off.effective(ms(30_000)), None);
        let set = SessionTimeout {
            explicit: Some(ms(500)),
        };
        assert_eq!(set.effective(None), ms(500));
    }
}
