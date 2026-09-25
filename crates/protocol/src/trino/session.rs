//! Per-request client session carried in `X-Trino-*` (or legacy
//! `X-Presto-*`) HTTP headers.
//!
//! The Trino protocol is stateless on the server: every request carries the
//! full session (user, catalog, schema, session properties, prepared
//! statements), and the server asks the client to change it through
//! `X-Trino-Set-*` / `X-Trino-Clear-*` response headers.

use axum::http::HeaderMap;
use percent_encoding::{percent_decode_str, utf8_percent_encode, AsciiSet, CONTROLS};

/// Characters escaped when writing a header value that Trino clients
/// URL-decode (session property values, prepared statement text).
const HEADER_VALUE: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'%')
    .add(b',')
    .add(b'=')
    .add(b';')
    .add(b'+')
    .add(b'&')
    .add(b'#')
    .add(b'<')
    .add(b'>')
    .add(b'\'')
    .add(b'\\');

pub(crate) fn url_encode(s: &str) -> String {
    utf8_percent_encode(s, HEADER_VALUE).to_string()
}

pub(crate) fn url_decode(s: &str) -> String {
    // Clients encode with Java's URLEncoder, which writes spaces as '+'.
    percent_decode_str(&s.replace('+', " "))
        .decode_utf8_lossy()
        .into_owned()
}

/// Header prefix the client used (`X-Trino-` or legacy `X-Presto-`).
/// Responses echo the same family so older Presto clients keep working.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HeaderFamily {
    Trino,
    Presto,
}

impl HeaderFamily {
    pub(crate) fn name(self, suffix: &str) -> String {
        match self {
            Self::Trino => format!("X-Trino-{suffix}"),
            Self::Presto => format!("X-Presto-{suffix}"),
        }
    }
}

/// Client session parsed from request headers.
#[derive(Debug, Clone)]
pub(crate) struct ClientSession {
    pub(crate) family: HeaderFamily,
    pub(crate) user: String,
    pub(crate) source: Option<String>,
    pub(crate) catalog: Option<String>,
    pub(crate) schema: Option<String>,
    pub(crate) client_info: Option<String>,
    /// `X-Trino-Session` key/value pairs (accepted and echoed by
    /// `SHOW SESSION`, not otherwise interpreted).
    pub(crate) properties: Vec<(String, String)>,
    /// `X-Trino-Prepared-Statement` name → SQL text.
    pub(crate) prepared: Vec<(String, String)>,
}

fn header<'a>(headers: &'a HeaderMap, family: HeaderFamily, suffix: &str) -> Option<&'a str> {
    headers
        .get(family.name(suffix))
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|v| !v.is_empty())
}

/// Parses a comma-separated `name=value` list (values URL-encoded). A header
/// may appear multiple times; all occurrences are merged.
fn key_values(headers: &HeaderMap, family: HeaderFamily, suffix: &str) -> Vec<(String, String)> {
    headers
        .get_all(family.name(suffix))
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .filter_map(|pair| {
            let (k, v) = pair.split_once('=')?;
            let k = k.trim();
            if k.is_empty() {
                return None;
            }
            Some((url_decode(k), url_decode(v.trim())))
        })
        .collect()
}

impl ClientSession {
    /// Reads the session from request headers. `X-Trino-*` wins when both
    /// families are present.
    pub(crate) fn from_headers(headers: &HeaderMap) -> Self {
        let family =
            if headers.get("X-Trino-User").is_none() && headers.get("X-Presto-User").is_some() {
                HeaderFamily::Presto
            } else {
                HeaderFamily::Trino
            };
        Self {
            family,
            user: header(headers, family, "User")
                .unwrap_or("anonymous")
                .to_string(),
            source: header(headers, family, "Source").map(str::to_string),
            catalog: header(headers, family, "Catalog").map(str::to_string),
            schema: header(headers, family, "Schema").map(str::to_string),
            client_info: header(headers, family, "Client-Info").map(str::to_string),
            properties: key_values(headers, family, "Session"),
            prepared: key_values(headers, family, "Prepared-Statement"),
        }
    }

    pub(crate) fn prepared_statement(&self, name: &str) -> Option<&str> {
        self.prepared
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, sql)| sql.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    #[test]
    fn parses_trino_headers() {
        let mut h = HeaderMap::new();
        h.insert("X-Trino-User", HeaderValue::from_static("alice"));
        h.insert("X-Trino-Catalog", HeaderValue::from_static("hive"));
        h.insert("X-Trino-Schema", HeaderValue::from_static("tpch"));
        h.insert(
            "X-Trino-Session",
            HeaderValue::from_static("query_max_run_time=1h,join_distribution_type=BROADCAST"),
        );
        h.insert(
            "X-Trino-Prepared-Statement",
            HeaderValue::from_static("q1=SELECT+%3F+%2B+1"),
        );
        let s = ClientSession::from_headers(&h);
        assert_eq!(s.family, HeaderFamily::Trino);
        assert_eq!(s.user, "alice");
        assert_eq!(s.catalog.as_deref(), Some("hive"));
        assert_eq!(s.schema.as_deref(), Some("tpch"));
        assert_eq!(s.properties.len(), 2);
        assert_eq!(s.prepared_statement("Q1"), Some("SELECT ? + 1"));
    }

    #[test]
    fn falls_back_to_presto_headers() {
        let mut h = HeaderMap::new();
        h.insert("X-Presto-User", HeaderValue::from_static("bob"));
        h.insert("X-Presto-Catalog", HeaderValue::from_static("memory"));
        let s = ClientSession::from_headers(&h);
        assert_eq!(s.family, HeaderFamily::Presto);
        assert_eq!(s.user, "bob");
        assert_eq!(s.catalog.as_deref(), Some("memory"));
        assert_eq!(s.family.name("Set-Catalog"), "X-Presto-Set-Catalog");
    }

    #[test]
    fn url_round_trip() {
        let sql = "SELECT 'a, b' = x";
        assert_eq!(url_decode(&url_encode(sql)), sql);
    }
}
