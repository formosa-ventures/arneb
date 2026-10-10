//! Per-connection pgwire session state: `SET` / `SHOW` / `RESET search_path`.
//!
//! The state lives in pgwire's per-connection `SessionExtensions`, so it is
//! shared by the Simple and Extended Query handlers of one connection and
//! never visible to another connection.
//!
//! Arneb (like Trino) has a single current schema, so only one entry of the
//! search path is used to resolve unqualified names: the first entry naming
//! an existing schema. When none does (e.g. PostgreSQL's `"$user", public`,
//! which clients and `pg_dump` output send), the server default schema is
//! kept. `SET` still accepts nonexistent schemas, as PostgreSQL does, and
//! `SHOW` reports the value as set. An entry may be `schema` (in the server's
//! default catalog) or `catalog.schema`.

use std::sync::Arc;

use arneb_catalog::CatalogManager;
use arrow::array::{RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};

use crate::metadata::{MetadataResponse, MetadataResult};

/// A connection's effective `search_path`.
pub(crate) struct SearchPath {
    /// Value reported by `SHOW search_path`.
    display: String,
    /// Catalog manager that resolves unqualified names for this connection.
    pub(crate) catalog_manager: Arc<CatalogManager>,
}

enum Command {
    Set(Vec<String>),
    Reset,
    Show,
}

/// Handle `SET` / `SHOW` / `RESET search_path`. Returns `None` for any other
/// statement. `current` is the connection's path, `None` meaning the server
/// default. On `Some((result, Some(path)))` the caller stores `path` as the
/// connection's new search path.
pub(crate) async fn handle_search_path(
    sql: &str,
    root: &Arc<CatalogManager>,
    current: Option<&SearchPath>,
) -> Option<(MetadataResult, Option<SearchPath>)> {
    let set_ok = || Ok(MetadataResponse::Command("SET".to_string()));
    match parse(sql)? {
        Command::Show => {
            let value = current.map_or(root.default_schema(), |p| p.display.as_str());
            Some((show_result(value), None))
        }
        Command::Reset => Some((set_ok(), Some(default_path(root)))),
        Command::Set(entries) => {
            let path = resolve(root, entries).await;
            Some((set_ok(), Some(path)))
        }
    }
}

fn default_path(root: &Arc<CatalogManager>) -> SearchPath {
    SearchPath {
        display: root.default_schema().to_string(),
        catalog_manager: Arc::clone(root),
    }
}

async fn resolve(root: &Arc<CatalogManager>, entries: Vec<String>) -> SearchPath {
    let candidates: Vec<(String, String)> = entries
        .iter()
        .filter(|e| *e != "$user")
        .map(|e| match e.split_once('.') {
            Some((c, s)) => (c.to_string(), s.to_string()),
            None => (root.default_catalog().to_string(), e.clone()),
        })
        .collect();
    // The first entry naming an existing schema wins. If none does (e.g.
    // PostgreSQL's `"$user", public`), keep the server default rather than
    // pointing unqualified names at a schema that doesn't exist.
    let mut chosen = None;
    for (catalog, schema) in &candidates {
        if let Some(provider) = root.catalog(catalog) {
            if provider.schema(schema).await.is_some() {
                chosen = Some((catalog.clone(), schema.clone()));
                break;
            }
        }
    }
    let display = entries.join(", ");
    match chosen {
        Some((c, s)) if c != root.default_catalog() || s != root.default_schema() => SearchPath {
            display,
            catalog_manager: Arc::new(root.with_session_defaults(c, s)),
        },
        _ => SearchPath {
            display,
            catalog_manager: Arc::clone(root),
        },
    }
}

fn show_result(value: &str) -> MetadataResult {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "search_path",
        DataType::Utf8,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec![value]))],
    )
    .map_err(|e| e.to_string())?;
    Ok(MetadataResponse::Query(
        schema.fields().to_vec(),
        vec![batch],
    ))
}

/// Strip a leading case-insensitive keyword followed by a word boundary.
fn keyword<'a>(s: &'a str, kw: &str) -> Option<&'a str> {
    let s = s.trim_start();
    let head = s.get(..kw.len())?;
    let rest = &s[kw.len()..];
    let boundary = rest
        .chars()
        .next()
        .is_none_or(|c| !(c.is_alphanumeric() || c == '_'));
    (head.eq_ignore_ascii_case(kw) && boundary).then_some(rest)
}

fn parse(sql: &str) -> Option<Command> {
    let sql = sql.trim().trim_end_matches(';').trim_end();
    if let Some(rest) = keyword(sql, "show") {
        return keyword(rest, "search_path")
            .filter(|r| r.trim().is_empty())
            .map(|_| Command::Show);
    }
    if let Some(rest) = keyword(sql, "reset") {
        return keyword(rest, "search_path")
            .or_else(|| keyword(rest, "all"))
            .filter(|r| r.trim().is_empty())
            .map(|_| Command::Reset);
    }
    let rest = keyword(sql, "set")?;
    let rest = keyword(rest, "session")
        .or_else(|| keyword(rest, "local"))
        .unwrap_or(rest);
    let rest = keyword(rest, "search_path")?.trim_start();
    let value = rest
        .strip_prefix('=')
        .or_else(|| keyword(rest, "to"))?
        .trim();
    if value.eq_ignore_ascii_case("default") {
        return Some(Command::Reset);
    }
    let entries = value
        .split(',')
        .map(|e| e.trim().trim_matches('\'').replace('"', ""))
        .filter(|e| !e.is_empty())
        .collect();
    Some(Command::Set(entries))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set_entries(sql: &str) -> Vec<String> {
        match parse(sql) {
            Some(Command::Set(e)) => e,
            _ => panic!("not a SET search_path: {sql}"),
        }
    }

    #[test]
    fn parses_set_forms() {
        assert_eq!(set_entries("SET search_path = tpch"), ["tpch"]);
        assert_eq!(set_entries("set search_path to tpch;"), ["tpch"]);
        assert_eq!(set_entries("SET SESSION search_path TO tpch"), ["tpch"]);
        assert_eq!(
            set_entries("SET search_path = 'a', \"B\", c.d"),
            ["a", "B", "c.d"]
        );
        assert_eq!(
            set_entries("SET search_path = \"$user\", public"),
            ["$user", "public"]
        );
        assert!(matches!(
            parse("SET search_path TO DEFAULT"),
            Some(Command::Reset)
        ));
        assert!(matches!(parse("RESET search_path"), Some(Command::Reset)));
        assert!(matches!(parse("RESET ALL"), Some(Command::Reset)));
        assert!(matches!(parse("show search_path"), Some(Command::Show)));
    }

    #[test]
    fn ignores_other_statements() {
        assert!(parse("SET client_encoding = 'UTF8'").is_none());
        assert!(parse("SET search_path_x = a").is_none());
        assert!(parse("SHOW server_version").is_none());
        assert!(parse("RESET client_encoding").is_none());
        assert!(parse("SELECT 1").is_none());
    }
}
