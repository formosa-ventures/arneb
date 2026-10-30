//! Structured errors of the agent query API.

use serde::Serialize;

use crate::trino::TrinoError;

/// Machine-readable error code, serialized in snake_case.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentErrorCode {
    /// The SQL does not parse.
    SyntaxError,
    /// A referenced catalog does not exist (or is not visible).
    CatalogNotFound,
    /// A referenced schema does not exist (or is not visible).
    SchemaNotFound,
    /// A referenced table does not exist (or is not visible).
    TableNotFound,
    /// A referenced column does not exist.
    ColumnNotFound,
    /// A referenced function does not exist.
    FunctionNotFound,
    /// An expression's operand types do not fit.
    TypeMismatch,
    /// The statement uses a feature the engine does not support.
    NotSupported,
    /// Any other user error while running the statement.
    QueryFailed,
    /// The statement exceeded its memory limit.
    ExceededMemory,
    /// The statement exceeded its time limit.
    ExceededTime,
    /// The statement was cancelled.
    Cancelled,
    /// The user may not run the statement.
    Forbidden,
    /// Too many statements; retry later.
    Overloaded,
    /// The request itself is invalid.
    InvalidArgument,
    /// No such statement (or it belongs to another user).
    NotFound,
    /// An internal engine error.
    Internal,
}

/// Position in the SQL text (1-based line and column).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct SqlLocation {
    /// 1-based line number.
    pub line: u64,
    /// 1-based column number.
    pub column: u64,
}

/// An agent-API error: a code, a message, and the context an agent needs to
/// correct the request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, thiserror::Error)]
#[error("{message}")]
pub struct AgentError {
    /// Machine-readable code.
    pub code: AgentErrorCode,
    /// Human-readable message.
    pub message: String,
    /// What to do next, when there is a useful suggestion.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hint: Option<String>,
    /// Where in the SQL the error is, when known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<SqlLocation>,
    /// Nearest existing names (column typos).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub candidates: Vec<String>,
}

/// Hint for an overloaded rejection.
const OVERLOADED_HINT: &str = "retry after a running statement finishes";

// Statement-path hints. `<…>` marks a placeholder the agent fills in from
// its own statement; it is never a literal path segment.
const COLUMN_NOT_FOUND_HINT: &str =
    "list columns with GET /api/v2/catalogs/<catalog>/schemas/<schema>/tables/<table>, \
     using the names from your statement";
const TABLE_NOT_FOUND_HINT: &str =
    "list tables with GET /api/v2/catalogs/<catalog>/schemas/<schema>/tables, \
     using the names from your statement";
const SCHEMA_NOT_FOUND_HINT: &str = "list schemas with GET /api/v2/catalogs/<catalog>/schemas, \
     using the catalog name from your statement";
const CATALOG_NOT_FOUND_HINT: &str = "list catalogs with GET /api/v2/catalogs";

impl AgentError {
    /// An error with only a code and a message.
    pub fn new(code: AgentErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            hint: None,
            location: None,
            candidates: Vec::new(),
        }
    }

    /// Attach a hint.
    pub fn with_hint(mut self, hint: impl Into<String>) -> Self {
        self.hint = Some(hint.into());
        self
    }

    /// Too many statements; carries the retry hint.
    pub fn overloaded(message: impl Into<String>) -> Self {
        Self::new(AgentErrorCode::Overloaded, message).with_hint(OVERLOADED_HINT)
    }

    /// No such statement for this user.
    pub fn statement_not_found(id: &str) -> Self {
        Self::new(
            AgentErrorCode::NotFound,
            format!("statement {id} not found"),
        )
    }

    /// Maps a Trino-protocol error by its Trino error name, adding a hint
    /// where an agent can act on one.
    pub(crate) fn from_trino(err: &TrinoError) -> Self {
        use AgentErrorCode as C;
        let code = match err.name {
            "SYNTAX_ERROR" => C::SyntaxError,
            "CATALOG_NOT_FOUND" => C::CatalogNotFound,
            "SCHEMA_NOT_FOUND" => C::SchemaNotFound,
            "TABLE_NOT_FOUND" => C::TableNotFound,
            "COLUMN_NOT_FOUND" => C::ColumnNotFound,
            "FUNCTION_NOT_FOUND" => C::FunctionNotFound,
            "TYPE_MISMATCH" => C::TypeMismatch,
            "NOT_SUPPORTED" => C::NotSupported,
            "PERMISSION_DENIED" => C::Forbidden,
            "EXCEEDED_TIME_LIMIT" => C::ExceededTime,
            "USER_CANCELED" => C::Cancelled,
            "QUERY_QUEUE_FULL" => C::Overloaded,
            name if name.starts_with("EXCEEDED_") && name.ends_with("_MEMORY_LIMIT") => {
                C::ExceededMemory
            }
            _ if err.kind == "INTERNAL_ERROR" => C::Internal,
            _ => C::QueryFailed,
        };
        let location = err.location.map(|l| SqlLocation {
            line: l.line,
            column: l.column,
        });
        let hint = match code {
            C::ColumnNotFound if !err.candidates.is_empty() => {
                Some(format!("did you mean: {}?", err.candidates.join(", ")))
            }
            C::ColumnNotFound => Some(COLUMN_NOT_FOUND_HINT.to_string()),
            C::TableNotFound => Some(TABLE_NOT_FOUND_HINT.to_string()),
            C::SchemaNotFound => Some(SCHEMA_NOT_FOUND_HINT.to_string()),
            C::CatalogNotFound => Some(CATALOG_NOT_FOUND_HINT.to_string()),
            C::SyntaxError => {
                location.map(|l| format!("check the SQL near line {}, column {}", l.line, l.column))
            }
            C::Overloaded => Some(OVERLOADED_HINT.to_string()),
            _ => None,
        };
        Self {
            code,
            message: err.message.clone(),
            hint,
            location,
            candidates: err.candidates.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arneb_common::error::Location;
    use serde_json::json;

    fn trino(name: &'static str, kind: &'static str) -> TrinoError {
        let mut e = TrinoError::user(0, name, "boom");
        e.kind = kind;
        e
    }

    #[test]
    fn maps_trino_error_names() {
        use AgentErrorCode as C;
        let cases = [
            ("SYNTAX_ERROR", "USER_ERROR", C::SyntaxError),
            ("CATALOG_NOT_FOUND", "USER_ERROR", C::CatalogNotFound),
            ("SCHEMA_NOT_FOUND", "USER_ERROR", C::SchemaNotFound),
            ("TABLE_NOT_FOUND", "USER_ERROR", C::TableNotFound),
            ("COLUMN_NOT_FOUND", "USER_ERROR", C::ColumnNotFound),
            ("FUNCTION_NOT_FOUND", "USER_ERROR", C::FunctionNotFound),
            ("TYPE_MISMATCH", "USER_ERROR", C::TypeMismatch),
            ("NOT_SUPPORTED", "USER_ERROR", C::NotSupported),
            ("PERMISSION_DENIED", "USER_ERROR", C::Forbidden),
            (
                "EXCEEDED_LOCAL_MEMORY_LIMIT",
                "INSUFFICIENT_RESOURCES",
                C::ExceededMemory,
            ),
            (
                "EXCEEDED_TIME_LIMIT",
                "INSUFFICIENT_RESOURCES",
                C::ExceededTime,
            ),
            ("USER_CANCELED", "USER_ERROR", C::Cancelled),
            ("QUERY_QUEUE_FULL", "INSUFFICIENT_RESOURCES", C::Overloaded),
            ("GENERIC_USER_ERROR", "USER_ERROR", C::QueryFailed),
            ("CONNECTOR_ERROR", "EXTERNAL", C::QueryFailed),
            ("GENERIC_INTERNAL_ERROR", "INTERNAL_ERROR", C::Internal),
        ];
        for (name, kind, code) in cases {
            assert_eq!(
                AgentError::from_trino(&trino(name, kind)).code,
                code,
                "{name}"
            );
        }
    }

    #[test]
    fn hints_follow_the_error() {
        let mut e = trino("COLUMN_NOT_FOUND", "USER_ERROR");
        e.candidates = vec!["a".into(), "b".into()];
        assert_eq!(
            AgentError::from_trino(&e).hint.as_deref(),
            Some("did you mean: a, b?")
        );
        e.candidates.clear();
        assert_eq!(
            AgentError::from_trino(&e).hint.as_deref(),
            Some(
                "list columns with GET /api/v2/catalogs/<catalog>/schemas/<schema>/tables/<table>, \
                 using the names from your statement"
            )
        );
        let e = trino("TABLE_NOT_FOUND", "USER_ERROR");
        assert_eq!(
            AgentError::from_trino(&e).hint.as_deref(),
            Some(
                "list tables with GET /api/v2/catalogs/<catalog>/schemas/<schema>/tables, \
                 using the names from your statement"
            )
        );
        let e = trino("SCHEMA_NOT_FOUND", "USER_ERROR");
        assert_eq!(
            AgentError::from_trino(&e).hint.as_deref(),
            Some(
                "list schemas with GET /api/v2/catalogs/<catalog>/schemas, \
                 using the catalog name from your statement"
            )
        );
        let e = trino("CATALOG_NOT_FOUND", "USER_ERROR");
        assert_eq!(
            AgentError::from_trino(&e).hint.as_deref(),
            Some("list catalogs with GET /api/v2/catalogs")
        );
        let mut e = trino("SYNTAX_ERROR", "USER_ERROR");
        assert_eq!(AgentError::from_trino(&e).hint, None);
        e.location = Some(Location { line: 2, column: 5 });
        assert_eq!(
            AgentError::from_trino(&e).hint.as_deref(),
            Some("check the SQL near line 2, column 5")
        );
        let e = trino("QUERY_QUEUE_FULL", "INSUFFICIENT_RESOURCES");
        assert_eq!(
            AgentError::from_trino(&e).hint.as_deref(),
            Some("retry after a running statement finishes")
        );
    }

    #[test]
    fn serializes_without_empty_fields() {
        let e = AgentError::new(AgentErrorCode::QueryFailed, "boom");
        assert_eq!(
            serde_json::to_value(&e).unwrap(),
            json!({"code": "query_failed", "message": "boom"})
        );
        let mut e = trino("COLUMN_NOT_FOUND", "USER_ERROR");
        e.location = Some(Location { line: 1, column: 8 });
        e.candidates = vec!["x".into()];
        assert_eq!(
            serde_json::to_value(AgentError::from_trino(&e)).unwrap(),
            json!({
                "code": "column_not_found",
                "message": "boom",
                "hint": "did you mean: x?",
                "location": {"line": 1, "column": 8},
                "candidates": ["x"],
            })
        );
    }
}
