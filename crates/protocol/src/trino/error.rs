//! Mapping from [`ArnebError`] to the Trino `QueryError` JSON object.

use arneb_common::diagnostic::{render_plan_error, SourceFile};
use arneb_common::error::{ArnebError, CatalogError, ConnectorError, ExecutionError, PlanError};
use serde_json::{json, Value};

/// A Trino `QueryError`: the `(errorCode, errorName, errorType)` triple
/// matches Trino's `StandardErrorCode` so client-side error classification
/// (e.g. "user error vs. server error" retries) behaves the same.
#[derive(Debug, Clone)]
pub(crate) struct TrinoError {
    pub(crate) message: String,
    pub(crate) code: i64,
    pub(crate) name: &'static str,
    pub(crate) kind: &'static str,
}

const USER_ERROR: &str = "USER_ERROR";
const INTERNAL_ERROR: &str = "INTERNAL_ERROR";
const INSUFFICIENT_RESOURCES: &str = "INSUFFICIENT_RESOURCES";
const EXTERNAL: &str = "EXTERNAL";

impl TrinoError {
    pub(crate) fn user(code: i64, name: &'static str, message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            code,
            name,
            kind: USER_ERROR,
        }
    }

    pub(crate) fn not_supported(message: impl Into<String>) -> Self {
        Self::user(13, "NOT_SUPPORTED", message)
    }

    pub(crate) fn canceled() -> Self {
        Self::user(3, "USER_CANCELED", "Query was canceled")
    }

    pub(crate) fn abandoned() -> Self {
        Self::user(
            2,
            "ABANDONED_QUERY",
            "Query was abandoned by the client (no request within the client timeout)",
        )
    }

    pub(crate) fn internal(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            code: 65536,
            name: "GENERIC_INTERNAL_ERROR",
            kind: INTERNAL_ERROR,
        }
    }

    /// Classifies an engine error. `source` enables the caret-annotated
    /// planner diagnostics already used by the pgwire path.
    pub(crate) fn from_arneb(err: &ArnebError, source: Option<&SourceFile>) -> Self {
        let message = match (err, source) {
            (ArnebError::Plan(e), Some(src)) => render_plan_error(e, src),
            _ => err.to_string(),
        };
        let (code, name, kind) = match err {
            ArnebError::Parse(_) => (1, "SYNTAX_ERROR", USER_ERROR),
            ArnebError::Plan(e) => match e {
                PlanError::TableNotFound(_) => (46, "TABLE_NOT_FOUND", USER_ERROR),
                PlanError::ColumnNotFound { .. } => (47, "COLUMN_NOT_FOUND", USER_ERROR),
                PlanError::FunctionNotFound { .. } => (6, "FUNCTION_NOT_FOUND", USER_ERROR),
                PlanError::TypeMismatch { .. } => (58, "TYPE_MISMATCH", USER_ERROR),
                PlanError::UnsupportedExpression { .. } => (13, "NOT_SUPPORTED", USER_ERROR),
                PlanError::InternalError(_) => (65536, "GENERIC_INTERNAL_ERROR", INTERNAL_ERROR),
                _ => (0, "GENERIC_USER_ERROR", USER_ERROR),
            },
            ArnebError::Catalog(e) => match e {
                CatalogError::CatalogNotFound(_) => (44, "CATALOG_NOT_FOUND", USER_ERROR),
                CatalogError::SchemaNotFound(_) => (45, "SCHEMA_NOT_FOUND", USER_ERROR),
                CatalogError::TableAlreadyExists(_) => (50, "TABLE_ALREADY_EXISTS", USER_ERROR),
                #[allow(unreachable_patterns)]
                _ => (0, "GENERIC_USER_ERROR", USER_ERROR),
            },
            ArnebError::Execution(ExecutionError::ResourceExhausted(_)) => (
                131079,
                "EXCEEDED_LOCAL_MEMORY_LIMIT",
                INSUFFICIENT_RESOURCES,
            ),
            ArnebError::Connector(ConnectorError::TableNotFound(_)) => {
                (46, "TABLE_NOT_FOUND", USER_ERROR)
            }
            ArnebError::Connector(ConnectorError::UnsupportedOperation(_)) => {
                (13, "NOT_SUPPORTED", USER_ERROR)
            }
            // Trino reserves 0x0100_0000+ for connector-specific codes.
            ArnebError::Connector(_) => (16777216, "CONNECTOR_ERROR", EXTERNAL),
            _ => (65536, "GENERIC_INTERNAL_ERROR", INTERNAL_ERROR),
        };
        Self {
            message,
            code,
            name,
            kind,
        }
    }

    /// The `error` object of a `QueryResults` response.
    pub(crate) fn to_json(&self) -> Value {
        json!({
            "message": self.message,
            "errorCode": self.code,
            "errorName": self.name,
            "errorType": self.kind,
            "failureInfo": {
                "type": "io.trino.spi.TrinoException",
                "message": self.message,
                "suppressed": [],
                "stack": [],
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arneb_common::error::ParseError;

    #[test]
    fn classifies_common_errors() {
        let e = TrinoError::from_arneb(
            &ArnebError::Parse(ParseError::InvalidSyntax("bad".into())),
            None,
        );
        assert_eq!((e.code, e.name, e.kind), (1, "SYNTAX_ERROR", "USER_ERROR"));
        let e = TrinoError::from_arneb(
            &ArnebError::Plan(PlanError::TableNotFound("t".into())),
            None,
        );
        assert_eq!(e.name, "TABLE_NOT_FOUND");
        let json = e.to_json();
        assert_eq!(json["errorCode"], 46);
        assert_eq!(json["failureInfo"]["type"], "io.trino.spi.TrinoException");
    }
}
