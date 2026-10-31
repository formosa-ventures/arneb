//! The Web API error envelope: `{"error":{"code","message","hint"}}`.
//!
//! Every [`ErrorCode`] maps to exactly one HTTP status. `/api/v1` keeps its
//! historical 400/404 bodies; only its new 401/403 responses use this.

use arneb_protocol::agent::{AgentError, AgentErrorCode, SqlLocation};
use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;

/// A machine-readable error code, serialized in snake_case.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    /// No valid credentials (401, `WWW-Authenticate: Bearer realm="arneb"`).
    Unauthenticated,
    /// Authenticated, but the access rules deny it (403).
    Forbidden,
    /// No such object, or one the caller may not see (404).
    NotFound,
    /// A malformed request (400).
    InvalidArgument,
    /// The operation needs `[auth] type = "password"` (409).
    AuthDisabled,
    /// Too many pending verifications; retry (503, `Retry-After: 1`).
    Overloaded,
    /// An unexpected server-side failure (500).
    Internal,
    /// The SQL does not parse (400).
    SyntaxError,
    /// No such catalog, or one the caller may not see (404).
    CatalogNotFound,
    /// No such schema, or one the caller may not see (404).
    SchemaNotFound,
    /// No such table, or one the caller may not see (404).
    TableNotFound,
    /// No such column (400).
    ColumnNotFound,
    /// No such function (400).
    FunctionNotFound,
    /// Operand types do not fit (400).
    TypeMismatch,
    /// The engine does not support the statement (400).
    NotSupported,
    /// Any other user error while running a statement (400).
    QueryFailed,
    /// The statement exceeded its memory limit (400).
    ExceededMemory,
    /// The statement exceeded its time limit (400).
    ExceededTime,
    /// The statement was cancelled (409).
    Cancelled,
    /// A manage action under `[auth] type = "none"` (403).
    ManageRequiresAuth,
}

impl ErrorCode {
    /// The HTTP status this code is sent with.
    pub fn status(self) -> StatusCode {
        match self {
            Self::Unauthenticated => StatusCode::UNAUTHORIZED,
            Self::Forbidden | Self::ManageRequiresAuth => StatusCode::FORBIDDEN,
            Self::NotFound => StatusCode::NOT_FOUND,
            Self::InvalidArgument
            | Self::SyntaxError
            | Self::ColumnNotFound
            | Self::FunctionNotFound
            | Self::TypeMismatch
            | Self::NotSupported
            | Self::QueryFailed
            | Self::ExceededMemory
            | Self::ExceededTime => StatusCode::BAD_REQUEST,
            Self::CatalogNotFound | Self::SchemaNotFound | Self::TableNotFound => {
                StatusCode::NOT_FOUND
            }
            Self::Cancelled => StatusCode::CONFLICT,
            Self::AuthDisabled => StatusCode::CONFLICT,
            Self::Overloaded => StatusCode::SERVICE_UNAVAILABLE,
            Self::Internal => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

/// A Web API error, rendered as the error envelope.
#[derive(Debug, Clone, Serialize)]
pub struct ApiError {
    pub code: ErrorCode,
    pub message: String,
    pub hint: Option<String>,
    /// Where in the SQL the error is, when known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<SqlLocation>,
    /// Nearest existing names (column typos).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub candidates: Vec<String>,
}

impl ApiError {
    /// An error with no hint.
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            hint: None,
            location: None,
            candidates: Vec::new(),
        }
    }

    /// Attach a hint telling the caller what to do.
    pub fn with_hint(mut self, hint: impl Into<String>) -> Self {
        self.hint = Some(hint.into());
        self
    }
}

impl From<AgentError> for ApiError {
    fn from(e: AgentError) -> Self {
        use AgentErrorCode as A;
        let code = match e.code {
            A::SyntaxError => ErrorCode::SyntaxError,
            A::CatalogNotFound => ErrorCode::CatalogNotFound,
            A::SchemaNotFound => ErrorCode::SchemaNotFound,
            A::TableNotFound => ErrorCode::TableNotFound,
            A::ColumnNotFound => ErrorCode::ColumnNotFound,
            A::FunctionNotFound => ErrorCode::FunctionNotFound,
            A::TypeMismatch => ErrorCode::TypeMismatch,
            A::NotSupported => ErrorCode::NotSupported,
            A::QueryFailed => ErrorCode::QueryFailed,
            A::ExceededMemory => ErrorCode::ExceededMemory,
            A::ExceededTime => ErrorCode::ExceededTime,
            A::Cancelled => ErrorCode::Cancelled,
            A::Forbidden => ErrorCode::Forbidden,
            A::Overloaded => ErrorCode::Overloaded,
            A::InvalidArgument => ErrorCode::InvalidArgument,
            A::NotFound => ErrorCode::NotFound,
            A::Internal => ErrorCode::Internal,
        };
        Self {
            code,
            message: e.message,
            hint: e.hint,
            location: e.location,
            candidates: e.candidates,
        }
    }
}

/// The `{"error": …}` body shared by the HTTP routes and MCP tool errors.
#[derive(Serialize)]
pub(crate) struct Envelope<'a> {
    pub(crate) error: &'a ApiError,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut resp = (self.code.status(), Json(Envelope { error: &self })).into_response();
        let headers = resp.headers_mut();
        match self.code {
            ErrorCode::Unauthenticated => {
                headers.insert(
                    header::WWW_AUTHENTICATE,
                    HeaderValue::from_static(r#"Bearer realm="arneb""#),
                );
            }
            ErrorCode::Overloaded => {
                headers.insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
            }
            _ => {}
        }
        resp
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn codes_map_to_statuses_and_the_envelope_shape() {
        for (code, status, name) in [
            (ErrorCode::Unauthenticated, 401, "unauthenticated"),
            (ErrorCode::Forbidden, 403, "forbidden"),
            (ErrorCode::NotFound, 404, "not_found"),
            (ErrorCode::InvalidArgument, 400, "invalid_argument"),
            (ErrorCode::AuthDisabled, 409, "auth_disabled"),
            (ErrorCode::Overloaded, 503, "overloaded"),
            (ErrorCode::Internal, 500, "internal"),
            (ErrorCode::SyntaxError, 400, "syntax_error"),
            (ErrorCode::CatalogNotFound, 404, "catalog_not_found"),
            (ErrorCode::SchemaNotFound, 404, "schema_not_found"),
            (ErrorCode::TableNotFound, 404, "table_not_found"),
            (ErrorCode::ColumnNotFound, 400, "column_not_found"),
            (ErrorCode::FunctionNotFound, 400, "function_not_found"),
            (ErrorCode::TypeMismatch, 400, "type_mismatch"),
            (ErrorCode::NotSupported, 400, "not_supported"),
            (ErrorCode::QueryFailed, 400, "query_failed"),
            (ErrorCode::ExceededMemory, 400, "exceeded_memory"),
            (ErrorCode::ExceededTime, 400, "exceeded_time"),
            (ErrorCode::Cancelled, 409, "cancelled"),
            (ErrorCode::ManageRequiresAuth, 403, "manage_requires_auth"),
        ] {
            let resp = ApiError::new(code, "m").with_hint("h").into_response();
            assert_eq!(resp.status().as_u16(), status);
            let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
                .await
                .unwrap();
            let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(
                json,
                serde_json::json!({"error": {"code": name, "message": "m", "hint": "h"}})
            );
        }
        let resp = ApiError::new(ErrorCode::Unauthenticated, "m").into_response();
        assert_eq!(
            resp.headers()[header::WWW_AUTHENTICATE],
            r#"Bearer realm="arneb""#
        );
        let resp = ApiError::new(ErrorCode::Overloaded, "m").into_response();
        assert_eq!(resp.headers()[header::RETRY_AFTER], "1");
    }

    #[test]
    fn agent_errors_keep_location_and_candidates() {
        let mut e = AgentError::new(AgentErrorCode::ColumnNotFound, "no column");
        e.location = Some(SqlLocation { line: 1, column: 8 });
        e.candidates = vec!["l_quantity".into()];
        let api = ApiError::from(e);
        assert_eq!(api.code, ErrorCode::ColumnNotFound);
        assert_eq!(
            serde_json::to_value(&api).unwrap(),
            serde_json::json!({
                "code": "column_not_found", "message": "no column", "hint": null,
                "location": {"line": 1, "column": 8}, "candidates": ["l_quantity"],
            })
        );
    }
}
