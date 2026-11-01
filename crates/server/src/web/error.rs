//! The Web API error envelope: `{"error":{"code","message","hint"}}`.
//!
//! Every [`ErrorCode`] maps to exactly one HTTP status. `/api/v1` keeps its
//! historical 400/404 bodies; only its new 401/403 responses use this.

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
}

impl ErrorCode {
    /// The HTTP status this code is sent with.
    pub fn status(self) -> StatusCode {
        match self {
            Self::Unauthenticated => StatusCode::UNAUTHORIZED,
            Self::Forbidden => StatusCode::FORBIDDEN,
            Self::NotFound => StatusCode::NOT_FOUND,
            Self::InvalidArgument => StatusCode::BAD_REQUEST,
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
}

impl ApiError {
    /// An error with no hint.
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            hint: None,
        }
    }

    /// Attach a hint telling the caller what to do.
    pub fn with_hint(mut self, hint: impl Into<String>) -> Self {
        self.hint = Some(hint.into());
        self
    }
}

#[derive(Serialize)]
struct Envelope<'a> {
    error: &'a ApiError,
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
}
