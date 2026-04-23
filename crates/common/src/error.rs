//! Error types for the arneb query engine.
//!
//! Variants that reference a specific SQL source construct carry an
//! optional `location: Option<Location>` field. The [`Display`] impl —
//! produced by `thiserror` — emits only the message body; line/column
//! prefixes and source snippets are the job of
//! [`crate::diagnostic::render_plan_error`]. This keeps
//! `err.to_string()` stable for log lines and test assertions that
//! don't care about position.

use thiserror::Error;

pub use sqlparser::tokenizer::Location;

use crate::types::DataType;

/// Top-level error type composing all domain errors.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ArnebError {
    /// SQL parsing error.
    #[error(transparent)]
    Parse(#[from] ParseError),

    /// Query planning error.
    #[error(transparent)]
    Plan(#[from] PlanError),

    /// Query execution error.
    #[error(transparent)]
    Execution(#[from] ExecutionError),

    /// Data source connector error.
    #[error(transparent)]
    Connector(#[from] ConnectorError),

    /// Catalog metadata error.
    #[error(transparent)]
    Catalog(#[from] CatalogError),

    /// Configuration loading error.
    #[error(transparent)]
    Config(#[from] ConfigError),
}

/// Errors from SQL parsing.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ParseError {
    /// The SQL statement contains a syntax error.
    #[error("invalid syntax: {0}")]
    InvalidSyntax(String),

    /// The SQL statement uses a feature not yet supported.
    #[error("unsupported feature: {0}")]
    UnsupportedFeature(String),
}

impl ParseError {
    /// Returns the source location associated with this error, if any.
    ///
    /// `ParseError` variants currently do not carry location information —
    /// syntax errors are sourced from the upstream parser and are kept
    /// position-free at this layer. Wire location through here when a
    /// future variant (e.g., `InvalidIdentifier { location }`) is added.
    pub fn location(&self) -> Option<Location> {
        None
    }
}

/// Errors from query planning.
///
/// Variants that reference a specific source construct carry
/// `location: Option<Location>`. Use [`PlanError::location`] to retrieve
/// it without matching every variant.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum PlanError {
    /// The referenced table does not exist in any catalog.
    #[error("table not found: {0}")]
    TableNotFound(String),

    /// The referenced column does not exist in the table schema.
    #[error("column not found: {name}")]
    ColumnNotFound {
        /// Column name that failed to resolve.
        name: String,
        /// Source position of the column reference, if known.
        location: Option<Location>,
    },

    /// An expression has incompatible types.
    #[error("type mismatch: expected {expected}, found {found}")]
    TypeMismatch {
        /// The type that was expected.
        expected: DataType,
        /// The type that was actually found.
        found: DataType,
        /// Source position of the offending expression, if known.
        location: Option<Location>,
    },

    /// An expression is syntactically valid but semantically invalid.
    #[error("invalid expression: {message}")]
    InvalidExpression {
        /// Explanation of what went wrong.
        message: String,
        /// Source position of the offending expression, if known.
        location: Option<Location>,
    },

    /// A referenced function name does not exist in the scalar or
    /// aggregate registry.
    #[error("function not found: {name}")]
    FunctionNotFound {
        /// Function name that failed to resolve.
        name: String,
        /// Source position of the call site, if known.
        location: Option<Location>,
    },

    /// The planner does not support the requested expression construct.
    #[error("unsupported expression: {message}")]
    UnsupportedExpression {
        /// Human-readable description of what is unsupported.
        message: String,
        /// Source position of the offending expression, if known.
        location: Option<Location>,
    },

    /// A column name is ambiguous (matches more than one input relation).
    #[error("ambiguous column reference: {name}")]
    AmbiguousReference {
        /// Column name that matched multiple sources.
        name: String,
        /// Source position of the column reference, if known.
        location: Option<Location>,
    },

    /// A literal value is malformed for its declared target type —
    /// detected at plan time by [`crate::diagnostic`] / constant
    /// folding when converting `Cast(Literal)` (e.g., unparseable
    /// `DATE '1998-13-45'`).
    #[error("invalid literal: {message}")]
    InvalidLiteral {
        /// Human-readable explanation of the parse / conversion failure.
        message: String,
        /// Source position of the offending literal, if known.
        location: Option<Location>,
    },

    /// Extended-query parameter inferred as two incompatible types at
    /// different usage sites (e.g., `$1` used in `a <= $1` against a
    /// Date column AND in `b = $1` against an Int column).
    #[error("parameter ${index} inferred as incompatible types: {conflict_types}")]
    ParameterTypeConflict {
        /// 1-based placeholder index.
        index: usize,
        /// Human-readable list of the conflicting types.
        conflict_types: String,
        /// Source position of the second inference site, if known.
        location: Option<Location>,
    },

    /// An internal planner invariant was violated. These errors carry no
    /// location because they point at engine-internal state rather than
    /// user SQL.
    #[error("planner internal error: {0}")]
    InternalError(String),
}

impl PlanError {
    /// Returns the source location attached to this error, if the
    /// variant carries one. This lets diagnostics render `file:line:col`
    /// without matching every variant explicitly.
    pub fn location(&self) -> Option<Location> {
        match self {
            PlanError::ColumnNotFound { location, .. }
            | PlanError::TypeMismatch { location, .. }
            | PlanError::InvalidExpression { location, .. }
            | PlanError::FunctionNotFound { location, .. }
            | PlanError::UnsupportedExpression { location, .. }
            | PlanError::AmbiguousReference { location, .. }
            | PlanError::InvalidLiteral { location, .. }
            | PlanError::ParameterTypeConflict { location, .. } => *location,
            PlanError::TableNotFound(_) | PlanError::InternalError(_) => None,
        }
    }

    /// Short helper to construct `ColumnNotFound` without a location.
    /// Keeps existing call sites `PlanError::ColumnNotFound(name)` working
    /// with a tiny migration (`PlanError::column_not_found(name)`).
    pub fn column_not_found(name: impl Into<String>) -> Self {
        Self::ColumnNotFound {
            name: name.into(),
            location: None,
        }
    }

    /// Short helper to construct `InvalidExpression` without a location.
    pub fn invalid_expression(message: impl Into<String>) -> Self {
        Self::InvalidExpression {
            message: message.into(),
            location: None,
        }
    }
}

/// Errors from query execution.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ExecutionError {
    /// An error propagated from the Arrow compute layer.
    #[error("arrow error: {0}")]
    ArrowError(#[from] arrow::error::ArrowError),

    /// An operation that is not valid in the current execution context.
    #[error("invalid operation: {0}")]
    InvalidOperation(String),

    /// A resource limit (memory, threads, etc.) has been exceeded.
    #[error("resource exhausted: {0}")]
    ResourceExhausted(String),
}

/// Errors from data source connectors.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ConnectorError {
    /// Failed to establish a connection to the data source.
    #[error("connection failed: {0}")]
    ConnectionFailed(String),

    /// The requested table does not exist in the data source.
    #[error("table not found: {0}")]
    TableNotFound(String),

    /// An error occurred while reading data from the source.
    #[error("read error: {0}")]
    ReadError(String),

    /// The connector does not support the requested operation.
    #[error("unsupported operation: {0}")]
    UnsupportedOperation(String),
}

/// Errors from the catalog system.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum CatalogError {
    /// The named catalog is not registered.
    #[error("catalog not found: {0}")]
    CatalogNotFound(String),

    /// The named schema does not exist within its catalog.
    #[error("schema not found: {0}")]
    SchemaNotFound(String),

    /// A table with this name already exists in the schema.
    #[error("table already exists: {0}")]
    TableAlreadyExists(String),
}

/// Errors from configuration loading.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ConfigError {
    /// The configuration file does not exist at the given path.
    #[error("config file not found: {0}")]
    FileNotFound(String),

    /// The configuration file could not be parsed.
    #[error("config parse error: {0}")]
    ParseError(String),

    /// A configuration value failed validation.
    #[error("invalid config value for '{key}': '{value}' ({reason})")]
    InvalidValue {
        /// The configuration key that has an invalid value.
        key: String,
        /// The invalid value that was provided.
        value: String,
        /// Why the value is invalid.
        reason: String,
    },
}

/// Convenience type alias for Results using ArnebError.
pub type Result<T> = std::result::Result<T, ArnebError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_error_display() {
        let err = ParseError::InvalidSyntax("unexpected token SELCT".to_string());
        assert_eq!(err.to_string(), "invalid syntax: unexpected token SELCT");
    }

    #[test]
    fn parse_error_unsupported_display() {
        let err = ParseError::UnsupportedFeature("LATERAL JOIN".to_string());
        assert_eq!(err.to_string(), "unsupported feature: LATERAL JOIN");
    }

    #[test]
    fn plan_error_type_mismatch_display() {
        let err = PlanError::TypeMismatch {
            expected: DataType::Int64,
            found: DataType::Utf8,
            location: None,
        };
        assert_eq!(err.to_string(), "type mismatch: expected Int64, found Utf8");
    }

    #[test]
    fn plan_error_location_accessor() {
        let loc = Location {
            line: 3,
            column: 19,
        };
        let err = PlanError::ColumnNotFound {
            name: "l_shipdate".to_string(),
            location: Some(loc),
        };
        assert_eq!(err.location(), Some(loc));

        let err = PlanError::TableNotFound("users".to_string());
        assert_eq!(err.location(), None);
    }

    #[test]
    fn execution_error_from_arrow() {
        let arrow_err = arrow::error::ArrowError::ComputeError("test".to_string());
        let exec_err = ExecutionError::from(arrow_err);
        assert!(exec_err.to_string().contains("arrow error"));
        // Verify source() chaining
        assert!(std::error::Error::source(&exec_err).is_some());
    }

    #[test]
    fn connector_error_display() {
        let err = ConnectorError::ConnectionFailed("timeout after 30s".to_string());
        assert_eq!(err.to_string(), "connection failed: timeout after 30s");
    }

    #[test]
    fn catalog_error_display() {
        let err = CatalogError::SchemaNotFound("public".to_string());
        assert_eq!(err.to_string(), "schema not found: public");
    }

    #[test]
    fn config_error_invalid_value_display() {
        let err = ConfigError::InvalidValue {
            key: "port".to_string(),
            value: "not_a_number".to_string(),
            reason: "expected u16".to_string(),
        };
        assert_eq!(
            err.to_string(),
            "invalid config value for 'port': 'not_a_number' (expected u16)"
        );
    }

    #[test]
    fn arneb_error_from_parse() {
        let parse_err = ParseError::InvalidSyntax("bad sql".to_string());
        let arneb_err: ArnebError = parse_err.into();
        assert!(matches!(arneb_err, ArnebError::Parse(_)));
        assert!(arneb_err.to_string().contains("bad sql"));
    }

    #[test]
    fn arneb_error_from_plan() {
        let plan_err = PlanError::TableNotFound("users".to_string());
        let arneb_err: ArnebError = plan_err.into();
        assert!(matches!(arneb_err, ArnebError::Plan(_)));
    }

    #[test]
    fn arneb_error_from_execution() {
        let exec_err = ExecutionError::ResourceExhausted("memory limit exceeded".to_string());
        let arneb_err: ArnebError = exec_err.into();
        assert!(matches!(arneb_err, ArnebError::Execution(_)));
    }

    #[test]
    fn arneb_error_from_connector() {
        let conn_err = ConnectorError::ReadError("disk full".to_string());
        let arneb_err: ArnebError = conn_err.into();
        assert!(matches!(arneb_err, ArnebError::Connector(_)));
    }

    #[test]
    fn arneb_error_from_catalog() {
        let cat_err = CatalogError::TableAlreadyExists("users".to_string());
        let arneb_err: ArnebError = cat_err.into();
        assert!(matches!(arneb_err, ArnebError::Catalog(_)));
    }

    #[test]
    fn arneb_error_from_config() {
        let cfg_err = ConfigError::FileNotFound("/etc/trino.toml".to_string());
        let arneb_err: ArnebError = cfg_err.into();
        assert!(matches!(arneb_err, ArnebError::Config(_)));
    }

    #[test]
    fn execution_error_source_chain() {
        let arrow_err = arrow::error::ArrowError::InvalidArgumentError("bad arg".to_string());
        let exec_err = ExecutionError::ArrowError(arrow_err);
        let source = std::error::Error::source(&exec_err).unwrap();
        let downcast = source.downcast_ref::<arrow::error::ArrowError>();
        assert!(downcast.is_some());
    }
}
