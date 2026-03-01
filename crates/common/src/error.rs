//! Error types for the trino-alt query engine.

use thiserror::Error;

use crate::types::DataType;

/// Top-level error type composing all domain errors.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum TrinoError {
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

/// Errors from query planning.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum PlanError {
    /// The referenced table does not exist in any catalog.
    #[error("table not found: {0}")]
    TableNotFound(String),

    /// The referenced column does not exist in the table schema.
    #[error("column not found: {0}")]
    ColumnNotFound(String),

    /// An expression has incompatible types.
    #[error("type mismatch: expected {expected}, found {found}")]
    TypeMismatch {
        /// The type that was expected.
        expected: DataType,
        /// The type that was actually found.
        found: DataType,
    },

    /// An expression is syntactically valid but semantically invalid.
    #[error("invalid expression: {0}")]
    InvalidExpression(String),
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

/// Convenience type alias for Results using TrinoError.
pub type Result<T> = std::result::Result<T, TrinoError>;

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
        };
        assert_eq!(err.to_string(), "type mismatch: expected Int64, found Utf8");
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
    fn trino_error_from_parse() {
        let parse_err = ParseError::InvalidSyntax("bad sql".to_string());
        let trino_err: TrinoError = parse_err.into();
        assert!(matches!(trino_err, TrinoError::Parse(_)));
        assert!(trino_err.to_string().contains("bad sql"));
    }

    #[test]
    fn trino_error_from_plan() {
        let plan_err = PlanError::TableNotFound("users".to_string());
        let trino_err: TrinoError = plan_err.into();
        assert!(matches!(trino_err, TrinoError::Plan(_)));
    }

    #[test]
    fn trino_error_from_execution() {
        let exec_err = ExecutionError::ResourceExhausted("memory limit exceeded".to_string());
        let trino_err: TrinoError = exec_err.into();
        assert!(matches!(trino_err, TrinoError::Execution(_)));
    }

    #[test]
    fn trino_error_from_connector() {
        let conn_err = ConnectorError::ReadError("disk full".to_string());
        let trino_err: TrinoError = conn_err.into();
        assert!(matches!(trino_err, TrinoError::Connector(_)));
    }

    #[test]
    fn trino_error_from_catalog() {
        let cat_err = CatalogError::TableAlreadyExists("users".to_string());
        let trino_err: TrinoError = cat_err.into();
        assert!(matches!(trino_err, TrinoError::Catalog(_)));
    }

    #[test]
    fn trino_error_from_config() {
        let cfg_err = ConfigError::FileNotFound("/etc/trino.toml".to_string());
        let trino_err: TrinoError = cfg_err.into();
        assert!(matches!(trino_err, TrinoError::Config(_)));
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
