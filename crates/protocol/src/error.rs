use pgwire::error::{ErrorInfo, PgWireError};
use trino_common::error::TrinoError;

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
#[allow(dead_code)]
pub enum ProtocolError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Pipeline(#[from] TrinoError),
}

pub fn trino_error_to_pg_error(err: &TrinoError) -> PgWireError {
    let (sqlstate, message) = match err {
        TrinoError::Parse(e) => ("42601".to_string(), e.to_string()),
        TrinoError::Plan(e) => {
            let code = if e.to_string().contains("column") {
                "42703"
            } else {
                "42P01"
            };
            (code.to_string(), e.to_string())
        }
        TrinoError::Execution(e) => ("XX000".to_string(), e.to_string()),
        TrinoError::Connector(e) => ("58030".to_string(), e.to_string()),
        TrinoError::Catalog(e) => ("3D000".to_string(), e.to_string()),
        TrinoError::Config(e) => ("F0000".to_string(), e.to_string()),
        _ => ("XX000".to_string(), err.to_string()),
    };

    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        sqlstate,
        message,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use trino_common::error::*;

    #[test]
    fn test_parse_error_maps_to_42601() {
        let err = TrinoError::Parse(ParseError::InvalidSyntax("unexpected token".into()));
        let pg_err = trino_error_to_pg_error(&err);
        match pg_err {
            PgWireError::UserError(info) => {
                assert_eq!(info.code, "42601");
                assert!(info.message.contains("unexpected token"));
            }
            _ => panic!("expected UserError"),
        }
    }

    #[test]
    fn test_plan_error_maps_to_42p01() {
        let err = TrinoError::Plan(PlanError::TableNotFound("users".into()));
        let pg_err = trino_error_to_pg_error(&err);
        match pg_err {
            PgWireError::UserError(info) => {
                assert_eq!(info.code, "42P01");
                assert!(info.message.contains("users"));
            }
            _ => panic!("expected UserError"),
        }
    }

    #[test]
    fn test_plan_error_column_maps_to_42703() {
        let err = TrinoError::Plan(PlanError::ColumnNotFound("age".into()));
        let pg_err = trino_error_to_pg_error(&err);
        match pg_err {
            PgWireError::UserError(info) => {
                assert_eq!(info.code, "42703");
                assert!(info.message.contains("age"));
            }
            _ => panic!("expected UserError"),
        }
    }

    #[test]
    fn test_execution_error_maps_to_xx000() {
        let err = TrinoError::Execution(ExecutionError::InvalidOperation("bad op".into()));
        let pg_err = trino_error_to_pg_error(&err);
        match pg_err {
            PgWireError::UserError(info) => {
                assert_eq!(info.code, "XX000");
            }
            _ => panic!("expected UserError"),
        }
    }

    #[test]
    fn test_catalog_error_maps_to_3d000() {
        let err = TrinoError::Catalog(CatalogError::CatalogNotFound("mycat".into()));
        let pg_err = trino_error_to_pg_error(&err);
        match pg_err {
            PgWireError::UserError(info) => {
                assert_eq!(info.code, "3D000");
                assert!(info.message.contains("mycat"));
            }
            _ => panic!("expected UserError"),
        }
    }
}
