use arneb_common::error::ArnebError;
use pgwire::error::{ErrorInfo, PgWireError};

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
#[allow(dead_code)]
pub enum ProtocolError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Pipeline(#[from] ArnebError),
}

pub fn arneb_error_to_pg_error(err: &ArnebError) -> PgWireError {
    let (sqlstate, message) = match err {
        ArnebError::Parse(e) => ("42601".to_string(), e.to_string()),
        ArnebError::Plan(e) => {
            let code = if e.to_string().contains("column") {
                "42703"
            } else {
                "42P01"
            };
            (code.to_string(), e.to_string())
        }
        ArnebError::Execution(e) => ("XX000".to_string(), e.to_string()),
        ArnebError::Connector(e) => ("58030".to_string(), e.to_string()),
        ArnebError::Catalog(e) => ("3D000".to_string(), e.to_string()),
        ArnebError::Config(e) => ("F0000".to_string(), e.to_string()),
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
    use arneb_common::error::*;

    #[test]
    fn test_parse_error_maps_to_42601() {
        let err = ArnebError::Parse(ParseError::InvalidSyntax("unexpected token".into()));
        let pg_err = arneb_error_to_pg_error(&err);
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
        let err = ArnebError::Plan(PlanError::TableNotFound("users".into()));
        let pg_err = arneb_error_to_pg_error(&err);
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
        let err = ArnebError::Plan(PlanError::ColumnNotFound("age".into()));
        let pg_err = arneb_error_to_pg_error(&err);
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
        let err = ArnebError::Execution(ExecutionError::InvalidOperation("bad op".into()));
        let pg_err = arneb_error_to_pg_error(&err);
        match pg_err {
            PgWireError::UserError(info) => {
                assert_eq!(info.code, "XX000");
            }
            _ => panic!("expected UserError"),
        }
    }

    #[test]
    fn test_catalog_error_maps_to_3d000() {
        let err = ArnebError::Catalog(CatalogError::CatalogNotFound("mycat".into()));
        let pg_err = arneb_error_to_pg_error(&err);
        match pg_err {
            PgWireError::UserError(info) => {
                assert_eq!(info.code, "3D000");
                assert!(info.message.contains("mycat"));
            }
            _ => panic!("expected UserError"),
        }
    }
}
