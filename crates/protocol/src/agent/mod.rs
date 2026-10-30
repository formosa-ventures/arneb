//! The agent query API service: an HTTP query API for AI agents on the
//! coordinator's Web port. Statements are submitted, polled and cancelled
//! through [`AgentQueryService`], which runs them on the Trino-protocol
//! engine; the configurable [`AgentLimits`] bound every request.

pub mod error;
pub mod limits;
pub mod results;
pub mod service;
pub mod statement;

pub use error::{AgentError, AgentErrorCode, SqlLocation};
pub use limits::AgentLimits;
pub use results::ResultPage;
pub use service::AgentQueryService;
pub use statement::{Column, StatementState, StatementStatus, SubmitRequest};
