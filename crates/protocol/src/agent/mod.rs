//! The agent query API service: an HTTP query API for AI agents on the
//! coordinator's Web port. Statements are submitted, polled and cancelled
//! through [`AgentQueryService`], which runs them on the Trino-protocol
//! engine; the configurable [`AgentLimits`] bound every request.

pub mod discovery;
pub mod error;
pub mod explain;
pub mod limits;
pub mod results;
pub mod service;
pub mod statement;

pub use discovery::{ColumnDescription, ListRequest, Listing, Named, TableDescription, TableStats};
pub use error::{AgentError, AgentErrorCode, SqlLocation};
pub use explain::{ExplainRequest, Explanation, ScanEstimate};
pub use limits::AgentLimits;
pub use results::ResultPage;
pub use service::AgentQueryService;
pub use statement::{Column, StatementState, StatementStatus, SubmitRequest};
