#![warn(missing_docs)]
#![warn(unreachable_pub)]
#![deny(unsafe_code)]

//! Shared types, error handling, and configuration for arneb.

pub mod config;
pub mod diagnostic;
pub mod error;
pub mod identifiers;
pub mod stream;
pub mod types;

pub use config::ServerConfig;
pub use error::ArnebError;
pub use identifiers::{QueryId, SplitId, StageId, TaskId};
pub use stream::{
    collect_stream, stream_from_batches, RecordBatchStream, SendableRecordBatchStream,
};
pub use types::{
    ColumnInfo, DataType, InvalidTableReference, ScalarValue, TableReference, TimeUnit,
    UnsupportedArrowType,
};
