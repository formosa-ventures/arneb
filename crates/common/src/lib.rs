#![warn(missing_docs)]
#![warn(unreachable_pub)]
#![deny(unsafe_code)]

//! Shared types, error handling, and configuration for arneb.

pub mod config;
pub mod diagnostic;
pub mod domain;
pub mod dynamic_filter;
pub mod error;
pub mod identifiers;
pub mod inflight_budget;
pub mod memory_pool;
pub mod memory_profile;
pub mod stream;
pub mod types;

pub use config::ServerConfig;
pub use domain::{bloom_dynamic_filter_enabled, BloomFilter, Domain, DEFAULT_MAX_DISTINCT_VALUES};
pub use dynamic_filter::DynamicFilterId;
pub use error::ArnebError;
pub use identifiers::{QueryId, SplitId, StageId, TaskId};
pub use stream::{
    collect_stream, stream_from_batches, RecordBatchStream, SendableRecordBatchStream,
};
pub use types::{
    ColumnInfo, DataType, InvalidTableReference, ScalarValue, TableReference, TimeUnit,
    UnsupportedArrowType,
};
