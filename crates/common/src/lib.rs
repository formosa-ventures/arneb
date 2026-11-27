#![warn(missing_docs)]
#![warn(unreachable_pub)]
#![deny(unsafe_code)]

//! Shared types, error handling, and configuration for arneb.

pub mod array_text;
pub mod catalog_version;
pub mod config;
pub mod diagnostic;
pub mod domain;
pub mod duration;
pub mod dynamic_filter;
pub mod error;
pub mod identifiers;
pub mod inflight_budget;
pub mod memory_pool;
pub mod memory_profile;
pub mod metrics;
pub mod numeric_cast;
pub mod pg_oid;
pub mod pg_settings;
pub mod stream;
pub mod suggest;
pub mod types;

pub use catalog_version::{spec_fingerprint, CatalogVersion};
pub use config::ServerConfig;
pub use domain::{
    bloom_dynamic_filter_enabled, BloomFilter, Domain, DomainSummary, DEFAULT_MAX_DISTINCT_VALUES,
    DYNAMIC_FILTER_LOG_TARGET,
};
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
