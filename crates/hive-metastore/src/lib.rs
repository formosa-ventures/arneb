//! Hive Metastore Thrift client for Arneb.
//!
//! This crate contains auto-generated Rust bindings for the Hive Metastore
//! Thrift API, produced by `volo-build` from Hive 4.2.0's `hive_metastore.thrift`.
//!
//! # Regeneration
//!
//! The generated code lives in `src/hms.rs`. It is committed to the repo —
//! `cargo build` does NOT regenerate it. To regenerate after updating
//! `thrift_idl/hive_metastore.thrift`:
//!
//! ```bash
//! cargo run -p hive-metastore-thrift-build
//! ```
//!
//! # Supported HMS Versions
//!
//! - **HMS 4.x** (4.0.1, 4.2.0): primary target, tested via E2E tests
//! - **HMS 5.x** (future): compatible by design — only `_req` methods are used
//! - **HMS 3.x**: best-effort; legacy methods still exist in the IDL but not actively tested

#![allow(
    clippy::all,
    warnings,
    unused_imports,
    dead_code,
    non_snake_case,
    non_camel_case_types
)]

mod hms;

// The generated code wraps everything in `hms::hms::{fb303, hive_metastore}`.
// Re-export the Hive service types at the crate root so callers can write
// `hive_metastore::GetTableRequest` directly.
pub use hms::hms::hive_metastore::*;

// Re-export fb303 under its namespace in case anything upstream needs it.
pub use hms::hms::fb303;

// Re-export commonly used runtime types from dependencies so callers don't
// need to depend on pilota/volo-thrift directly for simple cases.
pub use pilota::FastStr;
pub use volo_thrift::MaybeException;
