//! Engine adapter contract. Each adapter converts a SQL string into a
//! canonical row set plus a wall-clock measurement; the runner is generic
//! over the trait and contains no engine-specific branching.

use std::time::Duration;

use async_trait::async_trait;

use crate::canonical::CanonicalValue;

pub mod arneb;
pub mod datafusion;
pub mod trino;

#[derive(Debug)]
pub struct EngineResult {
    pub rows: Vec<Vec<CanonicalValue>>,
    pub elapsed: Duration,
}

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error("connection error: {0}")]
    Connect(String),
    #[error("query error: {0}")]
    Query(String),
}

#[async_trait]
pub trait BenchmarkEngine: Send {
    /// Stable engine name. Becomes the `engine` field in result JSON and
    /// the column header in reports.
    fn name(&self) -> &'static str;

    /// Host string for the result file metadata. In-process engines can
    /// return `"in-process"` or similar.
    fn host(&self) -> String;

    /// Port for the result file metadata. `None` for in-process engines.
    fn port(&self) -> Option<u16>;

    /// Prepare any connection state. Called once per engine, before any
    /// queries are executed.
    async fn connect(&mut self) -> Result<(), EngineError>;

    /// Execute a SQL string and return the canonical rows plus the
    /// wall-clock duration of the call (measured from just before send
    /// to just after the last row is materialized).
    async fn execute(&mut self, sql: &str) -> Result<EngineResult, EngineError>;
}
