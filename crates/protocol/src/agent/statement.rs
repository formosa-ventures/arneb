//! One agent-API statement: the request and status documents, and the
//! per-statement state the service keeps between requests.

use std::sync::Mutex;

use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tokio::task::AbortHandle;
use tokio::time::Instant;

use arneb_common::identifiers::QueryId;

use super::error::{AgentError, AgentErrorCode};
use super::limits::AgentLimits;
use super::results::{page, ResultPage, Retained};

/// Body of a statement submission.
#[derive(Debug, Clone, Deserialize)]
pub struct SubmitRequest {
    /// The SQL statement.
    pub sql: String,
    /// Default catalog for unqualified names.
    #[serde(default)]
    pub catalog: Option<String>,
    /// Default schema for unqualified names.
    #[serde(default)]
    pub schema: Option<String>,
    /// Rows to retain for the client.
    #[serde(default)]
    pub max_rows: Option<usize>,
}

/// Lifecycle state of a statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum StatementState {
    /// Waiting for admission.
    Queued,
    /// Executing.
    Running,
    /// Completed successfully.
    Finished,
    /// Completed with an error.
    Failed,
    /// Cancelled by the client or as abandoned.
    Cancelled,
}

impl StatementState {
    /// Whether the statement has stopped executing.
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Finished | Self::Failed | Self::Cancelled)
    }
}

/// A result column.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Column {
    /// Column name.
    pub name: String,
    /// SQL type, as Trino spells it (`bigint`, `varchar`, `decimal(12,2)`, …).
    #[serde(rename = "type")]
    pub type_name: String,
}

/// What `submit`, `status` and `cancel` report about a statement.
#[derive(Debug, Clone, Serialize)]
pub struct StatementStatus {
    /// Statement id (the query tracker's query id).
    pub id: String,
    /// Lifecycle state.
    pub state: StatementState,
    /// Position in the admission queue when the statement was queued.
    pub queue_position: Option<usize>,
    /// Time since submission (until completion, once terminal).
    pub elapsed_ms: u64,
    /// Kind of a statement without a result set (e.g. `CREATE TABLE`).
    pub update_type: Option<String>,
    /// Result columns, once finished.
    pub columns: Option<Vec<Column>>,
    /// Rows the statement produced, once finished.
    pub total_rows: Option<u64>,
    /// Whether rows beyond the retained ones were dropped.
    pub truncated: bool,
    /// Rows kept for the client at most (the request's `max_rows`).
    pub max_rows: usize,
    /// The first page of rows, once finished.
    pub first_page: Option<ResultPage>,
    /// Why the statement failed or was cancelled.
    pub error: Option<AgentError>,
}

/// What a finished statement keeps: its shape and the retained rows.
pub(crate) struct FinishedOutput {
    pub(crate) columns: Option<Vec<Column>>,
    pub(crate) update_type: Option<String>,
    pub(crate) retained: Retained,
}

pub(crate) enum Phase {
    /// Waiting for admission; the position once it has been queued.
    Queued(Option<usize>),
    Running,
    Finished(FinishedOutput),
    Failed(AgentError),
    Cancelled(AgentError),
}

impl Phase {
    pub(crate) fn state(&self) -> StatementState {
        match self {
            Self::Queued(_) => StatementState::Queued,
            Self::Running => StatementState::Running,
            Self::Finished(_) => StatementState::Finished,
            Self::Failed(_) => StatementState::Failed,
            Self::Cancelled(_) => StatementState::Cancelled,
        }
    }
}

pub(crate) struct Inner {
    pub(crate) phase: Phase,
    pub(crate) last_access: Instant,
    pub(crate) ended: Option<Instant>,
}

/// A statement the service tracks between requests.
pub(crate) struct Statement {
    pub(crate) id: String,
    pub(crate) tracker_id: QueryId,
    pub(crate) user: String,
    pub(crate) sql: String,
    /// Rows kept after completion (validated against the ceiling).
    pub(crate) max_rows: usize,
    pub(crate) created: Instant,
    pub(crate) inner: Mutex<Inner>,
    /// Flipped to `true` once the statement is terminal.
    pub(crate) done: watch::Sender<bool>,
    pub(crate) abort: Mutex<Option<AbortHandle>>,
}

impl Statement {
    pub(crate) fn new(tracker_id: QueryId, user: &str, sql: String, max_rows: usize) -> Self {
        let now = Instant::now();
        Self {
            id: tracker_id.to_string(),
            tracker_id,
            user: user.to_string(),
            sql,
            max_rows,
            created: now,
            inner: Mutex::new(Inner {
                phase: Phase::Queued(None),
                last_access: now,
                ended: None,
            }),
            done: watch::channel(false).0,
            abort: Mutex::new(None),
        }
    }

    pub(crate) fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub(crate) fn is_terminal(&self) -> bool {
        self.lock().phase.state().is_terminal()
    }

    /// Time since the last client request.
    pub(crate) fn idle(&self) -> std::time::Duration {
        self.lock().last_access.elapsed()
    }

    /// Records a client request against the statement.
    pub(crate) fn touch(&self) {
        self.lock().last_access = Instant::now();
    }

    /// Moves a statement that is still executing to `phase` (terminal).
    /// Returns `false` when it had already ended.
    pub(crate) fn end(&self, phase: Phase) -> bool {
        {
            let mut inner = self.lock();
            if inner.phase.state().is_terminal() {
                return false;
            }
            inner.phase = phase;
            inner.ended = Some(Instant::now());
        }
        let _ = self.done.send(true);
        true
    }

    /// The status document.
    pub(crate) fn status(&self, limits: &AgentLimits) -> StatementStatus {
        let inner = self.lock();
        let elapsed = inner
            .ended
            .unwrap_or_else(Instant::now)
            .duration_since(self.created);
        let mut status = StatementStatus {
            id: self.id.clone(),
            state: inner.phase.state(),
            queue_position: None,
            elapsed_ms: elapsed.as_millis() as u64,
            update_type: None,
            columns: None,
            total_rows: None,
            truncated: false,
            max_rows: self.max_rows,
            first_page: None,
            error: None,
        };
        match &inner.phase {
            Phase::Queued(position) => status.queue_position = *position,
            Phase::Running => {}
            Phase::Finished(output) => {
                status.update_type = output.update_type.clone();
                status.columns = output.columns.clone();
                status.total_rows = Some(output.retained.total_rows);
                status.truncated = output.retained.truncated;
                status.first_page = Some(page(
                    &output.retained,
                    0,
                    limits.default_page_bytes,
                    limits.max_cell_bytes,
                ));
            }
            Phase::Failed(err) | Phase::Cancelled(err) => status.error = Some(err.clone()),
        }
        status
    }
}

impl Statement {
    /// The page of retained rows starting at row `cursor`.
    pub(crate) fn page(
        &self,
        cursor: u64,
        max_bytes: usize,
        max_cell_bytes: usize,
    ) -> Result<ResultPage, AgentError> {
        let inner = self.lock();
        let Phase::Finished(output) = &inner.phase else {
            return Err(AgentError::new(
                AgentErrorCode::InvalidArgument,
                format!("statement is {:?}, not finished", inner.phase.state()),
            )
            .with_hint("poll the statement until its state is finished"));
        };
        let kept = output.retained.kept_rows();
        if cursor > kept {
            return Err(AgentError::new(
                AgentErrorCode::InvalidArgument,
                format!("cursor {cursor} is past the end of the {kept} retained rows"),
            )
            .with_hint(format!("cursor must be in 0..={kept}")));
        }
        Ok(page(&output.retained, cursor, max_bytes, max_cell_bytes))
    }
}
