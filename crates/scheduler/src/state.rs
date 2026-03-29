//! State machines for query, stage, and task lifecycle.

use std::fmt;
use std::time::Instant;

use trino_common::error::ExecutionError;
use trino_common::identifiers::QueryId;

// ===========================================================================
// QueryState
// ===========================================================================

/// Lifecycle states for a query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryState {
    Queued,
    Planning,
    Starting,
    Running,
    Finishing,
    Finished,
    Failed,
    Cancelled,
}

impl fmt::Display for QueryState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Queued => write!(f, "QUEUED"),
            Self::Planning => write!(f, "PLANNING"),
            Self::Starting => write!(f, "STARTING"),
            Self::Running => write!(f, "RUNNING"),
            Self::Finishing => write!(f, "FINISHING"),
            Self::Finished => write!(f, "FINISHED"),
            Self::Failed => write!(f, "FAILED"),
            Self::Cancelled => write!(f, "CANCELLED"),
        }
    }
}

impl QueryState {
    /// Whether this state is terminal (no further transitions possible).
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Finished | Self::Failed | Self::Cancelled)
    }

    fn can_transition_to(self, next: Self) -> bool {
        if self.is_terminal() {
            return false;
        }
        matches!(
            (self, next),
            (Self::Queued, Self::Planning)
                | (Self::Planning, Self::Starting)
                | (Self::Starting, Self::Running)
                | (Self::Running, Self::Finishing)
                | (Self::Finishing, Self::Finished)
                // Failure/cancellation from any active state.
                | (Self::Queued, Self::Failed)
                | (Self::Planning, Self::Failed)
                | (Self::Starting, Self::Failed)
                | (Self::Running, Self::Failed)
                | (Self::Finishing, Self::Failed)
                | (Self::Queued, Self::Cancelled)
                | (Self::Planning, Self::Cancelled)
                | (Self::Starting, Self::Cancelled)
                | (Self::Running, Self::Cancelled)
                | (Self::Finishing, Self::Cancelled)
        )
    }
}

/// Tracks the lifecycle of a query.
#[derive(Debug)]
pub struct QueryStateMachine {
    pub query_id: QueryId,
    pub state: QueryState,
    pub sql: String,
    pub created_at: Instant,
    pub error: Option<String>,
}

impl QueryStateMachine {
    /// Creates a new query in the QUEUED state.
    pub fn new(query_id: QueryId, sql: String) -> Self {
        Self {
            query_id,
            state: QueryState::Queued,
            sql,
            created_at: Instant::now(),
            error: None,
        }
    }

    /// Transition to the next state. Returns error if transition is invalid.
    pub fn transition(&mut self, next: QueryState) -> Result<(), ExecutionError> {
        if !self.state.can_transition_to(next) {
            return Err(ExecutionError::InvalidOperation(format!(
                "invalid query state transition: {} → {}",
                self.state, next
            )));
        }
        self.state = next;
        Ok(())
    }

    /// Transition to FAILED with an error message.
    pub fn fail(&mut self, error: String) -> Result<(), ExecutionError> {
        self.error = Some(error);
        self.transition(QueryState::Failed)
    }

    /// Transition to CANCELLED.
    pub fn cancel(&mut self) -> Result<(), ExecutionError> {
        self.transition(QueryState::Cancelled)
    }
}

// ===========================================================================
// StageState
// ===========================================================================

/// Lifecycle states for a stage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StageState {
    Planned,
    Scheduling,
    Running,
    Flushing,
    Finished,
    Failed,
    Cancelled,
}

impl fmt::Display for StageState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Planned => write!(f, "PLANNED"),
            Self::Scheduling => write!(f, "SCHEDULING"),
            Self::Running => write!(f, "RUNNING"),
            Self::Flushing => write!(f, "FLUSHING"),
            Self::Finished => write!(f, "FINISHED"),
            Self::Failed => write!(f, "FAILED"),
            Self::Cancelled => write!(f, "CANCELLED"),
        }
    }
}

impl StageState {
    fn is_terminal(self) -> bool {
        matches!(self, Self::Finished | Self::Failed | Self::Cancelled)
    }

    fn can_transition_to(self, next: Self) -> bool {
        if self.is_terminal() {
            return false;
        }
        matches!(
            (self, next),
            (Self::Planned, Self::Scheduling)
                | (Self::Scheduling, Self::Running)
                | (Self::Running, Self::Flushing)
                | (Self::Flushing, Self::Finished)
                | (_, Self::Failed)
                | (_, Self::Cancelled)
        ) && !self.is_terminal()
    }
}

/// Tracks the lifecycle of a stage.
#[derive(Debug)]
pub struct StageStateMachine {
    pub state: StageState,
    pub error: Option<String>,
}

impl StageStateMachine {
    pub fn new() -> Self {
        Self {
            state: StageState::Planned,
            error: None,
        }
    }

    pub fn transition(&mut self, next: StageState) -> Result<(), ExecutionError> {
        if !self.state.can_transition_to(next) {
            return Err(ExecutionError::InvalidOperation(format!(
                "invalid stage state transition: {} → {}",
                self.state, next
            )));
        }
        self.state = next;
        Ok(())
    }

    pub fn fail(&mut self, error: String) -> Result<(), ExecutionError> {
        self.error = Some(error);
        self.transition(StageState::Failed)
    }
}

impl Default for StageStateMachine {
    fn default() -> Self {
        Self::new()
    }
}

// ===========================================================================
// TaskState
// ===========================================================================

/// Lifecycle states for a task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskState {
    Planned,
    Running,
    Flushing,
    Finished,
    Failed,
    Cancelled,
}

impl fmt::Display for TaskState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Planned => write!(f, "PLANNED"),
            Self::Running => write!(f, "RUNNING"),
            Self::Flushing => write!(f, "FLUSHING"),
            Self::Finished => write!(f, "FINISHED"),
            Self::Failed => write!(f, "FAILED"),
            Self::Cancelled => write!(f, "CANCELLED"),
        }
    }
}

impl TaskState {
    fn is_terminal(self) -> bool {
        matches!(self, Self::Finished | Self::Failed | Self::Cancelled)
    }

    fn can_transition_to(self, next: Self) -> bool {
        if self.is_terminal() {
            return false;
        }
        matches!(
            (self, next),
            (Self::Planned, Self::Running)
                | (Self::Running, Self::Flushing)
                | (Self::Flushing, Self::Finished)
                | (_, Self::Failed)
                | (_, Self::Cancelled)
        ) && !self.is_terminal()
    }
}

/// Tracks the lifecycle of a task.
#[derive(Debug)]
pub struct TaskStateMachine {
    pub state: TaskState,
    pub error: Option<String>,
}

impl TaskStateMachine {
    pub fn new() -> Self {
        Self {
            state: TaskState::Planned,
            error: None,
        }
    }

    pub fn transition(&mut self, next: TaskState) -> Result<(), ExecutionError> {
        if !self.state.can_transition_to(next) {
            return Err(ExecutionError::InvalidOperation(format!(
                "invalid task state transition: {} → {}",
                self.state, next
            )));
        }
        self.state = next;
        Ok(())
    }

    pub fn fail(&mut self, error: String) -> Result<(), ExecutionError> {
        self.error = Some(error);
        self.transition(TaskState::Failed)
    }
}

impl Default for TaskStateMachine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_lifecycle_happy_path() {
        let mut qsm = QueryStateMachine::new(QueryId::new(), "SELECT 1".into());
        assert_eq!(qsm.state, QueryState::Queued);
        qsm.transition(QueryState::Planning).unwrap();
        qsm.transition(QueryState::Starting).unwrap();
        qsm.transition(QueryState::Running).unwrap();
        qsm.transition(QueryState::Finishing).unwrap();
        qsm.transition(QueryState::Finished).unwrap();
        assert_eq!(qsm.state, QueryState::Finished);
    }

    #[test]
    fn query_invalid_transition() {
        let mut qsm = QueryStateMachine::new(QueryId::new(), "SELECT 1".into());
        assert!(qsm.transition(QueryState::Running).is_err());
    }

    #[test]
    fn query_cannot_transition_from_terminal() {
        let mut qsm = QueryStateMachine::new(QueryId::new(), "SELECT 1".into());
        qsm.transition(QueryState::Planning).unwrap();
        qsm.fail("oops".into()).unwrap();
        assert_eq!(qsm.state, QueryState::Failed);
        assert!(qsm.transition(QueryState::Running).is_err());
    }

    #[test]
    fn query_cancel() {
        let mut qsm = QueryStateMachine::new(QueryId::new(), "SELECT 1".into());
        qsm.transition(QueryState::Planning).unwrap();
        qsm.transition(QueryState::Starting).unwrap();
        qsm.transition(QueryState::Running).unwrap();
        qsm.cancel().unwrap();
        assert_eq!(qsm.state, QueryState::Cancelled);
    }

    #[test]
    fn stage_lifecycle_happy_path() {
        let mut ssm = StageStateMachine::new();
        ssm.transition(StageState::Scheduling).unwrap();
        ssm.transition(StageState::Running).unwrap();
        ssm.transition(StageState::Flushing).unwrap();
        ssm.transition(StageState::Finished).unwrap();
        assert_eq!(ssm.state, StageState::Finished);
    }

    #[test]
    fn stage_invalid_transition() {
        let mut ssm = StageStateMachine::new();
        assert!(ssm.transition(StageState::Running).is_err());
    }

    #[test]
    fn task_lifecycle_happy_path() {
        let mut tsm = TaskStateMachine::new();
        tsm.transition(TaskState::Running).unwrap();
        tsm.transition(TaskState::Flushing).unwrap();
        tsm.transition(TaskState::Finished).unwrap();
        assert_eq!(tsm.state, TaskState::Finished);
    }

    #[test]
    fn task_fail_from_running() {
        let mut tsm = TaskStateMachine::new();
        tsm.transition(TaskState::Running).unwrap();
        tsm.fail("disk full".into()).unwrap();
        assert_eq!(tsm.state, TaskState::Failed);
        assert_eq!(tsm.error.as_deref(), Some("disk full"));
    }
}
