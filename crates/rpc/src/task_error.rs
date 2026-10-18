//! Keeping a remote task's error class across the Flight wire.
//!
//! A memory-limit failure on a worker must reach the client as one (pgwire
//! `53200`, Trino `EXCEEDED_LOCAL_MEMORY_LIMIT`), not as a generic internal
//! error. The class travels structurally, never in the error text:
//!
//! - the worker records a [`TaskFailure`] (message + class) from the typed
//!   error, in the output buffer's failure flag or the task status;
//! - the Flight server answers it with gRPC `RESOURCE_EXHAUSTED` (else
//!   `INTERNAL`), on `do_get` and as the final frame of a stream;
//! - the consumer's [`crate::ExchangeClient`] maps that status code back to
//!   `ExecutionError::ResourceExhausted` ([`remote_error`]);
//! - operators re-raising it keep the variant
//!   (`ExecutionError::upstream`), and the next hop starts over.

use std::fmt;

use arneb_common::error::{ArnebError, ExecutionError};
use arrow_flight::error::FlightError;
use tonic::{Code, Status};

/// Why a task failed, as it travels from the worker to its consumers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskFailure {
    message: String,
    resource_exhausted: bool,
}

impl TaskFailure {
    /// A failure that is not a memory-limit failure.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            resource_exhausted: false,
        }
    }

    /// A memory-limit failure; `message` is what the client will see.
    pub fn resource_exhausted(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            resource_exhausted: true,
        }
    }

    /// Describe `err` for the wire: `context: err`, or the memory-limit
    /// message itself when `err` is one.
    pub fn from_error(context: &str, err: &ArnebError) -> Self {
        match err {
            ArnebError::Execution(e) => Self::from_execution_error(context, e),
            other => Self::new(format!("{context}: {other}")),
        }
    }

    /// [`Self::from_error`] for an `ExecutionError`.
    pub fn from_execution_error(context: &str, err: &ExecutionError) -> Self {
        match err {
            ExecutionError::ResourceExhausted(msg) => Self::resource_exhausted(msg.clone()),
            other => Self::new(format!("{context}: {other}")),
        }
    }

    /// The failure's message.
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Whether this is a memory-limit failure.
    pub fn is_resource_exhausted(&self) -> bool {
        self.resource_exhausted
    }

    /// The status a consumer gets for this failure: `RESOURCE_EXHAUSTED`
    /// with the message unchanged, or `INTERNAL` with `context: message`.
    pub(crate) fn to_status(&self, context: impl fmt::Display) -> Status {
        if self.resource_exhausted {
            Status::resource_exhausted(self.message.clone())
        } else {
            Status::internal(format!("{context}: {}", self.message))
        }
    }
}

impl fmt::Display for TaskFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl From<String> for TaskFailure {
    fn from(message: String) -> Self {
        Self::new(message)
    }
}

impl From<&str> for TaskFailure {
    fn from(message: &str) -> Self {
        Self::new(message)
    }
}

/// A failure a consumer saw on the other side of an exchange.
pub(crate) trait RemoteFailure {
    /// The gRPC status, when there is one.
    fn status(&self) -> Option<&Status>;
    /// The text for a non-memory failure's message.
    fn describe(&self) -> String;
}

impl RemoteFailure for Status {
    fn status(&self) -> Option<&Status> {
        Some(self)
    }

    fn describe(&self) -> String {
        self.message().to_owned()
    }
}

impl RemoteFailure for FlightError {
    fn status(&self) -> Option<&Status> {
        match self {
            FlightError::Tonic(status) => Some(status),
            _ => None,
        }
    }

    fn describe(&self) -> String {
        self.to_string()
    }
}

/// Error for a remote failure seen by the consumer: `ResourceExhausted`
/// (with the worker's message) when the status code says so, else
/// `InvalidOperation("{context}: …")`.
pub(crate) fn remote_error(context: String, failure: &impl RemoteFailure) -> ExecutionError {
    match failure.status() {
        Some(status) if status.code() == Code::ResourceExhausted => {
            ExecutionError::ResourceExhausted(status.message().to_owned())
        }
        _ => ExecutionError::InvalidOperation(format!("{context}: {}", failure.describe())),
    }
}

/// Status for a server-side stream error: a status already chosen for a
/// task failure passes through; anything else is `INTERNAL`.
pub(crate) fn stream_error_status(err: FlightError) -> Status {
    match err {
        FlightError::Tonic(status) => *status,
        other => Status::internal(format!("flight encoding error: {other}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arneb_common::error::{is_resource_exhausted, PlanError};

    #[test]
    fn resource_exhausted_round_trips() {
        let err = ArnebError::Execution(ExecutionError::ResourceExhausted(
            "query memory limit exceeded".into(),
        ));
        let failure = TaskFailure::from_error("stream error", &err);
        assert!(failure.is_resource_exhausted());
        let status = failure.to_status("task 't' failed");
        assert_eq!(status.code(), Code::ResourceExhausted);
        let back = remote_error("remote task t".into(), &status);
        assert!(matches!(back, ExecutionError::ResourceExhausted(_)));
        assert_eq!(
            back.to_string(),
            "resource exhausted: query memory limit exceeded"
        );

        let mid_stream = FlightError::Tonic(Box::new(failure.to_status("producer task failed")));
        assert!(matches!(
            remote_error("Flight stream error".into(), &mid_stream),
            ExecutionError::ResourceExhausted(_)
        ));
    }

    /// An operator re-raises the remote ResourceExhausted; the next hop
    /// must still see it as one.
    #[test]
    fn class_survives_operator_rewrapping_over_two_hops() {
        let first = remote_error(
            "Flight stream error".into(),
            &Status::resource_exhausted("query memory limit exceeded"),
        );
        let rewrapped =
            ArnebError::Execution(ExecutionError::upstream("aggregate input stream", first));
        let failure = TaskFailure::from_error("execute(0) failed", &rewrapped);
        let status = failure.to_status("producer task failed");
        assert_eq!(status.code(), Code::ResourceExhausted);
        let top = ArnebError::Execution(ExecutionError::upstream(
            "TopKExec.input",
            remote_error("Flight stream error".into(), &status),
        ));
        assert!(is_resource_exhausted(&top));
        assert!(top.to_string().contains("query memory limit exceeded"));
    }

    #[test]
    fn other_errors_stay_internal() {
        let err = ArnebError::Execution(ExecutionError::InvalidOperation("boom".into()));
        let failure = TaskFailure::from_error("stream error", &err);
        assert!(!failure.is_resource_exhausted());
        let status = failure.to_status("task 't' failed");
        assert_eq!(status.code(), Code::Internal);
        assert!(matches!(
            remote_error("ctx".into(), &status),
            ExecutionError::InvalidOperation(_)
        ));
    }

    /// #145: the class comes from the status code alone. A message that
    /// spells the old wire tag (here a table name) stays an internal
    /// failure, on every hop.
    #[test]
    fn text_never_decides_the_class() {
        let err = ArnebError::Plan(PlanError::TableNotFound("[resource exhausted] x".into()));
        let failure = TaskFailure::from_error("execute(0) failed", &err);
        assert!(!failure.is_resource_exhausted());
        let status = failure.to_status("task 't' failed");
        assert_eq!(status.code(), Code::Internal);
        let back = remote_error("remote task t".into(), &status);
        assert!(matches!(back, ExecutionError::InvalidOperation(_)));
        let flight = FlightError::ProtocolError("[resource exhausted] x".into());
        assert!(matches!(
            remote_error("Flight stream error".into(), &flight),
            ExecutionError::InvalidOperation(_)
        ));
    }
}
