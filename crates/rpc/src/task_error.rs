//! Keeping a remote task's error class across the Flight wire.
//!
//! Worker task failures travel as strings: the output buffer's failure
//! flag, the task-status callback, and a mid-stream `FlightError`. A
//! memory-limit failure must still reach the client as
//! `ExecutionError::ResourceExhausted` (pgwire `53200`, Trino
//! `EXCEEDED_LOCAL_MEMORY_LIMIT`), not as a generic internal error. The
//! worker tags such messages with [`RESOURCE_EXHAUSTED_TAG`]; the Flight
//! server answers a tagged failure with `RESOURCE_EXHAUSTED`, and
//! [`crate::ExchangeClient`] turns a tagged status or stream error back into
//! `ResourceExhausted` with the tag removed.

use arneb_common::error::{ArnebError, ExecutionError};
use tonic::Status;

/// Marker prefixed to a task failure caused by a memory limit.
pub const RESOURCE_EXHAUSTED_TAG: &str = "[resource exhausted] ";

/// Describe a task failure for the wire: `context: error`, tagged when the
/// error is a memory-limit failure.
pub fn describe_task_failure(context: &str, err: &ArnebError) -> String {
    match err {
        ArnebError::Execution(e @ ExecutionError::ResourceExhausted(_)) => {
            describe_execution_failure(context, e)
        }
        other => format!("{context}: {other}"),
    }
}

/// [`describe_task_failure`] for an `ExecutionError`.
pub fn describe_execution_failure(context: &str, err: &ExecutionError) -> String {
    match err {
        ExecutionError::ResourceExhausted(msg) => {
            format!("{RESOURCE_EXHAUSTED_TAG}{context}: {}", strip_tag(msg))
        }
        other => format!("{context}: {other}"),
    }
}

/// Tag a message that already describes a memory-limit failure.
pub fn tag_resource_exhausted(msg: impl AsRef<str>) -> String {
    format!("{RESOURCE_EXHAUSTED_TAG}{}", strip_tag(msg.as_ref()))
}

fn strip_tag(msg: &str) -> &str {
    match msg.find(RESOURCE_EXHAUSTED_TAG) {
        Some(i) => &msg[i + RESOURCE_EXHAUSTED_TAG.len()..],
        None => msg,
    }
}

/// Status for a failed task: `RESOURCE_EXHAUSTED` when tagged, else
/// `INTERNAL`.
pub(crate) fn failure_status(msg: String) -> Status {
    if msg.contains(RESOURCE_EXHAUSTED_TAG) {
        Status::resource_exhausted(msg)
    } else {
        Status::internal(msg)
    }
}

/// Error for a remote failure seen by the consumer: `ResourceExhausted`
/// (tag removed, `context` kept) when the message is tagged, else
/// `InvalidOperation`.
pub(crate) fn remote_error(context: String, msg: &str) -> ExecutionError {
    if msg.contains(RESOURCE_EXHAUSTED_TAG) {
        ExecutionError::ResourceExhausted(format!("{context}: {}", strip_tag(msg)))
    } else {
        ExecutionError::InvalidOperation(format!("{context}: {msg}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_exhausted_round_trips() {
        let err = ArnebError::Execution(ExecutionError::ResourceExhausted(
            "query memory limit exceeded".into(),
        ));
        let wire = describe_task_failure("stream error", &err);
        assert!(wire.starts_with(RESOURCE_EXHAUSTED_TAG));
        let status = failure_status(format!("task 't' failed: {wire}"));
        assert_eq!(status.code(), tonic::Code::ResourceExhausted);
        match remote_error("remote task t".into(), status.message()) {
            ExecutionError::ResourceExhausted(m) => {
                assert!(!m.contains(RESOURCE_EXHAUSTED_TAG), "{m}");
                assert!(m.contains("query memory limit exceeded"), "{m}");
            }
            other => panic!("expected ResourceExhausted, got {other:?}"),
        }
    }

    #[test]
    fn other_errors_stay_internal() {
        let err = ArnebError::Execution(ExecutionError::InvalidOperation("boom".into()));
        let wire = describe_task_failure("stream error", &err);
        assert_eq!(failure_status(wire.clone()).code(), tonic::Code::Internal);
        assert!(matches!(
            remote_error("ctx".into(), &wire),
            ExecutionError::InvalidOperation(_)
        ));
    }

    #[test]
    fn retagging_does_not_nest() {
        let once = tag_resource_exhausted("limit");
        let twice = tag_resource_exhausted(&once);
        assert_eq!(once, twice);
    }
}
