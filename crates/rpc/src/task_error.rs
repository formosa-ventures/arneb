//! Keeping a remote task's error class across the Flight wire.
//!
//! Worker task failures travel as strings: the output buffer's failure
//! flag, the task-status callback, and a mid-stream `FlightError`. A
//! memory-limit failure must still reach the client as one (pgwire `53200`,
//! Trino `EXCEEDED_LOCAL_MEMORY_LIMIT`), not as a generic internal error.
//!
//! The worker tags such messages with [`RESOURCE_EXHAUSTED_TAG`]. The tag
//! stays in the text on every hop: the Flight server answers a tagged
//! failure with `RESOURCE_EXHAUSTED`, [`crate::ExchangeClient`] turns a
//! tagged status or stream error into `ExecutionError::ResourceExhausted`,
//! and when an operator further up re-wraps that error in its own message
//! the tag is still there for the next hop. The client-facing edge
//! classifies with `arneb_common::error::is_resource_exhausted` and strips
//! it with `resource_exhausted_message`.

pub use arneb_common::error::RESOURCE_EXHAUSTED_TAG;
use arneb_common::error::{ArnebError, ExecutionError};
use tonic::Status;

/// Describe a task failure for the wire: `context: error`, tagged when the
/// error is (or wraps) a memory-limit failure.
pub fn describe_task_failure(context: &str, err: &ArnebError) -> String {
    match err {
        ArnebError::Execution(e) => describe_execution_failure(context, e),
        other => format!("{context}: {other}"),
    }
}

/// [`describe_task_failure`] for an `ExecutionError`.
pub fn describe_execution_failure(context: &str, err: &ExecutionError) -> String {
    let text = format!("{context}: {err}");
    match err {
        ExecutionError::ResourceExhausted(_) if !text.contains(RESOURCE_EXHAUSTED_TAG) => {
            format!("{RESOURCE_EXHAUSTED_TAG}{text}")
        }
        _ => text,
    }
}

/// Tag a message that describes a memory-limit failure (idempotent).
pub fn tag_resource_exhausted(msg: impl AsRef<str>) -> String {
    let msg = msg.as_ref();
    if msg.contains(RESOURCE_EXHAUSTED_TAG) {
        msg.to_owned()
    } else {
        format!("{RESOURCE_EXHAUSTED_TAG}{msg}")
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
/// (tag kept for later hops) when the message is tagged, else
/// `InvalidOperation`.
pub(crate) fn remote_error(context: String, msg: &str) -> ExecutionError {
    if msg.contains(RESOURCE_EXHAUSTED_TAG) {
        ExecutionError::ResourceExhausted(format!("{context}: {msg}"))
    } else {
        ExecutionError::InvalidOperation(format!("{context}: {msg}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arneb_common::error::{is_resource_exhausted, resource_exhausted_message};

    #[test]
    fn resource_exhausted_round_trips() {
        let err = ArnebError::Execution(ExecutionError::ResourceExhausted(
            "query memory limit exceeded".into(),
        ));
        let wire = describe_task_failure("stream error", &err);
        assert!(wire.starts_with(RESOURCE_EXHAUSTED_TAG));
        let status = failure_status(format!("task 't' failed: {wire}"));
        assert_eq!(status.code(), tonic::Code::ResourceExhausted);
        let back = remote_error("remote task t".into(), status.message());
        assert!(matches!(back, ExecutionError::ResourceExhausted(_)));
        let client = resource_exhausted_message(&ArnebError::Execution(back));
        assert!(client.contains("query memory limit exceeded"), "{client}");
        assert!(!client.contains(RESOURCE_EXHAUSTED_TAG), "{client}");
    }

    /// Tier 2 regression: an operator re-wraps the remote ResourceExhausted
    /// as InvalidOperation; the next hop must still see it as one.
    #[test]
    fn class_survives_operator_rewrapping_over_two_hops() {
        let first = remote_error(
            "Flight stream error".into(),
            &tag_resource_exhausted("stream error during execution: query memory limit exceeded"),
        );
        let rewrapped = ArnebError::Execution(ExecutionError::InvalidOperation(format!(
            "aggregate input stream: {first}"
        )));
        let wire = describe_task_failure("execute(0) failed", &rewrapped);
        assert_eq!(
            failure_status(wire.clone()).code(),
            tonic::Code::ResourceExhausted
        );
        let top = ArnebError::Execution(ExecutionError::InvalidOperation(format!(
            "TopKExec.input: {}",
            remote_error("Flight stream error".into(), &wire)
        )));
        assert!(is_resource_exhausted(&top));
        assert!(resource_exhausted_message(&top).contains("query memory limit exceeded"));
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
    fn tagging_does_not_nest() {
        let once = tag_resource_exhausted("limit");
        assert_eq!(tag_resource_exhausted(&once), once);
    }
}
