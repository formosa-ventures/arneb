//! Keeping a remote task's error class across the Flight wire.
//!
//! A memory-limit failure on a worker must reach the client as one (pgwire
//! `53200`, Trino `EXCEEDED_LOCAL_MEMORY_LIMIT`), not as a generic internal
//! error. The class travels structurally, never in the error text:
//!
//! - the worker records a [`TaskFailure`] (message + class) from the typed
//!   error, in the output buffer's failure flag or the task status;
//! - the Flight server answers it with gRPC `RESOURCE_EXHAUSTED` (else
//!   `INTERNAL`), on `do_get` and as the final frame of a stream, marked
//!   with [`CLASS_KEY`] metadata;
//! - the consumer's [`crate::ExchangeClient`] maps that status back to
//!   `ExecutionError::ResourceExhausted` ([`remote_error`]). The code alone
//!   is not enough: tonic also reports an h2 `ENHANCE_YOUR_CALM` GOAWAY
//!   (rapid-reset / too-many-pings defence) as `RESOURCE_EXHAUSTED`, and
//!   that transport failure is not a memory limit;
//! - operators re-raising it keep the variant
//!   (`ExecutionError::upstream`), and the next hop starts over.
//!
//! A data error (`ExecutionError::Data`: a failed cast, an overflow, a
//! division by zero) travels the same way so the client gets its SQLSTATE:
//! gRPC `INTERNAL`, as for any task failure, with its
//! [`DataErrorKind::tag`] under [`CLASS_KEY`] and its message unwrapped. A
//! consumer older than the marker sees an ordinary task failure.

use std::fmt;

use arneb_common::error::{ArnebError, DataErrorKind, ExecutionError};
use arrow_flight::error::FlightError;
use tonic::metadata::MetadataValue;
use tonic::{Code, Status};

/// Status metadata naming a task failure's class. Only statuses built by
/// [`TaskFailure::to_status`] carry it.
const CLASS_KEY: &str = "x-arneb-error-class";
/// [`CLASS_KEY`] value of a memory-limit failure.
const CLASS_RESOURCE_EXHAUSTED: &str = "resource-exhausted";

/// Why a task failed, as it travels from the worker to its consumers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskFailure {
    message: String,
    class: FailureClass,
}

/// The part of a task failure's type the consumer restores.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FailureClass {
    Other,
    ResourceExhausted,
    Data(DataErrorKind),
}

impl TaskFailure {
    /// A failure that is not a memory-limit failure.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            class: FailureClass::Other,
        }
    }

    /// A memory-limit failure; `message` is what the client will see.
    pub fn resource_exhausted(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            class: FailureClass::ResourceExhausted,
        }
    }

    /// A data error of `kind`; `message` is what the client will see.
    pub fn data(kind: DataErrorKind, message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            class: FailureClass::Data(kind),
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
            ExecutionError::Data { kind, message } => Self::data(*kind, message.clone()),
            other => Self::new(format!("{context}: {other}")),
        }
    }

    /// The failure's message.
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Whether this is a memory-limit failure.
    pub fn is_resource_exhausted(&self) -> bool {
        self.class == FailureClass::ResourceExhausted
    }

    /// The status a consumer gets for this failure: `RESOURCE_EXHAUSTED`
    /// with the message unchanged, `INTERNAL` marked with the data error's
    /// kind and the message unchanged, or `INTERNAL` with `context: message`.
    pub(crate) fn to_status(&self, context: impl fmt::Display) -> Status {
        let (mut status, class) = match self.class {
            FailureClass::ResourceExhausted => (
                Status::resource_exhausted(self.message.clone()),
                CLASS_RESOURCE_EXHAUSTED,
            ),
            FailureClass::Data(kind) => (Status::internal(self.message.clone()), kind.tag()),
            FailureClass::Other => {
                return Status::internal(format!("{context}: {}", self.message));
            }
        };
        status
            .metadata_mut()
            .insert(CLASS_KEY, MetadataValue::from_static(class));
        status
    }
}

impl TaskFailure {
    /// The error a coordinator raises for this failure: `ResourceExhausted`
    /// with the worker's message for a memory-limit failure, `Data` with
    /// its kind for a data error, else `InvalidOperation("{context}: message")`.
    pub fn into_execution_error(self, context: impl fmt::Display) -> ExecutionError {
        match self.class {
            FailureClass::ResourceExhausted => ExecutionError::ResourceExhausted(self.message),
            FailureClass::Data(kind) => ExecutionError::Data {
                kind,
                message: self.message,
            },
            FailureClass::Other => {
                ExecutionError::InvalidOperation(format!("{context}: {}", self.message))
            }
        }
    }
}

/// Worker → coordinator (`report_task_failure`): a task of a distributed
/// query failed, so the coordinator can fail the query at once with this
/// error instead of waiting until a consumer stumbles on it.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TaskFailureReport {
    /// The query's id, as the coordinator dispatched it.
    pub query_id: arneb_common::QueryId,
    /// `query_id.stage.partition`, for the log.
    pub task_id: String,
    /// The failure's message.
    pub message: String,
    /// Whether it is a memory or spill limit failure (`ResourceExhausted`).
    #[serde(default)]
    pub resource_exhausted: bool,
    /// A data error's [`DataErrorKind::tag`], so the client gets its
    /// SQLSTATE. Absent from an older worker, whose report then reads as
    /// an ordinary failure.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_kind: Option<String>,
}

impl TaskFailureReport {
    /// A report of `failure` for task `task_id` of `query_id`.
    pub fn new(
        query_id: arneb_common::QueryId,
        task_id: impl Into<String>,
        failure: &TaskFailure,
    ) -> Self {
        Self {
            query_id,
            task_id: task_id.into(),
            message: failure.message.clone(),
            resource_exhausted: failure.is_resource_exhausted(),
            data_kind: match failure.class {
                FailureClass::Data(kind) => Some(kind.tag().to_string()),
                _ => None,
            },
        }
    }

    /// The reported failure.
    pub fn failure(&self) -> TaskFailure {
        let data = self.data_kind.as_deref().and_then(DataErrorKind::from_tag);
        TaskFailure {
            message: self.message.clone(),
            class: match (self.resource_exhausted, data) {
                (true, _) => FailureClass::ResourceExhausted,
                (false, Some(kind)) => FailureClass::Data(kind),
                (false, None) => FailureClass::Other,
            },
        }
    }

    /// Serialize to JSON.
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("TaskFailureReport serialization should not fail")
    }

    /// Deserialize from JSON.
    pub fn decode(data: &[u8]) -> Result<Self, String> {
        serde_json::from_slice(data).map_err(|e| e.to_string())
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
/// (with the worker's message) for a memory-limit failure the producer
/// sent, `Data` for a data error it sent, else
/// `InvalidOperation("{context}: …")`.
pub(crate) fn remote_error(context: String, failure: &impl RemoteFailure) -> ExecutionError {
    match failure.status() {
        Some(status) if is_memory_limit(status) => {
            ExecutionError::ResourceExhausted(status.message().to_owned())
        }
        Some(status) => match data_error_kind(status) {
            Some(kind) => ExecutionError::Data {
                kind,
                message: status.message().to_owned(),
            },
            None => ExecutionError::InvalidOperation(format!("{context}: {}", failure.describe())),
        },
        None => ExecutionError::InvalidOperation(format!("{context}: {}", failure.describe())),
    }
}

/// The kind of a data error the producer sent as an `INTERNAL` status.
fn data_error_kind(status: &Status) -> Option<DataErrorKind> {
    if status.code() != Code::Internal {
        return None;
    }
    let tag = status.metadata().get(CLASS_KEY)?.to_str().ok()?;
    DataErrorKind::from_tag(tag)
}

/// A `RESOURCE_EXHAUSTED` the producer sent for a memory-limit failure,
/// not one the transport produced.
pub(crate) fn is_memory_limit(status: &Status) -> bool {
    status.code() == Code::ResourceExhausted
        && status
            .metadata()
            .get(CLASS_KEY)
            .is_some_and(|v| v == CLASS_RESOURCE_EXHAUSTED)
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
    fn a_failure_report_round_trips_with_its_class() {
        let query_id = arneb_common::QueryId::new();
        let failure = TaskFailure::resource_exhausted("spill limit exceeded");
        let report = TaskFailureReport::new(query_id, "q.2.1", &failure);
        let back = TaskFailureReport::decode(&report.encode()).unwrap();
        assert_eq!(back, report);
        assert_eq!(back.failure(), failure);
        assert!(matches!(
            back.failure().into_execution_error("task q.2.1 failed"),
            ExecutionError::ResourceExhausted(m) if m == "spill limit exceeded"
        ));
        let other = TaskFailure::new("boom").into_execution_error("task q.2.1 failed");
        assert_eq!(
            other.to_string(),
            "invalid operation: task q.2.1 failed: boom"
        );
        // A report from a worker that predates the class field decodes as
        // an ordinary failure.
        let legacy = format!(
            r#"{{"query_id":"{}","task_id":"t","message":"m"}}"#,
            query_id.0
        );
        assert!(
            !TaskFailureReport::decode(legacy.as_bytes())
                .unwrap()
                .resource_exhausted
        );
    }

    /// A data error reported straight to the coordinator keeps its kind,
    /// so the client gets its SQLSTATE, not a generic internal error.
    #[test]
    fn a_data_error_report_round_trips_with_its_kind() {
        let query_id = arneb_common::QueryId::new();
        let failure = TaskFailure::data(
            DataErrorKind::NumericValueOutOfRange,
            "value 3000000000 is out of range for Int32",
        );
        let report = TaskFailureReport::new(query_id, "q.2.1", &failure);
        let back = TaskFailureReport::decode(&report.encode()).unwrap();
        assert_eq!(back.failure(), failure);
        assert!(matches!(
            back.failure().into_execution_error("task q.2.1 failed"),
            ExecutionError::Data {
                kind: DataErrorKind::NumericValueOutOfRange,
                message,
            } if message == "value 3000000000 is out of range for Int32"
        ));
        // A kind this coordinator does not know reads as an ordinary failure.
        let unknown = TaskFailureReport {
            data_kind: Some("from-a-newer-worker".to_string()),
            ..report
        };
        assert_eq!(unknown.failure().class, FailureClass::Other);
    }

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
            &TaskFailure::resource_exhausted("query memory limit exceeded")
                .to_status("producer task failed"),
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

    /// #150 review: tonic reports an h2 `ENHANCE_YOUR_CALM` GOAWAY as
    /// `RESOURCE_EXHAUSTED`; without the producer's class marker it is a
    /// transport failure, not a memory limit.
    #[test]
    fn unmarked_resource_exhausted_is_not_a_memory_limit() {
        let goaway = Status::resource_exhausted("h2 protocol error: ENHANCE_YOUR_CALM");
        assert!(matches!(
            remote_error("remote task t".into(), &goaway),
            ExecutionError::InvalidOperation(_)
        ));
        let mid_stream = FlightError::Tonic(Box::new(goaway));
        assert!(matches!(
            remote_error("Flight stream error".into(), &mid_stream),
            ExecutionError::InvalidOperation(_)
        ));
    }

    /// #145: the class comes from the status alone. A message that
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
