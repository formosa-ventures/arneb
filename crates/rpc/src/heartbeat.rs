//! Heartbeat client for worker → coordinator registration.
//!
//! Workers periodically send heartbeat messages to the coordinator
//! to register themselves and report health.

use arneb_common::error::ExecutionError;
use arrow_flight::Action;
use serde::{Deserialize, Serialize};

use crate::security::{cluster_security, ClusterSecurity};

/// The release this binary was built from. Every crate shares the workspace
/// version, so this is the server's version.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Heartbeat message sent from worker to coordinator, encoded as JSON.
///
/// Fields added after `version` must be `#[serde(default)]` so a heartbeat
/// stays readable within a release line.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeartbeatMessage {
    /// Unique worker identifier.
    pub worker_id: String,
    /// Address the worker's Flight server listens on.
    pub flight_address: String,
    /// Maximum concurrent splits this worker can handle.
    pub max_splits: usize,
    /// The worker's release version; the coordinator admits only its own.
    pub version: String,
    /// Whether the worker takes new queries.
    #[serde(default)]
    pub state: WorkerState,
}

/// A worker's lifecycle state, as its heartbeat reports it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerState {
    /// Takes new queries.
    #[default]
    Active,
    /// Shutting down: finishes its queries but takes no new ones.
    Draining,
    /// Drained and exiting; the coordinator forgets it.
    Stopped,
}

/// The version a legacy (pre-handshake) heartbeat decodes to. It matches no
/// release, so the coordinator refuses such a worker like any other mismatch.
pub const LEGACY_VERSION: &str = "<0.1.6";

impl HeartbeatMessage {
    /// A heartbeat from this binary, carrying [`VERSION`].
    pub fn new(worker_id: String, flight_address: String, max_splits: usize) -> Self {
        Self {
            worker_id,
            flight_address,
            max_splits,
            version: VERSION.to_string(),
            state: WorkerState::Active,
        }
    }

    /// Serialize to JSON.
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("HeartbeatMessage serialization should not fail")
    }

    /// Deserialize from JSON. The pipe-separated
    /// `worker_id|flight_address|max_splits` body of releases before 0.1.6
    /// decodes with [`LEGACY_VERSION`].
    pub fn decode(data: &[u8]) -> Result<Self, ExecutionError> {
        serde_json::from_slice(data).or_else(|json_err| {
            Self::decode_legacy(data).ok_or_else(|| {
                ExecutionError::InvalidOperation(format!("invalid heartbeat: {json_err}"))
            })
        })
    }

    fn decode_legacy(data: &[u8]) -> Option<Self> {
        let text = std::str::from_utf8(data).ok()?;
        let [worker_id, flight_address, max_splits] = text.split('|').collect::<Vec<_>>()[..]
        else {
            return None;
        };
        Some(Self {
            worker_id: worker_id.to_string(),
            flight_address: flight_address.to_string(),
            max_splits: max_splits.parse().ok()?,
            version: LEGACY_VERSION.to_string(),
            state: WorkerState::Active,
        })
    }
}

/// Send a heartbeat to the coordinator's Flight server.
///
/// Uses the Flight `do_action` RPC with action type "heartbeat".
pub async fn send_heartbeat(
    coordinator_address: &str,
    message: &HeartbeatMessage,
) -> Result<(), ExecutionError> {
    Ok(send_heartbeat_with(coordinator_address, message, cluster_security()).await?)
}

/// Why a heartbeat failed.
#[derive(Debug, thiserror::Error)]
pub enum HeartbeatError {
    /// The coordinator rejected this node's cluster secret.
    #[error(
        "coordinator {0} rejected the cluster secret; check [cluster] secret / \
         ARNEB_CLUSTER_SECRET on this worker and the coordinator"
    )]
    Unauthenticated(String),
    /// The coordinator runs another release and refused this worker.
    #[error("coordinator {0} refused this worker: {1}")]
    Incompatible(String, String),
    /// Any other failure (connect, transport, server error).
    #[error("{0}")]
    Other(String),
}

impl From<HeartbeatError> for ExecutionError {
    fn from(e: HeartbeatError) -> Self {
        ExecutionError::InvalidOperation(e.to_string())
    }
}

/// [`send_heartbeat`] with explicit cluster security, keeping the
/// rejected-secret case distinct.
pub async fn send_heartbeat_with(
    coordinator_address: &str,
    message: &HeartbeatMessage,
    security: &ClusterSecurity,
) -> Result<(), HeartbeatError> {
    let mut client = crate::security::connect(coordinator_address, security)
        .await
        .map_err(|e| HeartbeatError::Other(format!("coordinator at {coordinator_address}: {e}")))?;

    let action = Action {
        r#type: "heartbeat".to_string(),
        body: message.encode().into(),
    };

    let _response = client.do_action(action).await.map_err(|status| {
        if status.code() == tonic::Code::Unauthenticated {
            HeartbeatError::Unauthenticated(coordinator_address.to_owned())
        } else if status.code() == tonic::Code::FailedPrecondition {
            HeartbeatError::Incompatible(
                coordinator_address.to_owned(),
                status.message().to_owned(),
            )
        } else {
            HeartbeatError::Other(format!("heartbeat failed: {status}"))
        }
    })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn heartbeat_encode_decode() {
        let msg = HeartbeatMessage::new("w1".into(), "http://localhost:9090".into(), 256);
        assert_eq!(msg.version, VERSION);
        let decoded = HeartbeatMessage::decode(&msg.encode()).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn state_round_trips_and_defaults_to_active() {
        let mut msg = HeartbeatMessage::new("w1".into(), "http://localhost:9090".into(), 8);
        msg.state = WorkerState::Draining;
        let json = String::from_utf8(msg.encode()).unwrap();
        assert!(json.contains("\"state\":\"draining\""), "{json}");
        assert_eq!(HeartbeatMessage::decode(json.as_bytes()).unwrap(), msg);

        let without = r#"{"worker_id":"w1","flight_address":"a","max_splits":8,"version":"x"}"#;
        let decoded = HeartbeatMessage::decode(without.as_bytes()).unwrap();
        assert_eq!(decoded.state, WorkerState::Active);
    }

    #[test]
    fn a_legacy_heartbeat_decodes_with_the_legacy_version() {
        let msg = HeartbeatMessage::decode(b"w1|http://localhost:9090|256").unwrap();
        assert_eq!(msg.worker_id, "w1");
        assert_eq!(msg.flight_address, "http://localhost:9090");
        assert_eq!(msg.max_splits, 256);
        assert_eq!(msg.version, LEGACY_VERSION);
    }

    #[test]
    fn heartbeat_decode_invalid() {
        for body in [&b"bad"[..], b"{\"worker_id\":\"w1\"}", b"a|b", b"a|b|many"] {
            assert!(HeartbeatMessage::decode(body).is_err());
        }
    }
}
