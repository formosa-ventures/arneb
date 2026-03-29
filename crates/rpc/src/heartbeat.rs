//! Heartbeat client for worker → coordinator registration.
//!
//! Workers periodically send heartbeat messages to the coordinator
//! to register themselves and report health.

use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::Action;
use trino_common::error::ExecutionError;

/// Heartbeat message sent from worker to coordinator.
#[derive(Debug, Clone)]
pub struct HeartbeatMessage {
    /// Unique worker identifier.
    pub worker_id: String,
    /// Address the worker's Flight server listens on.
    pub flight_address: String,
    /// Maximum concurrent splits this worker can handle.
    pub max_splits: usize,
}

impl HeartbeatMessage {
    /// Serialize to a simple string format: "worker_id|flight_address|max_splits"
    pub fn encode(&self) -> Vec<u8> {
        format!(
            "{}|{}|{}",
            self.worker_id, self.flight_address, self.max_splits
        )
        .into_bytes()
    }

    /// Deserialize from the string format.
    pub fn decode(data: &[u8]) -> Result<Self, ExecutionError> {
        let s = String::from_utf8(data.to_vec()).map_err(|e| {
            ExecutionError::InvalidOperation(format!("invalid heartbeat encoding: {e}"))
        })?;
        let parts: Vec<&str> = s.split('|').collect();
        if parts.len() != 3 {
            return Err(ExecutionError::InvalidOperation(
                "heartbeat format: worker_id|flight_address|max_splits".into(),
            ));
        }
        Ok(Self {
            worker_id: parts[0].to_string(),
            flight_address: parts[1].to_string(),
            max_splits: parts[2].parse().map_err(|e| {
                ExecutionError::InvalidOperation(format!("invalid max_splits: {e}"))
            })?,
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
    let mut client = FlightServiceClient::connect(coordinator_address.to_string())
        .await
        .map_err(|e| {
            ExecutionError::InvalidOperation(format!(
                "failed to connect to coordinator at {coordinator_address}: {e}"
            ))
        })?;

    let action = Action {
        r#type: "heartbeat".to_string(),
        body: message.encode().into(),
    };

    let _response = client
        .do_action(action)
        .await
        .map_err(|e| ExecutionError::InvalidOperation(format!("heartbeat failed: {e}")))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn heartbeat_encode_decode() {
        let msg = HeartbeatMessage {
            worker_id: "w1".into(),
            flight_address: "http://localhost:9090".into(),
            max_splits: 256,
        };
        let encoded = msg.encode();
        let decoded = HeartbeatMessage::decode(&encoded).unwrap();
        assert_eq!(decoded.worker_id, "w1");
        assert_eq!(decoded.flight_address, "http://localhost:9090");
        assert_eq!(decoded.max_splits, 256);
    }

    #[test]
    fn heartbeat_decode_invalid() {
        assert!(HeartbeatMessage::decode(b"bad").is_err());
    }
}
