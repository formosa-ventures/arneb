//! Client-side task submission to workers via Flight RPC.

use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::Action;
use tonic::transport::Channel;

use crate::task_descriptor::TaskDescriptor;

/// Submit a task to a remote worker via Flight RPC.
pub async fn submit_task(worker_address: &str, descriptor: &TaskDescriptor) -> Result<(), String> {
    let channel = Channel::from_shared(worker_address.to_string())
        .map_err(|e| format!("invalid worker address: {e}"))?
        .connect()
        .await
        .map_err(|e| format!("failed to connect to worker {worker_address}: {e}"))?;

    let mut client = FlightServiceClient::new(channel);

    let action = Action {
        r#type: "submit_task".to_string(),
        body: descriptor.encode().into(),
    };

    let mut stream = client
        .do_action(action)
        .await
        .map_err(|e| format!("submit_task RPC failed: {e}"))?
        .into_inner();

    // Consume acknowledgment
    use futures::StreamExt;
    if let Some(result) = stream.next().await {
        let _ = result.map_err(|e| format!("task submission response error: {e}"))?;
    }

    Ok(())
}
