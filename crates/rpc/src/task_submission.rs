//! Client-side Flight `do_action` helpers.
//!
//! Each helper opens a channel, sends one `Action`, and drains the
//! single ack frame the server returns. The wire formats are owned by
//! [`crate::task_descriptor`] and [`crate::dynamic_filter`].

use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::Action;
use tonic::transport::Channel;

use crate::dynamic_filter::{NotifyDynamicFilterRequest, ReportDynamicFilterRequest};
use crate::task_descriptor::TaskDescriptor;

async fn connect(address: &str) -> Result<FlightServiceClient<Channel>, String> {
    let channel = Channel::from_shared(address.to_string())
        .map_err(|e| format!("invalid flight address {address}: {e}"))?
        .connect()
        .await
        .map_err(|e| format!("failed to connect to {address}: {e}"))?;
    // A Bloom dynamic-filter Domain serialises to ~20 MB (fixed-size
    // 2^26-bit filter), which blows past tonic's 4 MB default message cap
    // and silently fails the `report_dynamic_filters` / `notify_dynamic_filter`
    // RPCs. Raise both directions so cross-fragment DF reports deliver.
    Ok(FlightServiceClient::new(channel)
        .max_decoding_message_size(crate::MAX_FLIGHT_MESSAGE_BYTES)
        .max_encoding_message_size(crate::MAX_FLIGHT_MESSAGE_BYTES))
}

async fn drain_ack(
    mut stream: tonic::Streaming<arrow_flight::Result>,
    label: &str,
) -> Result<(), String> {
    use futures::StreamExt;
    if let Some(result) = stream.next().await {
        let _ = result.map_err(|e| format!("{label} response error: {e}"))?;
    }
    Ok(())
}

/// Submit a task to a remote worker via Flight RPC.
pub async fn submit_task(worker_address: &str, descriptor: &TaskDescriptor) -> Result<(), String> {
    let mut client = connect(worker_address).await?;
    let action = Action {
        r#type: "submit_task".to_string(),
        body: descriptor.encode().into(),
    };
    let stream = client
        .do_action(action)
        .await
        .map_err(|e| format!("submit_task RPC failed: {e}"))?
        .into_inner();
    drain_ack(stream, "submit_task").await
}

/// Worker → coord: report one partition's partial dynamic filter Domain.
///
/// A1.3 (2026-05-27): defined but currently has no producer call site.
/// A1.5 wires `HashJoinExec` / `SemiJoinExec` build phases to invoke it
/// once a partition's `JoinDomainBuilder` finalises.
pub async fn report_dynamic_filters(
    coord_address: &str,
    request: &ReportDynamicFilterRequest,
) -> Result<(), String> {
    let mut client = connect(coord_address).await?;
    let action = Action {
        r#type: "report_dynamic_filters".to_string(),
        body: request.encode().into(),
    };
    let stream = client
        .do_action(action)
        .await
        .map_err(|e| format!("report_dynamic_filters RPC failed: {e}"))?
        .into_inner();
    drain_ack(stream, "report_dynamic_filters").await
}

/// Coord → worker: notify a worker task that a dynamic filter resolved.
///
/// A1.3 (2026-05-27): defined but currently has no producer call site.
/// A1.5's `QueryCoordinator` glue will invoke it from a
/// `DynamicFilterService` subscription handler once the service fires.
pub async fn notify_dynamic_filter(
    worker_address: &str,
    request: &NotifyDynamicFilterRequest,
) -> Result<(), String> {
    let mut client = connect(worker_address).await?;
    let action = Action {
        r#type: "notify_dynamic_filter".to_string(),
        body: request.encode().into(),
    };
    let stream = client
        .do_action(action)
        .await
        .map_err(|e| format!("notify_dynamic_filter RPC failed: {e}"))?
        .into_inner();
    drain_ack(stream, "notify_dynamic_filter").await
}
