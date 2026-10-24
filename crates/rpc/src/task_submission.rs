//! Client-side Flight `do_action` helpers.
//!
//! Each helper opens a channel, sends one `Action`, and drains the
//! single ack frame the server returns. The wire formats are owned by
//! [`crate::task_descriptor`] and [`crate::dynamic_filter`].

use arrow_flight::Action;

use crate::dynamic_filter::{NotifyDynamicFilterRequest, ReportDynamicFilterRequest};
use crate::security::{cluster_security, ClusterSecurity, FlightClient};
use crate::task_descriptor::TaskDescriptor;

async fn connect(address: &str, security: &ClusterSecurity) -> Result<FlightClient, String> {
    let client = crate::security::connect(address, security).await?;
    // A Bloom dynamic-filter Domain serialises to ~20 MB (fixed-size
    // 2^26-bit filter), which blows past tonic's 4 MB default message cap
    // and silently fails the `report_dynamic_filters` / `notify_dynamic_filter`
    // RPCs. Raise both directions so cross-fragment DF reports deliver.
    Ok(client
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
    submit_task_with(worker_address, descriptor, cluster_security()).await
}

/// [`submit_task`] with explicit cluster security instead of the installed one.
pub async fn submit_task_with(
    worker_address: &str,
    descriptor: &TaskDescriptor,
    security: &ClusterSecurity,
) -> Result<(), String> {
    let mut client = connect(worker_address, security).await?;
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
    let mut client = connect(coord_address, cluster_security()).await?;
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
    let mut client = connect(worker_address, cluster_security()).await?;
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

/// Coord → worker: cancel every task of `query_id` on that worker
/// (admission-control D4). Idempotent; an unknown query is a no-op ack.
pub async fn cancel_query(
    worker_address: &str,
    query_id: arneb_common::QueryId,
) -> Result<(), String> {
    cancel_query_with(worker_address, query_id, cluster_security()).await
}

/// [`cancel_query`] with explicit cluster security instead of the installed one.
pub async fn cancel_query_with(
    worker_address: &str,
    query_id: arneb_common::QueryId,
    security: &ClusterSecurity,
) -> Result<(), String> {
    query_action(worker_address, "cancel_query", query_id, security).await
}

/// Coord → worker: `query_id` finished, so drop its tasks and buffers on
/// that worker. Idempotent; an unknown query is a no-op ack.
pub async fn release_query(
    worker_address: &str,
    query_id: arneb_common::QueryId,
) -> Result<(), String> {
    query_action(
        worker_address,
        "release_query",
        query_id,
        cluster_security(),
    )
    .await
}

async fn query_action(
    worker_address: &str,
    kind: &str,
    query_id: arneb_common::QueryId,
    security: &ClusterSecurity,
) -> Result<(), String> {
    let mut client = connect(worker_address, security).await?;
    let action = Action {
        r#type: kind.to_string(),
        body: query_id.0.to_string().into_bytes().into(),
    };
    let stream = client
        .do_action(action)
        .await
        .map_err(|e| format!("{kind} RPC failed: {e}"))?
        .into_inner();
    drain_ack(stream, kind).await
}
