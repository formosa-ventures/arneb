//! Arrow Flight RPC layer for inter-node data exchange.
//!
//! Provides the communication infrastructure for distributed query execution:
//! - [`OutputBuffer`]: Bounded, partition-aware buffer for task output
//! - [`ExchangeClient`]: Async client for fetching data from remote workers
//! - [`FlightState`] + [`start_flight_server`]: Arrow Flight server for streaming RecordBatches
//! - [`HeartbeatMessage`] + [`send_heartbeat`]: Worker → coordinator heartbeat

mod dynamic_filter;
mod exchange_client;
mod flight_service;
mod heartbeat;
mod output_buffer;
pub mod task_descriptor;
mod task_submission;

/// Max Arrow Flight gRPC message size (encode + decode) for the
/// `do_action` control RPCs. tonic defaults to 4 MB, but a cross-fragment
/// Bloom dynamic-filter Domain serialises to ~20 MB (fixed-size 2^26-bit
/// filter), so the default silently fails `report_dynamic_filters` /
/// `notify_dynamic_filter`. 64 MB leaves comfortable headroom. Data
/// exchange (`do_get`) is streamed per-batch and unaffected.
pub const MAX_FLIGHT_MESSAGE_BYTES: usize = 64 * 1024 * 1024;

pub use dynamic_filter::{NotifyDynamicFilterRequest, ReportDynamicFilterRequest};
pub use exchange_client::ExchangeClient;
pub use flight_service::{
    start_flight_server, BufferKind, DfNotifyCallback, DfReportCallback, FlightState,
    HeartbeatCallback, TaskCallback, TaskStatusCallback, TaskStatusResponse,
};
pub use heartbeat::{send_heartbeat, HeartbeatMessage};
pub use output_buffer::{
    BroadcastOutputBuffer, BroadcastStream, OutputBuffer, TrackedReceiver, TrackedSendOutcome,
    TrackedSender,
};
pub use task_descriptor::{SourceExchange, TaskDescriptor};
pub use task_submission::{notify_dynamic_filter, report_dynamic_filters, submit_task};

/// Test helper: create a FlightServiceServer for integration tests.
#[doc(hidden)]
pub fn __flight_service_for_test(
    state: FlightState,
) -> arrow_flight::flight_service_server::FlightServiceServer<flight_service::TrinoFlightService> {
    arrow_flight::flight_service_server::FlightServiceServer::new(
        flight_service::TrinoFlightService::new(state),
    )
}
