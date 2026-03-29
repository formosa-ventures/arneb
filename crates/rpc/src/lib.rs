//! Arrow Flight RPC layer for inter-node data exchange.
//!
//! Provides the communication infrastructure for distributed query execution:
//! - [`OutputBuffer`]: Bounded, partition-aware buffer for task output
//! - [`ExchangeClient`]: Async client for fetching data from remote workers
//! - [`FlightState`] + [`start_flight_server`]: Arrow Flight server for streaming RecordBatches
//! - [`HeartbeatMessage`] + [`send_heartbeat`]: Worker → coordinator heartbeat

mod exchange_client;
mod flight_service;
mod heartbeat;
mod output_buffer;
pub mod task_descriptor;
mod task_submission;

pub use exchange_client::ExchangeClient;
pub use flight_service::{start_flight_server, FlightState, HeartbeatCallback, TaskCallback};
pub use heartbeat::{send_heartbeat, HeartbeatMessage};
pub use output_buffer::OutputBuffer;
pub use task_descriptor::TaskDescriptor;
pub use task_submission::submit_task;

/// Test helper: create a FlightServiceServer for integration tests.
#[doc(hidden)]
pub fn __flight_service_for_test(
    state: FlightState,
) -> arrow_flight::flight_service_server::FlightServiceServer<flight_service::TrinoFlightService> {
    arrow_flight::flight_service_server::FlightServiceServer::new(
        flight_service::TrinoFlightService::new(state),
    )
}
