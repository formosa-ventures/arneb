//! Arrow Flight service for inter-node data exchange.
//!
//! The Flight server runs on each worker (and coordinator in standalone mode).
//! It serves RecordBatch streams from [`OutputBuffer`]s to remote consumers.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, RwLock};

use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PutResult, SchemaResult, Ticket,
};
use futures::Stream;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

use crate::heartbeat::HeartbeatMessage;
use crate::output_buffer::OutputBuffer;

/// Callback invoked when a heartbeat is received.
pub type HeartbeatCallback = Arc<dyn Fn(HeartbeatMessage) + Send + Sync>;

/// Shared state for the Flight service — holds output buffers keyed by task ID.
#[derive(Clone)]
pub struct FlightState {
    buffers: Arc<RwLock<HashMap<String, Arc<tokio::sync::Mutex<OutputBuffer>>>>>,
    heartbeat_callback: Option<HeartbeatCallback>,
}

impl Default for FlightState {
    fn default() -> Self {
        Self {
            buffers: Arc::new(RwLock::new(HashMap::new())),
            heartbeat_callback: None,
        }
    }
}

impl FlightState {
    /// Creates a new empty state.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a new state with a heartbeat callback (for coordinator mode).
    pub fn with_heartbeat_callback(callback: HeartbeatCallback) -> Self {
        Self {
            buffers: Arc::new(RwLock::new(HashMap::new())),
            heartbeat_callback: Some(callback),
        }
    }

    /// Register an output buffer for a task.
    pub fn register_buffer(&self, task_id: String, buffer: OutputBuffer) {
        self.buffers
            .write()
            .unwrap()
            .insert(task_id, Arc::new(tokio::sync::Mutex::new(buffer)));
    }

    /// Get a buffer by task ID.
    pub fn get_buffer(&self, task_id: &str) -> Option<Arc<tokio::sync::Mutex<OutputBuffer>>> {
        self.buffers.read().unwrap().get(task_id).cloned()
    }

    /// Remove a buffer when task is complete.
    pub fn remove_buffer(&self, task_id: &str) {
        self.buffers.write().unwrap().remove(task_id);
    }
}

/// Arrow Flight service implementation for data exchange.
#[doc(hidden)]
pub struct TrinoFlightService {
    state: FlightState,
}

impl TrinoFlightService {
    /// Creates a new Flight service with the given shared state.
    pub fn new(state: FlightState) -> Self {
        Self { state }
    }
}

type BoxedStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

#[tonic::async_trait]
impl FlightService for TrinoFlightService {
    type HandshakeStream = BoxedStream<HandshakeResponse>;
    type ListFlightsStream = BoxedStream<FlightInfo>;
    type DoGetStream = BoxedStream<FlightData>;
    type DoPutStream = BoxedStream<PutResult>;
    type DoActionStream = BoxedStream<arrow_flight::Result>;
    type ListActionsStream = BoxedStream<ActionType>;
    type DoExchangeStream = BoxedStream<FlightData>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake not needed"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info"))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema"))
    }

    /// Fetch RecordBatches from a task's output buffer.
    ///
    /// Ticket format: `task_id:partition_id` (e.g., "task-123:0")
    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();
        let ticket_str = String::from_utf8(ticket.ticket.to_vec())
            .map_err(|_| Status::invalid_argument("invalid ticket encoding"))?;

        let parts: Vec<&str> = ticket_str.split(':').collect();
        if parts.len() != 2 {
            return Err(Status::invalid_argument(
                "ticket format: task_id:partition_id",
            ));
        }
        let task_id = parts[0];
        let partition_id: usize = parts[1]
            .parse()
            .map_err(|_| Status::invalid_argument("invalid partition_id"))?;

        let buffer = self
            .state
            .get_buffer(task_id)
            .ok_or_else(|| Status::not_found(format!("no buffer for task '{task_id}'")))?;

        let mut buf = buffer.lock().await;
        let receiver = buf.take_receiver(partition_id).ok_or_else(|| {
            Status::already_exists(format!(
                "partition {partition_id} already consumed for task '{task_id}'"
            ))
        })?;

        let schema = buf.schema();

        // Stream RecordBatches as FlightData.
        let stream = async_stream(schema, receiver);
        Ok(Response::new(stream))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("do_put"))
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let action = request.into_inner();
        match action.r#type.as_str() {
            "heartbeat" => {
                let msg = HeartbeatMessage::decode(&action.body)
                    .map_err(|e| Status::invalid_argument(format!("bad heartbeat: {e}")))?;

                tracing::debug!(
                    worker_id = %msg.worker_id,
                    address = %msg.flight_address,
                    "received heartbeat"
                );

                if let Some(ref callback) = self.state.heartbeat_callback {
                    callback(msg);
                }

                let result = arrow_flight::Result { body: "ok".into() };
                let stream = futures::stream::once(async { Ok(result) });
                Ok(Response::new(Box::pin(stream)))
            }
            other => Err(Status::unimplemented(format!(
                "unknown action type: {other}"
            ))),
        }
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange"))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<arrow_flight::PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info"))
    }
}

/// Convert an mpsc::Receiver of RecordBatch into a Stream of FlightData.
fn async_stream(
    schema: Arc<arrow::datatypes::Schema>,
    mut receiver: tokio::sync::mpsc::Receiver<arrow::array::RecordBatch>,
) -> BoxedStream<FlightData> {
    use arrow_flight::encode::FlightDataEncoderBuilder;

    // Create a stream from the receiver.
    let batch_stream = async_stream::stream! {
        while let Some(batch) = receiver.recv().await {
            yield Ok(batch) as Result<arrow::array::RecordBatch, arrow_flight::error::FlightError>;
        }
    };

    // Use FlightDataEncoderBuilder to encode batches as FlightData.
    let encoder = FlightDataEncoderBuilder::new()
        .with_schema(schema)
        .build(batch_stream);

    // Convert FlightError to tonic::Status.
    #[allow(clippy::result_large_err)]
    let mapped = futures::StreamExt::map(encoder, |result| {
        result.map_err(|e| Status::internal(format!("flight encoding error: {e}")))
    });

    Box::pin(mapped)
}

/// Start the Arrow Flight server on the given address.
pub async fn start_flight_server(
    addr: &str,
    state: FlightState,
) -> Result<(), Box<dyn std::error::Error>> {
    let addr = addr.parse()?;
    let service = TrinoFlightService::new(state);

    tracing::info!(%addr, "starting Arrow Flight server");

    Server::builder()
        .add_service(FlightServiceServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::array::RecordBatch;
    use arrow::datatypes::{DataType, Field, Schema};

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]))
    }

    fn test_batch(schema: &Arc<Schema>, values: Vec<i32>) -> RecordBatch {
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(values))]).unwrap()
    }

    #[test]
    fn flight_state_register_and_get() {
        let state = FlightState::new();
        let schema = test_schema();
        let buf = OutputBuffer::single(32, schema);
        state.register_buffer("task-1".into(), buf);
        assert!(state.get_buffer("task-1").is_some());
        assert!(state.get_buffer("task-2").is_none());
    }

    #[test]
    fn flight_state_remove_buffer() {
        let state = FlightState::new();
        let schema = test_schema();
        let buf = OutputBuffer::single(32, schema);
        state.register_buffer("task-1".into(), buf);
        state.remove_buffer("task-1");
        assert!(state.get_buffer("task-1").is_none());
    }
}
