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

use crate::dynamic_filter::{NotifyDynamicFilterRequest, ReportDynamicFilterRequest};
use crate::heartbeat::HeartbeatMessage;
use crate::output_buffer::{BroadcastOutputBuffer, OutputBuffer};
use crate::task_descriptor::TaskDescriptor;

/// Task liveness as reported by the worker's `TaskManager`.
///
/// Used by the `do_get` handler to distinguish "task alive but output
/// not ready" from "task failed" when the `OutputBuffer` is not yet
/// registered. This lets the coordinator's `ExchangeClient` replace the
/// hard 5-minute deadline with a liveness check.
#[derive(Debug, Clone)]
pub enum TaskStatusResponse {
    /// Task is executing; buffer will appear eventually.
    Running,
    /// Task failed before registering its output buffer.
    Failed(String),
}

/// Callback that resolves a task's liveness on this worker. Keyed by the
/// query-scoped task ID (`"{query_id}.{task_id}"`). Returns `None` when
/// the task ID is unknown (never submitted or already cleaned up).
pub type TaskStatusCallback = Arc<dyn Fn(&str) -> Option<TaskStatusResponse> + Send + Sync>;

/// Callback invoked when a heartbeat is received.
pub type HeartbeatCallback = Arc<dyn Fn(HeartbeatMessage) + Send + Sync>;

/// Callback invoked when a task submission is received.
pub type TaskCallback = Arc<dyn Fn(TaskDescriptor) + Send + Sync>;

/// Coord-side callback invoked when a worker reports a partition's
/// partial dynamic filter Domain via the `report_dynamic_filters`
/// Flight action. The handler routes the report into the matching
/// query's `DynamicFilterService`. A1.3 — inert when unset.
pub type DfReportCallback = Arc<dyn Fn(ReportDynamicFilterRequest) + Send + Sync>;

/// Worker-side callback invoked when the coordinator pushes a
/// resolved dynamic filter Domain via the `notify_dynamic_filter`
/// Flight action. The handler routes the Domain into the target
/// task's per-task `DynamicFilterCollector`. A1.3 — inert when unset.
pub type DfNotifyCallback = Arc<dyn Fn(NotifyDynamicFilterRequest) + Send + Sync>;

/// Producer-side buffer kind held by the worker's `FlightState`.
///
/// A2.1.2 (2026-05-28): partitioned producers (the W3-Hash auto-wrap
/// path) allocate an `OutputBuffer` with N per-partition mpsc channels;
/// broadcast producers allocate a single in-memory `BroadcastOutputBuffer`
/// that every consumer subscribes to independently. They share the
/// `{query_id}.{task_id}` key namespace in `FlightState.buffers`; `do_get`
/// matches on this enum to pick the right consume path.
#[derive(Clone)]
pub enum BufferKind {
    /// Partitioned producer — consumers call `take_receiver(partition_id)`
    /// (one-shot per partition).
    Partitioned(Arc<tokio::sync::Mutex<OutputBuffer>>),
    /// Broadcast producer — consumers call `subscribe()` (multi-call,
    /// each call returns an independent `BroadcastStream`).
    Broadcast(Arc<BroadcastOutputBuffer>),
}

/// Shared state for the Flight service — holds output buffers keyed by task ID.
///
/// A2.1.2 (2026-05-28): values are wrapped in `BufferKind` so partitioned
/// and broadcast producers share one key namespace (`{query_id}.{task_id}`).
/// `do_get` matches on the kind to pick the partitioned `take_receiver`
/// path vs the broadcast `subscribe` path. Removing the kind enum and
/// reverting to a single concrete type is a one-commit revert if the
/// broadcast path is ever disabled.
#[derive(Clone)]
pub struct FlightState {
    buffers: Arc<RwLock<HashMap<String, BufferKind>>>,
    heartbeat_callback: Option<HeartbeatCallback>,
    task_callback: Option<TaskCallback>,
    task_status_callback: Option<TaskStatusCallback>,
    df_report_callback: Option<DfReportCallback>,
    df_notify_callback: Option<DfNotifyCallback>,
}

impl Default for FlightState {
    fn default() -> Self {
        Self {
            buffers: Arc::new(RwLock::new(HashMap::new())),
            heartbeat_callback: None,
            task_callback: None,
            task_status_callback: None,
            df_report_callback: None,
            df_notify_callback: None,
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
            task_callback: None,
            task_status_callback: None,
            df_report_callback: None,
            df_notify_callback: None,
        }
    }

    /// Set a task submission callback (for worker mode).
    pub fn set_task_callback(&mut self, callback: TaskCallback) {
        self.task_callback = Some(callback);
    }

    /// Set a task-status callback (for worker mode). Enables `do_get`
    /// to distinguish "task running, buffer not yet registered" from
    /// "task failed" — replacing the coordinator's hard 5-min deadline
    /// with a liveness check.
    pub fn set_task_status_callback(&mut self, callback: TaskStatusCallback) {
        self.task_status_callback = Some(callback);
    }

    /// Set a callback for worker-reported partition Domains
    /// (coord-side). A1.3 — caller is `QueryCoordinator`.
    pub fn set_df_report_callback(&mut self, callback: DfReportCallback) {
        self.df_report_callback = Some(callback);
    }

    /// Set a callback for coord-pushed resolved Domains (worker-side).
    /// A1.3 — caller is `TaskManager`.
    pub fn set_df_notify_callback(&mut self, callback: DfNotifyCallback) {
        self.df_notify_callback = Some(callback);
    }

    /// Register a partitioned output buffer for a task. A2.1.2 renamed
    /// from `register_buffer`; the broadcast equivalent is
    /// `register_broadcast_buffer`.
    pub fn register_partitioned_buffer(&self, task_id: String, buffer: OutputBuffer) {
        self.buffers.write().unwrap().insert(
            task_id,
            BufferKind::Partitioned(Arc::new(tokio::sync::Mutex::new(buffer))),
        );
    }

    /// Register a broadcast output buffer for a task. A2.1.2 (2026-05-28).
    /// The `Arc<BroadcastOutputBuffer>` is shared between the producer
    /// pumper (which calls `write_batch` + `finish`) and the Flight
    /// `do_get` handler (which calls `subscribe` per consumer).
    pub fn register_broadcast_buffer(&self, task_id: String, buffer: Arc<BroadcastOutputBuffer>) {
        self.buffers
            .write()
            .unwrap()
            .insert(task_id, BufferKind::Broadcast(buffer));
    }

    /// Look up the buffer for a task by key. A2.1.2 returns `BufferKind`
    /// (cloned out of the map) so the caller — typically the `do_get`
    /// handler — matches on the kind to pick partitioned vs broadcast
    /// consume.
    pub fn get_buffer(&self, task_id: &str) -> Option<BufferKind> {
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
    ///
    /// A2.1.2 (2026-05-28): server matches on `BufferKind`. Partitioned
    /// buffers use the existing one-shot `take_receiver(partition_id)`
    /// path; broadcast buffers ignore `partition_id` and subscribe an
    /// independent `BroadcastStream` per call — so multiple consumer
    /// tasks can each pull the full output set.
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

        match self.state.get_buffer(task_id) {
            Some(BufferKind::Partitioned(buffer)) => {
                let mut buf = buffer.lock().await;
                // dist-mxn-nested-joins T2 (2026-06-05): env-gated trace of
                // every partitioned do_get so the double-consume ("already
                // consumed") in q07/q08 at N>2 is captured with the exact
                // (task_id, partition_id) pair and whether the take succeeded.
                let traced = std::env::var("ARNEB_TRACE_FRAGMENTS")
                    .map(|v| v != "0" && !v.is_empty())
                    .unwrap_or(false);
                let receiver = buf.take_receiver(partition_id).ok_or_else(|| {
                    if traced {
                        eprintln!(
                            "[FRAGTRACE] DO_GET task='{task_id}' partition={partition_id} -> ALREADY_CONSUMED"
                        );
                    }
                    Status::already_exists(format!(
                        "partition {partition_id} already consumed for task '{task_id}'"
                    ))
                })?;
                if traced {
                    eprintln!("[FRAGTRACE] DO_GET task='{task_id}' partition={partition_id} -> OK");
                }

                let schema = buf.schema();
                // B-fix-3 (2026-05-22): clone the producer's failure handle so
                // `async_stream` can yield a Flight error frame instead of clean
                // EOF if the pumper crashes mid-stream.
                let failure = buf.failure_handle();
                drop(buf);

                // Stream RecordBatches as FlightData.
                let stream = async_stream(
                    schema,
                    receiver,
                    failure,
                    format!("{task_id}:{partition_id}"),
                );
                Ok(Response::new(stream))
            }
            Some(BufferKind::Broadcast(buffer)) => {
                // A2.1.2: each `do_get` subscribes a fresh independent
                // `BroadcastStream` — multiple consumer tasks can pull
                // the same full output set without coordination.
                // `partition_id` is informational only (kept in the
                // ticket format for protocol uniformity).
                let _ = partition_id;
                let schema = buffer.schema();
                let stream = buffer.subscribe();
                let failure = buffer.failure_handle();
                let flight_stream = async_stream_broadcast(schema, stream, failure);
                Ok(Response::new(flight_stream))
            }
            None => {
                if let Some(ref cb) = self.state.task_status_callback {
                    match cb(task_id) {
                        Some(TaskStatusResponse::Running) => {
                            return Err(Status::unavailable(format!(
                                "task '{task_id}' running, buffer not yet registered"
                            )));
                        }
                        Some(TaskStatusResponse::Failed(reason)) => {
                            return Err(Status::internal(format!(
                                "task '{task_id}' failed: {reason}"
                            )));
                        }
                        None => {}
                    }
                }
                Err(Status::not_found(format!("no buffer for task '{task_id}'")))
            }
        }
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
            "submit_task" => {
                let descriptor = TaskDescriptor::decode(&action.body)
                    .map_err(|e| Status::invalid_argument(format!("bad task descriptor: {e}")))?;

                tracing::info!(
                    task_id = %descriptor.task_id,
                    stage_id = %descriptor.stage_id,
                    "received task submission"
                );

                if let Some(ref callback) = self.state.task_callback {
                    callback(descriptor);
                } else {
                    return Err(Status::unavailable("worker has no task manager registered"));
                }

                let result = arrow_flight::Result {
                    body: "task_accepted".into(),
                };
                let stream = futures::stream::once(async { Ok(result) });
                Ok(Response::new(Box::pin(stream)))
            }
            "report_dynamic_filters" => {
                let req = ReportDynamicFilterRequest::decode(&action.body)
                    .map_err(|e| Status::invalid_argument(format!("bad df report: {e}")))?;

                tracing::debug!(
                    query_id = %req.query_id,
                    task_id = %req.task_id,
                    df_id = %req.df_id,
                    partition_idx = req.partition_idx,
                    "received dynamic filter report"
                );

                if let Some(ref callback) = self.state.df_report_callback {
                    callback(req);
                }
                // No callback registered = inert during A1.3; still ack
                // so the worker doesn't treat it as a transport error.

                let result = arrow_flight::Result {
                    body: "df_report_accepted".into(),
                };
                let stream = futures::stream::once(async { Ok(result) });
                Ok(Response::new(Box::pin(stream)))
            }
            "notify_dynamic_filter" => {
                let req = NotifyDynamicFilterRequest::decode(&action.body)
                    .map_err(|e| Status::invalid_argument(format!("bad df notify: {e}")))?;

                tracing::debug!(
                    query_id = %req.query_id,
                    task_id = %req.task_id,
                    df_id = %req.df_id,
                    "received dynamic filter notify"
                );

                if let Some(ref callback) = self.state.df_notify_callback {
                    callback(req);
                }

                let result = arrow_flight::Result {
                    body: "df_notify_accepted".into(),
                };
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
///
/// B-fix-3 (2026-05-22): when the receiver EOFs, check the producer's
/// shared `failure` flag. If the pumper recorded an error before its
/// sender dropped, the stream yields a `FlightError` so the consumer
/// (`ExchangeClient`) sees `Some(Err(_))` and propagates the failure to
/// the coord instead of treating the truncated output as success.
fn async_stream(
    schema: Arc<arrow::datatypes::Schema>,
    mut receiver: crate::output_buffer::TrackedReceiver,
    failure: std::sync::Arc<std::sync::Mutex<Option<String>>>,
    ticket: String,
) -> BoxedStream<FlightData> {
    use arrow_flight::encode::FlightDataEncoderBuilder;

    // [EXCHTRACE] (2026-06-12, q21 drop-point profiling): per-ticket serve-side
    // row tally, gated by ARNEB_TRACE_FRAGMENTS. Pinning a silent distributed
    // row-drop = compare SERVE (here) vs the consumer-side RECV across runs; a
    // ticket whose count varies run-to-run marks the dropping exchange. See
    // project_2026-06-11_sf30_blast_radius_q21_only.
    let traced = std::env::var("ARNEB_TRACE_FRAGMENTS")
        .map(|v| v != "0" && !v.is_empty())
        .unwrap_or(false);

    // Create a stream from the receiver. After EOF, surface any
    // producer failure as a final `Err` frame.
    let batch_stream = async_stream::stream! {
        let mut rows = 0usize;
        let mut batches = 0usize;
        while let Some(batch) = receiver.recv().await {
            rows += batch.num_rows();
            batches += 1;
            yield Ok(batch) as Result<arrow::array::RecordBatch, arrow_flight::error::FlightError>;
        }
        if traced {
            eprintln!("[EXCHTRACE] SERVE ticket={ticket} rows={rows} batches={batches}");
        }
        if let Some(msg) = failure.lock().ok().and_then(|mut g| g.take()) {
            yield Err(arrow_flight::error::FlightError::ProtocolError(format!(
                "producer task failed: {msg}"
            ))) as Result<arrow::array::RecordBatch, arrow_flight::error::FlightError>;
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

/// A2.1.2 (2026-05-28): broadcast counterpart to `async_stream`. Drains
/// a `BroadcastStream` (which yields `Option<RecordBatch>` via async
/// `.next()`) instead of an mpsc receiver. Same B-fix-3 failure
/// propagation semantics — after the stream EOFs, the shared `failure`
/// flag is checked and surfaced as a final `Err` frame if set.
fn async_stream_broadcast(
    schema: Arc<arrow::datatypes::Schema>,
    mut stream: crate::output_buffer::BroadcastStream,
    failure: std::sync::Arc<std::sync::Mutex<Option<String>>>,
) -> BoxedStream<FlightData> {
    use arrow_flight::encode::FlightDataEncoderBuilder;

    let batch_stream = async_stream::stream! {
        while let Some(batch) = stream.next().await {
            yield Ok(batch) as Result<arrow::array::RecordBatch, arrow_flight::error::FlightError>;
        }
        if let Some(msg) = failure.lock().ok().and_then(|mut g| g.take()) {
            yield Err(arrow_flight::error::FlightError::ProtocolError(format!(
                "producer task failed: {msg}"
            ))) as Result<arrow::array::RecordBatch, arrow_flight::error::FlightError>;
        }
    };

    let encoder = FlightDataEncoderBuilder::new()
        .with_schema(schema)
        .build(batch_stream);

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
        .add_service(
            FlightServiceServer::new(service)
                .max_decoding_message_size(crate::MAX_FLIGHT_MESSAGE_BYTES)
                .max_encoding_message_size(crate::MAX_FLIGHT_MESSAGE_BYTES),
        )
        .serve(addr)
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]))
    }

    #[test]
    fn flight_state_register_and_get() {
        let state = FlightState::new();
        let schema = test_schema();
        let buf = OutputBuffer::single(32, schema);
        state.register_partitioned_buffer("task-1".into(), buf);
        assert!(state.get_buffer("task-1").is_some());
        assert!(state.get_buffer("task-2").is_none());
    }

    #[test]
    fn flight_state_remove_buffer() {
        let state = FlightState::new();
        let schema = test_schema();
        let buf = OutputBuffer::single(32, schema);
        state.register_partitioned_buffer("task-1".into(), buf);
        state.remove_buffer("task-1");
        assert!(state.get_buffer("task-1").is_none());
    }

    // ---- D1 liveness tests ----

    /// Spin up a Flight server + ExchangeClient on a random port and
    /// return (client, state, port). Helper for D1 integration tests.
    async fn start_test_server(
        state: FlightState,
    ) -> (
        crate::ExchangeClient,
        FlightState,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let state_for_server = state.clone();
        let handle = tokio::spawn(async move {
            let service = TrinoFlightService::new(state_for_server);
            tonic::transport::Server::builder()
                .add_service(FlightServiceServer::new(service))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let client = crate::ExchangeClient::new(format!("http://127.0.0.1:{port}"));
        (client, state, handle)
    }

    /// Helper: call do_get on a TrinoFlightService and expect an error.
    async fn do_get_expect_err(service: &TrinoFlightService, ticket: &str) -> Status {
        let req = Request::new(Ticket::new(ticket.to_string()));
        match service.do_get(req).await {
            Err(e) => e,
            Ok(_) => panic!("expected do_get error"),
        }
    }

    #[tokio::test]
    async fn d1_do_get_returns_unavailable_when_task_running() {
        let mut state = FlightState::new();
        state.set_task_status_callback(Arc::new(|_task_id| Some(TaskStatusResponse::Running)));
        let service = TrinoFlightService::new(state);
        let err = do_get_expect_err(&service, "somequery.task-1:0").await;
        assert_eq!(err.code(), tonic::Code::Unavailable);
        assert!(err.message().contains("running"));
    }

    #[tokio::test]
    async fn d1_do_get_returns_internal_when_task_failed() {
        let mut state = FlightState::new();
        state.set_task_status_callback(Arc::new(|_task_id| {
            Some(TaskStatusResponse::Failed("OOM killed".into()))
        }));
        let service = TrinoFlightService::new(state);
        let err = do_get_expect_err(&service, "somequery.task-1:0").await;
        assert_eq!(err.code(), tonic::Code::Internal);
        assert!(err.message().contains("OOM killed"));
    }

    #[tokio::test]
    async fn d1_do_get_returns_not_found_when_task_unknown() {
        let mut state = FlightState::new();
        state.set_task_status_callback(Arc::new(|_task_id| None));
        let service = TrinoFlightService::new(state);
        let err = do_get_expect_err(&service, "somequery.task-99:0").await;
        assert_eq!(err.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn d1_do_get_returns_not_found_without_callback() {
        let state = FlightState::new();
        let service = TrinoFlightService::new(state);
        let err = do_get_expect_err(&service, "somequery.task-1:0").await;
        assert_eq!(err.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn d1_exchange_client_fails_fast_on_task_failure() {
        let mut state = FlightState::new();
        state.set_task_status_callback(Arc::new(|_task_id| {
            Some(TaskStatusResponse::Failed("plan build error".into()))
        }));
        let (client, _state, handle) = start_test_server(state).await;
        let result = client.fetch_partition("somequery.task-1", 0).await;
        let err = match result {
            Err(e) => e,
            Ok(_) => panic!("expected error for failed task"),
        };
        let msg = format!("{err}");
        assert!(
            msg.contains("failed"),
            "expected 'failed' in error, got: {msg}"
        );
        handle.abort();
    }

    #[tokio::test]
    async fn d1_exchange_client_succeeds_after_delayed_buffer_registration() {
        let mut state = FlightState::new();
        let registered = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let reg_clone = registered.clone();
        state.set_task_status_callback(Arc::new(move |_task_id| {
            if reg_clone.load(std::sync::atomic::Ordering::Relaxed) {
                None
            } else {
                Some(TaskStatusResponse::Running)
            }
        }));

        let (client, state_ref, handle) = start_test_server(state).await;

        let schema = test_schema();
        let state_for_reg = state_ref.clone();
        let reg_for_spawn = registered.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            let mut buf = OutputBuffer::single(32, schema.clone());
            let sender = buf.take_senders().into_iter().next().unwrap();
            state_for_reg.register_partitioned_buffer("somequery.task-1".into(), buf);
            let batch = arrow::array::RecordBatch::try_new(
                schema,
                vec![Arc::new(arrow::array::Int32Array::from(vec![42]))],
            )
            .unwrap();
            sender.send(batch).await.unwrap();
            drop(sender);
            reg_for_spawn.store(true, std::sync::atomic::Ordering::Relaxed);
        });

        let stream = client.fetch_partition("somequery.task-1", 0).await.unwrap();
        use futures::TryStreamExt;
        let batches: Vec<_> = stream.try_collect().await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        handle.abort();
    }

    /// D4 regression test: a producer fills the OutputBuffer channel to
    /// capacity (simulating back-pressure), then a second "downstream"
    /// consumer connects and successfully drains — proving that the
    /// parked producer holds no resource that blocks the consumer's
    /// admission. This is the exact deadlock class from the 2026-05-23
    /// streaming refactor: producer blocked on full buffer + consumer
    /// awaiting admission → permanent stall. Phase A removed the
    /// semaphore, so this must always pass.
    #[tokio::test]
    async fn d4_producer_blocked_on_full_buffer_does_not_block_consumer() {
        let schema = test_schema();
        let cap = 2; // tiny capacity → fills fast

        let mut buf = OutputBuffer::new(1, cap, schema.clone());
        let sender = buf.take_senders().into_iter().next().unwrap();

        let mut state = FlightState::new();
        state.set_task_status_callback(Arc::new(|_| Some(TaskStatusResponse::Running)));
        state.register_partitioned_buffer("q.producer".into(), buf);

        let (client, _state, handle) = start_test_server(state).await;

        let make_batch = || {
            arrow::array::RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(arrow::array::Int32Array::from(vec![1]))],
            )
            .unwrap()
        };

        // Fill the channel to capacity (non-blocking).
        for _ in 0..cap {
            sender.send(make_batch()).await.unwrap();
        }

        // The producer is now effectively "parked" — the next send
        // would block. Spawn it so the test can proceed concurrently.
        let extra = make_batch();
        let sender_handle = tokio::spawn(async move {
            sender.send(extra).await.unwrap();
        });

        // "Consumer" connects WHILE the producer is blocked on the
        // full channel. This must succeed — no deadlock.
        let stream = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            client.fetch_partition("q.producer", 0),
        )
        .await
        .expect("consumer must not deadlock waiting for admission")
        .expect("fetch_partition must succeed");

        use futures::TryStreamExt;
        let batches: Vec<_> = stream.try_collect().await.unwrap();
        assert_eq!(
            batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            cap + 1,
            "consumer must receive all batches including the blocked one"
        );

        sender_handle.await.unwrap();
        handle.abort();
    }
}
