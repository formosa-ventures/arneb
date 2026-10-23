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
    Failed(crate::TaskFailure),
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

/// Callback for the `cancel_query` action: cancel every task of a query on
/// this node and return how many were aborted (admission-control D4).
pub type CancelQueryCallback = Arc<dyn Fn(arneb_common::QueryId) -> usize + Send + Sync>;

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
    /// Each registered buffer's failure flag, held outside the buffer so
    /// [`FlightState::fail_output`] needs no lock a `do_get` may hold.
    failures: Arc<RwLock<HashMap<String, FailureHandle>>>,
    heartbeat_callback: Option<HeartbeatCallback>,
    task_callback: Option<TaskCallback>,
    task_status_callback: Option<TaskStatusCallback>,
    df_report_callback: Option<DfReportCallback>,
    df_notify_callback: Option<DfNotifyCallback>,
    cancel_query_callback: Option<CancelQueryCallback>,
}

impl Default for FlightState {
    fn default() -> Self {
        Self {
            buffers: Arc::new(RwLock::new(HashMap::new())),
            failures: Arc::new(RwLock::new(HashMap::new())),
            heartbeat_callback: None,
            task_callback: None,
            task_status_callback: None,
            df_report_callback: None,
            df_notify_callback: None,
            cancel_query_callback: None,
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
            heartbeat_callback: Some(callback),
            ..Self::default()
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
    /// Install the `cancel_query` handler (workers).
    pub fn set_cancel_query_callback(&mut self, callback: CancelQueryCallback) {
        self.cancel_query_callback = Some(callback);
    }

    pub fn set_df_notify_callback(&mut self, callback: DfNotifyCallback) {
        self.df_notify_callback = Some(callback);
    }

    /// Register a partitioned output buffer for a task. A2.1.2 renamed
    /// from `register_buffer`; the broadcast equivalent is
    /// `register_broadcast_buffer`.
    pub fn register_partitioned_buffer(&self, task_id: String, buffer: OutputBuffer) {
        self.failures
            .write()
            .unwrap()
            .insert(task_id.clone(), buffer.failure_handle());
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
        self.failures
            .write()
            .unwrap()
            .insert(task_id.clone(), buffer.failure_handle());
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
        self.failures.write().unwrap().remove(task_id);
    }

    /// Mark task `task_id`'s output failed unless it already is, so its
    /// consumers end on an error frame instead of a clean EOF. Takes no
    /// buffer lock, so a cancel can't miss a partitioned buffer a `do_get`
    /// holds (#156). `false` when no buffer is registered.
    pub fn fail_output(&self, task_id: &str, failure: impl FnOnce() -> crate::TaskFailure) -> bool {
        let Some(handle) = self.failures.read().unwrap().get(task_id).cloned() else {
            return false;
        };
        handle
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_or_insert_with(failure);
        true
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

type FailureHandle = Arc<std::sync::Mutex<Option<crate::TaskFailure>>>;

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
        let peer = request.remote_addr();
        let ticket = request.into_inner();
        let ticket_str = String::from_utf8(ticket.ticket.to_vec())
            .map_err(|_| Status::invalid_argument("invalid ticket encoding"))?;

        // `task_id:partition_id`, or `task_id:partition_id:from_row` for a
        // consumer resuming a broken broadcast read (#139).
        let parts: Vec<&str> = ticket_str.split(':').collect();
        if !(2..=3).contains(&parts.len()) {
            return Err(Status::invalid_argument(
                "ticket format: task_id:partition_id[:from_row]",
            ));
        }
        let task_id = parts[0];
        let partition_id: usize = parts[1]
            .parse()
            .map_err(|_| Status::invalid_argument("invalid partition_id"))?;
        let from_row: usize = match parts.get(2) {
            Some(n) => n
                .parse()
                .map_err(|_| Status::invalid_argument("invalid from_row"))?,
            None => 0,
        };

        match self.state.get_buffer(task_id) {
            Some(BufferKind::Partitioned(_)) if from_row > 0 => Err(Status::failed_precondition(
                format!("partitioned output of task '{task_id}' can't be resumed"),
            )),
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
                Ok(Response::new(ServedStream::wrap(
                    stream,
                    ticket_str.clone(),
                    "partitioned",
                    peer,
                )))
            }
            Some(BufferKind::Broadcast(buffer)) => {
                // A2.1.2: each `do_get` subscribes a fresh independent
                // `BroadcastStream` — multiple consumer tasks can pull
                // the same full output set without coordination.
                // `partition_id` is informational only (kept in the
                // ticket format for protocol uniformity).
                let _ = partition_id;
                let schema = buffer.schema();
                let stream = buffer.subscribe_from_row(from_row);
                let failure = buffer.failure_handle();
                let flight_stream = async_stream_broadcast(schema, stream, failure);
                Ok(Response::new(ServedStream::wrap(
                    flight_stream,
                    ticket_str.clone(),
                    "broadcast",
                    peer,
                )))
            }
            None => {
                if let Some(ref cb) = self.state.task_status_callback {
                    match cb(task_id) {
                        Some(TaskStatusResponse::Running) => {
                            return Err(Status::unavailable(format!(
                                "task '{task_id}' running, buffer not yet registered"
                            )));
                        }
                        Some(TaskStatusResponse::Failed(failure)) => {
                            return Err(failure.to_status(format_args!("task '{task_id}' failed")));
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
            "cancel_query" => {
                let query_id = std::str::from_utf8(&action.body)
                    .ok()
                    .and_then(|s| uuid::Uuid::parse_str(s.trim()).ok())
                    .map(arneb_common::QueryId)
                    .ok_or_else(|| Status::invalid_argument("bad cancel_query body"))?;
                // Idempotent: an unknown query (already finished or never
                // here) cancels nothing and is still acknowledged.
                let aborted = self
                    .state
                    .cancel_query_callback
                    .as_ref()
                    .map_or(0, |cb| cb(query_id));
                let result = arrow_flight::Result {
                    body: format!("cancelled {aborted}").into(),
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

/// A `do_get` response stream that records how far it got. Dropped before
/// its end — the connection went away, or the consumer stopped reading —
/// it counts on `arneb_exchange_streams_dropped_total` and logs one line on
/// `arneb::exchange`. Read with the consumer's "exchange stream broke"
/// warning, it tells which side ended a broken stream (#139).
struct ServedStream {
    inner: BoxedStream<FlightData>,
    ticket: String,
    kind: &'static str,
    peer: Option<std::net::SocketAddr>,
    opened: std::time::Instant,
    messages: u64,
    bytes: u64,
    ended: bool,
}

impl ServedStream {
    fn wrap(
        inner: BoxedStream<FlightData>,
        ticket: String,
        kind: &'static str,
        peer: Option<std::net::SocketAddr>,
    ) -> BoxedStream<FlightData> {
        Box::pin(Self {
            inner,
            ticket,
            kind,
            peer,
            opened: std::time::Instant::now(),
            messages: 0,
            bytes: 0,
            ended: false,
        })
    }
}

impl Stream for ServedStream {
    type Item = Result<FlightData, Status>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let polled = self.inner.as_mut().poll_next(cx);
        match &polled {
            std::task::Poll::Ready(Some(Ok(data))) => {
                self.messages += 1;
                self.bytes += (data.data_header.len() + data.data_body.len()) as u64;
            }
            // An error frame is the producer's own end of the stream.
            std::task::Poll::Ready(Some(Err(_)) | None) => self.ended = true,
            std::task::Poll::Pending => {}
        }
        polled
    }
}

impl Drop for ServedStream {
    fn drop(&mut self) {
        if self.ended {
            return;
        }
        arneb_common::metrics::exchange_stream_dropped_early();
        tracing::info!(
            target: "arneb::exchange",
            ticket = %self.ticket,
            kind = self.kind,
            peer = ?self.peer,
            messages = self.messages,
            bytes = self.bytes,
            age_ms = self.opened.elapsed().as_millis() as u64,
            "exchange response dropped before its end"
        );
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
    failure: std::sync::Arc<std::sync::Mutex<Option<crate::TaskFailure>>>,
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
        if let Some(failure) = failure.lock().ok().and_then(|mut g| g.take()) {
            // The status (and so the error class) reaches the consumer as is.
            yield Err(arrow_flight::error::FlightError::Tonic(Box::new(
                failure.to_status("producer task failed"),
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
        result.map_err(crate::task_error::stream_error_status)
    });

    Box::pin(mapped)
}

/// A2.1.2 (2026-05-28): broadcast counterpart to `async_stream`. Drains
/// a `BroadcastStream` (which yields `Option<RecordBatch>` via async
/// `.next()`) instead of an mpsc receiver. Same B-fix-3 failure
/// propagation semantics — after the stream EOFs, the shared `failure`
/// flag is checked and surfaced as a final `Err` frame if set. The flag
/// is read, not taken: every reader of a broadcast (each consumer task, a
/// resumed read) must see the failure.
fn async_stream_broadcast(
    schema: Arc<arrow::datatypes::Schema>,
    mut stream: crate::output_buffer::BroadcastStream,
    failure: std::sync::Arc<std::sync::Mutex<Option<crate::TaskFailure>>>,
) -> BoxedStream<FlightData> {
    use arrow_flight::encode::FlightDataEncoderBuilder;

    let batch_stream = async_stream::stream! {
        while let Some(batch) = stream.next().await {
            yield Ok(batch) as Result<arrow::array::RecordBatch, arrow_flight::error::FlightError>;
        }
        let failure = failure.lock().ok().and_then(|g| g.clone());
        if let Some(failure) = failure {
            // The status (and so the error class) reaches the consumer as is.
            yield Err(arrow_flight::error::FlightError::Tonic(Box::new(
                failure.to_status("producer task failed"),
            ))) as Result<arrow::array::RecordBatch, arrow_flight::error::FlightError>;
        } else if stream.rows_short() > 0 {
            // A resume past the end: the consumer already holds rows this
            // buffer doesn't, so a clean EOF would hide a mismatch.
            yield Err(arrow_flight::error::FlightError::Tonic(Box::new(Status::out_of_range(
                format!(
                    "resume past the end of the broadcast output ({} rows short)",
                    stream.rows_short()
                ),
            )))) as Result<arrow::array::RecordBatch, arrow_flight::error::FlightError>;
        }
    };

    let encoder = FlightDataEncoderBuilder::new()
        .with_schema(schema)
        .build(batch_stream);

    #[allow(clippy::result_large_err)]
    let mapped = futures::StreamExt::map(encoder, |result| {
        result.map_err(crate::task_error::stream_error_status)
    });

    Box::pin(mapped)
}

/// Start the Arrow Flight server on the given address, secured with the
/// installed [`crate::ClusterSecurity`].
pub async fn start_flight_server(
    addr: &str,
    state: FlightState,
) -> Result<(), Box<dyn std::error::Error>> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    serve_flight(listener, state, crate::cluster_security()).await
}

/// Serve Flight on an already-bound listener with explicit security: the
/// bearer-secret check runs before every handler, and `[cluster.tls]`
/// adds TLS.
pub async fn serve_flight(
    listener: tokio::net::TcpListener,
    state: FlightState,
    security: &crate::ClusterSecurity,
) -> Result<(), Box<dyn std::error::Error>> {
    let addr = listener.local_addr()?;
    tracing::info!(
        %addr,
        cluster_auth = if security.secret.is_some() { "secret" } else { "off" },
        cluster_tls = security.tls.is_some(),
        "starting Arrow Flight server"
    );
    let service = FlightServiceServer::new(TrinoFlightService::new(state))
        .max_decoding_message_size(crate::MAX_FLIGHT_MESSAGE_BYTES)
        .max_encoding_message_size(crate::MAX_FLIGHT_MESSAGE_BYTES);
    let check = crate::security::SecretCheck::new(security.secret.as_ref());
    let mut builder = Server::builder();
    if let Some(tls) = security.server_tls() {
        builder = builder.tls_config(tls)?;
    }
    builder
        .add_service(tonic::service::interceptor::InterceptedService::new(
            service, check,
        ))
        .serve_with_incoming(flight_incoming(listener))
        .await?;
    Ok(())
}

/// Accepted connections for a Flight server on an already-bound listener,
/// with `TCP_NODELAY` set. `serve_with_incoming` ignores the builder's
/// `tcp_nodelay` (on by default for `serve(addr)`), and Flight's small
/// request/response frames otherwise stall on Nagle + delayed ACK.
pub(crate) fn flight_incoming(
    listener: tokio::net::TcpListener,
) -> tonic::transport::server::TcpIncoming {
    tonic::transport::server::TcpIncoming::from(listener).with_nodelay(Some(true))
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::datatypes::{DataType, Field, Schema};

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]))
    }

    /// `flight_incoming` sets `TCP_NODELAY` on every accepted socket.
    #[tokio::test]
    async fn accepted_flight_connections_disable_nagle() {
        use futures::StreamExt;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mut incoming = flight_incoming(listener);
        let _client = tokio::net::TcpStream::connect(addr).await.unwrap();
        let accepted = incoming.next().await.unwrap().unwrap();
        assert!(accepted.nodelay().unwrap());
    }

    fn served(frames: usize, error_last: bool) -> ServedStream {
        let mut items: Vec<Result<FlightData, Status>> = (0..frames)
            .map(|_| {
                Ok(FlightData {
                    data_body: vec![0u8; 8].into(),
                    ..Default::default()
                })
            })
            .collect();
        if error_last {
            items.push(Err(Status::internal("producer task failed")));
        }
        ServedStream {
            inner: Box::pin(futures::stream::iter(items)),
            ticket: "q.t:0".into(),
            kind: "partitioned",
            peer: None,
            opened: std::time::Instant::now(),
            messages: 0,
            bytes: 0,
            ended: false,
        }
    }

    /// A response read to its end, or to the producer's error frame, is not
    /// an early drop; one dropped part-way is, with what it had sent.
    #[tokio::test]
    async fn served_stream_tracks_whether_it_reached_its_end() {
        use futures::StreamExt;
        let mut full = served(3, false);
        while full.next().await.is_some() {}
        assert!(full.ended);
        assert_eq!((full.messages, full.bytes), (3, 24));

        let mut failed = served(1, true);
        while failed.next().await.is_some() {}
        assert!(failed.ended);

        let mut partial = served(3, false);
        partial.next().await.unwrap().unwrap();
        assert!(!partial.ended);
        assert_eq!(partial.messages, 1);
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

    /// #156: a cancel flags a partitioned buffer even while a `do_get`
    /// holds its lock, and the first failure recorded wins.
    #[tokio::test]
    async fn fail_output_flags_a_locked_partitioned_buffer() {
        let state = FlightState::new();
        state.register_partitioned_buffer("q.t".into(), OutputBuffer::single(4, test_schema()));
        let Some(BufferKind::Partitioned(buf)) = state.get_buffer("q.t") else {
            panic!("partitioned buffer missing");
        };
        let held = buf.lock().await;
        assert!(state.fail_output("q.t", || "query cancelled".into()));
        assert!(state.fail_output("q.t", || "later".into()));
        let failure = held.failure_handle().lock().unwrap().clone().unwrap();
        assert!(failure.to_status("x").message().contains("query cancelled"));
        drop(held);

        state.remove_buffer("q.t");
        assert!(!state.fail_output("q.t", || "gone".into()));
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
        let (addr, handle) = start_server_at(state.clone()).await;
        let client = crate::ExchangeClient::new(format!("http://{addr}"));
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

    /// Where [`proxy`] cuts its first connection.
    #[derive(Clone, Copy)]
    enum Cut {
        /// After forwarding this many bytes from the server.
        AfterBytes(usize),
        /// Just before the server's first h2 HEADERS frame: the request
        /// reached the server but its response never arrives.
        BeforeResponseHeaders,
    }

    /// #139: a TCP proxy to `target` that cuts its first connection after
    /// forwarding `cut_after` bytes from the server (no GOAWAY, like the
    /// h2 `BrokenPipe` seen under load). Later connections pass through.
    async fn cutting_proxy(target: std::net::SocketAddr, cut_after: usize) -> String {
        proxy(target, Cut::AfterBytes(cut_after)).await
    }

    async fn proxy(target: std::net::SocketAddr, cut: Cut) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let mut first = true;
            loop {
                let (inbound, _) = listener.accept().await.unwrap();
                let cut = std::mem::take(&mut first).then_some(cut);
                tokio::spawn(async move {
                    let outbound = tokio::net::TcpStream::connect(target).await.unwrap();
                    let (mut client_read, mut client_write) = inbound.into_split();
                    let (mut server_read, mut server_write) = outbound.into_split();
                    let upstream = tokio::io::copy(&mut client_read, &mut server_write);
                    let downstream = async {
                        let mut sent = 0usize;
                        // Server-to-client bytes seen, for finding h2 frame
                        // boundaries (the server sends no preface).
                        let mut seen = Vec::new();
                        let mut next_frame = 0usize;
                        let mut buf = vec![0u8; 16 * 1024];
                        loop {
                            let n = server_read.read(&mut buf).await.unwrap_or(0);
                            if n == 0 {
                                return;
                            }
                            let limit = match cut {
                                Some(Cut::AfterBytes(limit)) => Some(limit),
                                Some(Cut::BeforeResponseHeaders) => {
                                    seen.extend_from_slice(&buf[..n]);
                                    let mut at = None;
                                    while next_frame + 9 <= seen.len() {
                                        let h = &seen[next_frame..next_frame + 9];
                                        if h[3] == 0x1 {
                                            at = Some(next_frame);
                                            break;
                                        }
                                        let len = (usize::from(h[0]) << 16)
                                            | (usize::from(h[1]) << 8)
                                            | usize::from(h[2]);
                                        next_frame += 9 + len;
                                    }
                                    at
                                }
                                None => None,
                            };
                            let n = match limit {
                                Some(limit) if sent + n >= limit => {
                                    let _ = client_write.write_all(&buf[..limit - sent]).await;
                                    return;
                                }
                                _ => n,
                            };
                            if client_write.write_all(&buf[..n]).await.is_err() {
                                return;
                            }
                            sent += n;
                        }
                    };
                    tokio::select! {
                        _ = upstream => {}
                        _ = downstream => {}
                    }
                });
            }
        });
        format!("http://localhost:{}", addr.port())
    }

    async fn start_server_at(
        state: FlightState,
    ) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
        start_secured_server_at(state, crate::ClusterSecurity::default()).await
    }

    /// The production `serve_flight` on a random port. The listener is
    /// bound before the spawn, so a client can connect right away.
    async fn start_secured_server_at(
        state: FlightState,
        security: crate::ClusterSecurity,
    ) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            serve_flight(listener, state, &security).await.unwrap();
        });
        (addr, handle)
    }

    /// State with a finished `q.bcast` broadcast buffer holding `batches`
    /// numbered batches of `rows` rows (values `0..batches * rows`).
    fn broadcast_state(batches: i32, rows: i32) -> FlightState {
        let schema = test_schema();
        let buffer = Arc::new(BroadcastOutputBuffer::new(schema.clone()));
        for b in 0..batches {
            buffer.write_batch(numbered_batch(&schema, b, rows));
        }
        buffer.finish();
        let state = FlightState::new();
        state.register_broadcast_buffer("q.bcast".into(), buffer);
        state
    }

    /// Read `ticket` to the end through `client`, failing a hang after 20 s.
    async fn read_all(
        client: &crate::ExchangeClient,
        ticket: &str,
    ) -> Result<Vec<arrow::array::RecordBatch>, arneb_common::error::ArnebError> {
        use futures::TryStreamExt;
        let (task, partition) = ticket.rsplit_once(':').unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(20), async {
            client
                .fetch_partition(task, partition.parse().unwrap())
                .await?
                .try_collect::<Vec<_>>()
                .await
        })
        .await
        .expect("no hang")
    }

    fn int_values(batches: &[arrow::array::RecordBatch]) -> Vec<i32> {
        batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::Int32Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect()
    }

    fn numbered_batch(schema: &Arc<Schema>, batch: i32, rows: i32) -> arrow::array::RecordBatch {
        let start = batch * rows;
        arrow::array::RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow::array::Int32Array::from_iter_values(
                start..start + rows,
            ))],
        )
        .unwrap()
    }

    /// #139: a broadcast read that breaks mid-stream is reopened and
    /// resumes after the rows already delivered: every row arrives exactly
    /// once, in order.
    #[tokio::test]
    async fn broadcast_read_resumes_after_connection_cut() {
        let (addr, handle) = start_server_at(broadcast_state(200, 1000)).await;
        // ~800 KB in total; cut at 300 KB (not on a batch boundary).
        let client = crate::ExchangeClient::new(cutting_proxy(addr, 300_007).await);

        let batches = read_all(&client, "q.bcast:0")
            .await
            .expect("resumed read succeeds");
        assert_eq!(int_values(&batches), (0..200_000).collect::<Vec<i32>>());
        handle.abort();
    }

    /// The client counts the rows of decoded batches, but the Flight
    /// encoder splits a batch above its message size (2 MiB) into several,
    /// so a resume can start inside a producer batch: the server slices it.
    /// 3 batches of 1 M rows (4 MB, two 500 k-row messages each); the cut at
    /// 7 MB lands after the second batch's first message, so the resume
    /// starts at row 1.5 M, inside that batch.
    #[tokio::test]
    async fn broadcast_resume_inside_a_producer_batch() {
        let (addr, handle) = start_server_at(broadcast_state(3, 1_000_000)).await;
        let client = crate::ExchangeClient::new(cutting_proxy(addr, 7_000_007).await);

        let batches = read_all(&client, "q.bcast:0")
            .await
            .expect("resumed read succeeds");
        assert!(batches.len() > 3, "the encoder split the batches");
        assert_eq!(int_values(&batches), (0..3_000_000).collect::<Vec<i32>>());
        handle.abort();
    }

    /// #139 with cluster security: the reopen goes through the same
    /// bearer secret and TLS as the first `do_get`.
    #[tokio::test]
    async fn broadcast_resume_keeps_cluster_security() {
        let ca_key = rcgen::KeyPair::generate().unwrap();
        let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let ca = ca_params.self_signed(&ca_key).unwrap();
        let issuer = rcgen::Issuer::new(ca_params, ca_key);
        let key = rcgen::KeyPair::generate().unwrap();
        let cert = rcgen::CertificateParams::new(vec!["localhost".to_owned()])
            .unwrap()
            .signed_by(&key, &issuer)
            .unwrap();
        let security = crate::ClusterSecurity {
            secret: Some(crate::ClusterSecret::new("s".repeat(40)).unwrap()),
            tls: Some(crate::ClusterTls {
                cert_pem: cert.pem().into_bytes(),
                key_pem: key.serialize_pem().into_bytes(),
                ca_pem: ca.pem().into_bytes(),
            }),
        };

        let (addr, handle) =
            start_secured_server_at(broadcast_state(200, 1000), security.clone()).await;
        let client =
            crate::ExchangeClient::new(cutting_proxy(addr, 300_007).await).with_security(security);

        let batches = read_all(&client, "q.bcast:0")
            .await
            .expect("resumed read succeeds over TLS with the secret");
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 200_000);
        handle.abort();
    }

    /// #139: a partitioned stream is consumed once, so a cut can't be
    /// resumed: the reopen is refused and the original transport error
    /// surfaces (no duplicate rows, no hang).
    #[tokio::test]
    async fn partitioned_read_cut_surfaces_the_transport_error() {
        let schema = test_schema();
        let mut buf = OutputBuffer::single(256, schema.clone());
        let sender = buf.take_senders().into_iter().next().unwrap();
        let state = FlightState::new();
        state.register_partitioned_buffer("q.part".into(), buf);
        let producer_schema = schema.clone();
        let producer = tokio::spawn(async move {
            for b in 0..200 {
                if sender
                    .send(numbered_batch(&producer_schema, b, 1000))
                    .await
                    .is_err()
                {
                    return;
                }
            }
        });
        let (addr, handle) = start_server_at(state).await;
        let client = crate::ExchangeClient::new(cutting_proxy(addr, 300_007).await);

        use futures::StreamExt;
        let mut stream = client.fetch_partition("q.part", 0).await.unwrap();
        let outcome = tokio::time::timeout(std::time::Duration::from_secs(20), async {
            let mut rows = 0usize;
            while let Some(item) = stream.next().await {
                match item {
                    Ok(b) => rows += b.num_rows(),
                    Err(e) => return (rows, Some(e.to_string())),
                }
            }
            (rows, None)
        })
        .await
        .expect("no hang");
        let (rows, err) = outcome;
        let err = err.expect("the cut surfaces as an error");
        assert!(err.contains("Flight stream error"), "{err}");
        assert!(rows < 200_000, "rows={rows}");
        producer.abort();
        handle.abort();
    }

    /// #140 review: a break before the response headers fails the `do_get`
    /// call itself (no stream yet). A broadcast ticket is reconnected and
    /// read in full.
    #[tokio::test]
    async fn broadcast_do_get_retries_a_break_before_the_response() {
        let (addr, handle) = start_server_at(broadcast_state(10, 100)).await;
        let client = crate::ExchangeClient::new(proxy(addr, Cut::BeforeResponseHeaders).await);

        let batches = read_all(&client, "q.bcast:0")
            .await
            .expect("do_get retried after the break");
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 1000);
        handle.abort();
    }

    /// The same break on a partitioned ticket: the server already handed
    /// the buffer to the lost response, so the retry is refused and the
    /// original transport error surfaces instead of `AlreadyExists`.
    #[tokio::test]
    async fn partitioned_do_get_break_surfaces_the_transport_error() {
        let schema = test_schema();
        let mut buf = OutputBuffer::single(256, schema.clone());
        let sender = buf.take_senders().into_iter().next().unwrap();
        let state = FlightState::new();
        state.register_partitioned_buffer("q.part".into(), buf);
        sender.send(numbered_batch(&schema, 0, 100)).await.unwrap();
        drop(sender);
        let (addr, handle) = start_server_at(state).await;
        let client = crate::ExchangeClient::new(proxy(addr, Cut::BeforeResponseHeaders).await);

        let err = tokio::time::timeout(
            std::time::Duration::from_secs(20),
            client.fetch_partition("q.part", 0),
        )
        .await
        .expect("no hang")
        .err()
        .expect("the break surfaces as an error")
        .to_string();
        assert!(err.contains("Flight do_get failed"), "{err}");
        assert!(
            !err.contains("AlreadyExists") && !err.contains("already"),
            "{err}"
        );
        handle.abort();
    }

    /// A resume ticket (`task:partition:from_row`) makes the broadcast
    /// producer send only the remainder, with the partial batch sliced.
    #[tokio::test]
    async fn broadcast_do_get_from_row_sends_only_the_remainder() {
        let (addr, handle) = start_server_at(broadcast_state(10, 100)).await;
        let mut client = crate::security::connect(
            &format!("http://{addr}"),
            &crate::ClusterSecurity::default(),
        )
        .await
        .unwrap();

        use futures::TryStreamExt;
        let response = client
            .do_get(Ticket::new("q.bcast:0:250"))
            .await
            .unwrap()
            .into_inner();
        let batches: Vec<_> = arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(
            response.map_err(|e| arrow_flight::error::FlightError::Tonic(Box::new(e))),
        )
        .try_collect()
        .await
        .unwrap();
        assert_eq!(int_values(&batches), (250..1000).collect::<Vec<i32>>());

        let err = client.do_get(Ticket::new("q.bcast:0:x")).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);

        // A resume past the end is an error, not a clean EOF.
        let response = client
            .do_get(Ticket::new("q.bcast:0:1250"))
            .await
            .unwrap()
            .into_inner();
        let err = arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(
            response.map_err(|e| arrow_flight::error::FlightError::Tonic(Box::new(e))),
        )
        .try_collect::<Vec<_>>()
        .await
        .unwrap_err()
        .to_string();
        assert!(err.contains("250 rows short"), "{err}");
        handle.abort();
    }

    /// A broadcast buffer has many readers: every consumer task, and a
    /// resumed read (#139) whose first reader's error frame was lost. A
    /// producer failure must end every one of them, not only the first to
    /// reach EOF; otherwise a later reader takes a clean EOF and builds
    /// from a truncated broadcast.
    #[tokio::test]
    async fn every_broadcast_reader_sees_the_producer_failure() {
        let schema = test_schema();
        let buffer = Arc::new(BroadcastOutputBuffer::new(schema.clone()));
        buffer.write_batch(numbered_batch(&schema, 0, 100));
        *buffer.failure_handle().lock().unwrap() = Some(crate::TaskFailure::new("scan failed"));
        buffer.finish();
        let state = FlightState::new();
        state.register_broadcast_buffer("q.bcast".into(), buffer);
        let (addr, handle) = start_server_at(state).await;
        let client = crate::ExchangeClient::new(format!("http://{addr}"));

        for (reader, ticket) in [(1, "q.bcast:0"), (2, "q.bcast:0"), (3, "q.bcast:0:50")] {
            let err = if ticket.matches(':').count() == 1 {
                read_all(&client, ticket).await.err().map(|e| e.to_string())
            } else {
                let mut raw = crate::security::connect(
                    &format!("http://{addr}"),
                    &crate::ClusterSecurity::default(),
                )
                .await
                .unwrap();
                use futures::TryStreamExt;
                let response = raw.do_get(Ticket::new(ticket)).await.unwrap().into_inner();
                arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(
                    response.map_err(|e| arrow_flight::error::FlightError::Tonic(Box::new(e))),
                )
                .try_collect::<Vec<_>>()
                .await
                .err()
                .map(|e| e.to_string())
            };
            let err = err.unwrap_or_else(|| panic!("reader {reader} got a clean EOF"));
            assert!(err.contains("scan failed"), "reader {reader}: {err}");
        }
        handle.abort();
    }

    /// A partitioned buffer is consumed once, so a resume ticket is refused
    /// without taking the buffer.
    #[tokio::test]
    async fn partitioned_do_get_refuses_a_resume_ticket() {
        let schema = test_schema();
        let buf = OutputBuffer::single(4, schema.clone());
        let state = FlightState::new();
        state.register_partitioned_buffer("q.part".into(), buf);
        let service = TrinoFlightService::new(state);
        let err = do_get_expect_err(&service, "q.part:0:10").await;
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        // The buffer was not taken: a normal read still gets it.
        assert!(service
            .do_get(Request::new(Ticket::new("q.part:0")))
            .await
            .is_ok());
    }
}
