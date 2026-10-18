//! Exchange client for fetching RecordBatches from remote Flight servers.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arneb_common::error::{ArnebError, ExecutionError};
use arneb_common::memory_profile::{record_live_alloc, record_live_free, LiveBytesGuard};
use arneb_common::stream::{stream_from_batches, RecordBatchStream, SendableRecordBatchStream};
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::Ticket;
use futures::stream::Stream;
use futures::TryStreamExt;

use crate::security::{cluster_security, ClusterSecurity};

/// Decoded batches of one `do_get` response.
type FlightBatchStream = Pin<Box<dyn Stream<Item = Result<RecordBatch, FlightError>> + Send>>;

/// How many times a stream that broke on a transport error is reopened
/// before the error is surfaced (#139).
const MAX_RESUME_ATTEMPTS: usize = 3;
/// Client that fetches RecordBatches from a remote Arrow Flight server.
#[derive(Debug, Clone)]
pub struct ExchangeClient {
    /// Address of the remote Flight server (e.g., "http://host:9090").
    address: String,
    /// Explicit cluster credentials; `None` uses the process-wide
    /// installed ones. Shared, so cloning the client per stream copies no
    /// TLS material.
    security: Option<Arc<ClusterSecurity>>,
}

impl ExchangeClient {
    /// Creates a new exchange client targeting the given address.
    pub fn new(address: impl Into<String>) -> Self {
        Self {
            address: address.into(),
            security: None,
        }
    }

    /// Use explicit cluster security instead of the installed one.
    pub fn with_security(mut self, security: ClusterSecurity) -> Self {
        self.security = Some(Arc::new(security));
        self
    }

    /// Fetch a partition from a remote task as a stream of RecordBatches.
    ///
    /// Retries with exponential backoff (50 ms → 2 s cap). The worker's
    /// `do_get` returns one of three statuses when the OutputBuffer is
    /// not yet registered:
    ///
    /// - **Unavailable** — task running, buffer not ready. Keep retrying
    ///   with no fixed deadline (a slow stage at SF100 legitimately takes
    ///   a long time). A configurable safety ceiling (default 1 hour)
    ///   catches genuinely hung tasks.
    /// - **NotFound** — task ID unknown on this worker (never submitted
    ///   or already cleaned up). Retry for a short grace period (60 s)
    ///   to cover dispatch-to-start latency, then fail.
    /// - **Internal** — task failed before producing output. Fail
    ///   immediately with the worker's error message.
    ///
    /// A transport break before the response arrives (`Unknown`, #139)
    /// reconnects and asks again, up to [`MAX_RESUME_ATTEMPTS`] times. A
    /// partitioned buffer the lost response already took answers the
    /// retry with `AlreadyExists`; the transport error is reported then.
    pub async fn fetch_partition(
        &self,
        task_id: &str,
        partition_id: usize,
    ) -> Result<SendableRecordBatchStream, ExecutionError> {
        let ticket_text = format!("{task_id}:{partition_id}");
        let mut backoff = std::time::Duration::from_millis(50);
        let backoff_max = std::time::Duration::from_secs(2);

        // Grace period for the NotFound case (task not yet started on
        // the worker). Covers task submission → handle_task latency.
        let not_found_deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(60);

        // Safety ceiling for the Unavailable case (task alive but
        // slow). Default 1 hour — much larger than any legitimate stage
        // but catches a genuinely hung process. Override via
        // ARNEB_EXCHANGE_TIMEOUT_SECS for experiments.
        let safety_secs: u64 = std::env::var("ARNEB_EXCHANGE_TIMEOUT_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(3600);
        let safety_deadline =
            tokio::time::Instant::now() + std::time::Duration::from_secs(safety_secs);

        let mut client = self.connect().await?;
        let mut transport_attempts = 0usize;
        let mut transport_error: Option<tonic::Status> = None;

        loop {
            let ticket = Ticket::new(ticket_text.clone());

            let response = match client.do_get(ticket).await {
                Ok(resp) => resp,

                // Task running, buffer not ready — keep waiting.
                Err(status) if status.code() == tonic::Code::Unavailable => {
                    if tokio::time::Instant::now() >= safety_deadline {
                        return Err(ExecutionError::InvalidOperation(format!(
                            "Flight do_get safety ceiling ({safety_secs}s) exceeded \
                             waiting for task {ticket_text} on {} \
                             (task alive but never produced output)",
                            self.address
                        )));
                    }
                    tokio::time::sleep(backoff).await;
                    backoff = std::cmp::min(backoff * 2, backoff_max);
                    continue;
                }

                // Task ID unknown — short grace period, then fail.
                Err(status) if status.code() == tonic::Code::NotFound => {
                    if tokio::time::Instant::now() >= not_found_deadline {
                        return Err(ExecutionError::InvalidOperation(format!(
                            "Flight do_get: task {ticket_text} unknown on {} \
                             after 60s (task never started or already cleaned up)",
                            self.address
                        )));
                    }
                    tokio::time::sleep(backoff).await;
                    backoff = std::cmp::min(backoff * 2, backoff_max);
                    continue;
                }

                // Task failed — propagate the worker's error immediately,
                // keeping a memory-limit failure's class.
                Err(status)
                    if matches!(
                        status.code(),
                        tonic::Code::Internal | tonic::Code::ResourceExhausted
                    ) =>
                {
                    return Err(crate::task_error::remote_error(
                        format!("remote task {ticket_text} on {} failed", self.address),
                        status.message(),
                    ));
                }

                // The connection broke before the response: reconnect.
                Err(status)
                    if status.code() == tonic::Code::Unknown
                        && transport_attempts < MAX_RESUME_ATTEMPTS =>
                {
                    transport_attempts += 1;
                    tracing::warn!(
                        target: "arneb::exchange",
                        ticket = %ticket_text,
                        address = %self.address,
                        attempt = transport_attempts,
                        error = %status,
                        "exchange do_get broke before the response, reconnecting"
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(50 << transport_attempts))
                        .await;
                    if let Ok(reconnected) = self.connect().await {
                        client = reconnected;
                    }
                    transport_error.get_or_insert(status);
                    continue;
                }

                Err(e) => {
                    let e = transport_error.unwrap_or(e);
                    return Err(ExecutionError::InvalidOperation(format!(
                        "Flight do_get failed: {e}"
                    )));
                }
            };

            return self.clone().collect_response(response, ticket_text).await;
        }
    }

    /// A Flight connection with this client's cluster security (bearer
    /// secret, TLS), for the first `do_get` and for every reopen.
    async fn connect(&self) -> Result<crate::security::FlightClient, ExecutionError> {
        let security = self
            .security
            .as_deref()
            .unwrap_or_else(|| cluster_security());
        crate::security::connect(&self.address, security)
            .await
            .map_err(|e| ExecutionError::InvalidOperation(format!("Flight server: {e}")))
    }

    /// Open `ticket` again after its stream broke. `Err(true)`: worth
    /// another attempt (transport trouble); `Err(false)`: the output can't
    /// be read again (a partitioned buffer is consumed once, or is gone).
    async fn reopen(&self, ticket: &str) -> Result<FlightBatchStream, bool> {
        let mut client = self.connect().await.map_err(|_| true)?;
        match client.do_get(Ticket::new(ticket.to_owned())).await {
            Ok(response) => Ok(decode(response)),
            Err(status) => Err(is_transport_status(&status)),
        }
    }

    /// Wrap a response so a transport break mid-stream (#139) reopens the
    /// ticket and skips the rows already delivered. Only broadcast output
    /// can be read again: it replays the same batches in the same order to
    /// every `do_get`. A partitioned ticket answers the reopen with
    /// `AlreadyExists`, and the original error is surfaced as before.
    fn resumable(self, ticket: String, first: FlightBatchStream) -> FlightBatchStream {
        Box::pin(async_stream::stream! {
            use futures::StreamExt;
            let mut stream = first;
            let mut delivered = 0usize;
            let mut to_skip = 0usize;
            let mut attempts = 0usize;
            loop {
                match stream.next().await {
                    Some(Ok(batch)) => {
                        let rows = batch.num_rows();
                        if to_skip > 0 && to_skip >= rows {
                            to_skip -= rows;
                            continue;
                        }
                        let batch = if to_skip > 0 {
                            let rest = batch.slice(to_skip, rows - to_skip);
                            to_skip = 0;
                            rest
                        } else {
                            batch
                        };
                        delivered += batch.num_rows();
                        yield Ok(batch);
                    }
                    Some(Err(err)) if is_transport_error(&err) && attempts < MAX_RESUME_ATTEMPTS => {
                        loop {
                            attempts += 1;
                            tracing::warn!(
                                target: "arneb::exchange",
                                ticket = %ticket,
                                address = %self.address,
                                delivered_rows = delivered,
                                attempt = attempts,
                                error = %err,
                                "exchange stream broke, reopening"
                            );
                            tokio::time::sleep(std::time::Duration::from_millis(50 << attempts)).await;
                            match self.reopen(&ticket).await {
                                Ok(reopened) => {
                                    stream = reopened;
                                    to_skip = delivered;
                                    break;
                                }
                                Err(true) if attempts < MAX_RESUME_ATTEMPTS => continue,
                                Err(_) => {
                                    yield Err(err);
                                    return;
                                }
                            }
                        }
                    }
                    Some(Err(err)) => {
                        yield Err(err);
                        return;
                    }
                    None => {
                        if to_skip > 0 {
                            yield Err(FlightError::ProtocolError(format!(
                                "reopened stream for {ticket} ended {to_skip} rows short"
                            )));
                        }
                        return;
                    }
                }
            }
        })
    }

    async fn collect_response(
        self,
        response: tonic::Response<tonic::Streaming<arrow_flight::FlightData>>,
        ticket_text: String,
    ) -> Result<SendableRecordBatchStream, ExecutionError> {
        // [EXCHTRACE] (2026-06-12): consumer-side RECV counterpart to
        // async_stream's SERVE — gated by ARNEB_TRACE_FRAGMENTS. SERVE!=RECV
        // for a ticket = transport drop; RECV varying run-to-run = that
        // exchange is the q21 drop-point. See blast_radius memory.
        let traced = std::env::var("ARNEB_TRACE_FRAGMENTS")
            .map(|v| v != "0" && !v.is_empty())
            .unwrap_or(false);
        let mut flight_stream = self.resumable(ticket_text.clone(), decode(response));

        // Phase 3b.7a (2026-05-21): true streaming. Peek the first batch
        // only (so we have a schema before returning a typed stream), then
        // wrap the rest as a passthrough. Previously this collected the
        // entire remote partition into a `Vec<RecordBatch>` before
        // yielding, accumulating 50-100 MB per (task, upstream worker)
        // pair on the consumer side — defeating Phase 3b.4's producer-
        // side streaming and contributing ~1 GB of intermediate buffer
        // on Q09 workers.
        use futures::StreamExt;
        let first = match flight_stream.next().await {
            Some(Ok(batch)) => Some(batch),
            Some(Err(e)) => {
                return Err(crate::task_error::remote_error(
                    "Flight stream error".to_owned(),
                    &e.to_string(),
                ));
            }
            None => None,
        };

        let Some(first_batch) = first else {
            // Empty remote partition. Return an empty typed stream.
            if traced {
                eprintln!("[EXCHTRACE] RECV ticket={ticket_text} rows=0 batches=0 empty");
            }
            let schema = Arc::new(arrow::datatypes::Schema::empty());
            return Ok(stream_from_batches(schema, vec![]));
        };

        let schema = first_batch.schema();
        let first_live = Some(LiveBytesGuard::new(
            "ExchangeReceive.live",
            first_batch.get_array_memory_size() as u64,
        ));
        Ok(Box::pin(FlightPassthroughStream {
            schema,
            first: Some(first_batch),
            first_live,
            inner: flight_stream,
            ticket: ticket_text,
            traced,
            rows: 0,
            batches: 0,
            logged: false,
        }))
    }
}

/// `RecordBatchStream` adapter that yields one peeked batch followed
/// by passthrough of the underlying Arrow Flight stream. Used so the
/// consumer-side `ExchangeClient` doesn't accumulate the full remote
/// partition into a `Vec<RecordBatch>` before returning.
struct FlightPassthroughStream {
    schema: SchemaRef,
    first: Option<RecordBatch>,
    first_live: Option<LiveBytesGuard>,
    inner: FlightBatchStream,
    // [EXCHTRACE] per-ticket consumer-side row tally (gated by ARNEB_TRACE_FRAGMENTS).
    ticket: String,
    traced: bool,
    rows: usize,
    batches: usize,
    logged: bool,
}

impl Stream for FlightPassthroughStream {
    type Item = Result<RecordBatch, ArnebError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Drain the peeked first batch before polling the underlying stream.
        if let Some(b) = self.first.take() {
            self.rows += b.num_rows();
            self.batches += 1;
            self.first_live.take();
            return Poll::Ready(Some(Ok(b)));
        }
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(b))) => {
                let bytes = exchange_receive_batch_bytes(&b);
                record_live_alloc("ExchangeReceive.live", bytes);
                self.rows += b.num_rows();
                self.batches += 1;
                record_live_free("ExchangeReceive.live", bytes);
                Poll::Ready(Some(Ok(b)))
            }
            Poll::Ready(Some(Err(e))) => {
                // [EXCHTRACE] §1 measure-first: the inner Flight stream yielded an
                // ERROR mid-stream = a connection reset/abort (drop-trigger case
                // a/b: h2 idle-timeout / flow-control / tonic error), NOT a clean
                // lazy-drop. Distinguishes the trigger from the Drop-before-EOF case.
                if self.traced && !self.logged {
                    self.logged = true;
                    eprintln!(
                        "[EXCHTRACE] RECV_ERR ticket={} rows={} batches={} err={e}",
                        self.ticket, self.rows, self.batches
                    );
                }
                Poll::Ready(Some(Err(ArnebError::Execution(
                    crate::task_error::remote_error(
                        "Flight stream error".to_owned(),
                        &e.to_string(),
                    ),
                ))))
            }
            Poll::Ready(None) => {
                if self.traced && !self.logged {
                    self.logged = true;
                    eprintln!(
                        "[EXCHTRACE] RECV ticket={} rows={} batches={}",
                        self.ticket, self.rows, self.batches
                    );
                }
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

fn decode(
    response: tonic::Response<tonic::Streaming<arrow_flight::FlightData>>,
) -> FlightBatchStream {
    Box::pin(FlightRecordBatchStream::new_from_flight_data(
        response
            .into_inner()
            .map_err(|e| FlightError::Tonic(Box::new(e))),
    ))
}

/// A status the transport produced (connection closed or reset, h2
/// error), as opposed to one the producer sent: producer failures arrive
/// as `Internal` / `ResourceExhausted`.
fn is_transport_status(status: &tonic::Status) -> bool {
    matches!(
        status.code(),
        tonic::Code::Unknown | tonic::Code::Unavailable
    )
}

fn is_transport_error(err: &FlightError) -> bool {
    matches!(err, FlightError::Tonic(status) if is_transport_status(status))
}

fn exchange_receive_batch_bytes(batch: &RecordBatch) -> u64 {
    batch.get_array_memory_size() as u64
}

impl RecordBatchStream for FlightPassthroughStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Drop for FlightPassthroughStream {
    fn drop(&mut self) {
        // [EXCHTRACE] §1 measure-first: if this consumer-side stream is dropped
        // BEFORE reaching EOF or an error (`logged` still false), the consumer
        // stopped pulling and abandoned the stream — drop-trigger case c/d
        // (resource/admission cancellation, or a lazy `try_stream!` dropped by a
        // higher consumer). This is the silent path (no Err frame) that lets the
        // producer's receiver drop. Contrast with RECV (clean EOF) / RECV_ERR
        // (reset). Pinpoints whether the q21 stall ends in a reset or a lazy-drop.
        if self.traced && !self.logged {
            eprintln!(
                "[EXCHTRACE] RECV_DROP_MIDSTREAM ticket={} rows={} batches={} (consumer abandoned stream before EOF)",
                self.ticket, self.rows, self.batches
            );
        }
    }
}
