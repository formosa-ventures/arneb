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
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::Ticket;
use futures::stream::Stream;
use futures::TryStreamExt;
use tonic::transport::Channel;

/// Client that fetches RecordBatches from a remote Arrow Flight server.
#[derive(Debug, Clone)]
pub struct ExchangeClient {
    /// Address of the remote Flight server (e.g., "http://host:9090").
    address: String,
}

impl ExchangeClient {
    /// Creates a new exchange client targeting the given address.
    pub fn new(address: impl Into<String>) -> Self {
        Self {
            address: address.into(),
        }
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

        let channel = Channel::from_shared(self.address.clone())
            .map_err(|e| {
                ExecutionError::InvalidOperation(format!(
                    "invalid Flight server address {}: {e}",
                    self.address
                ))
            })?
            .connect()
            .await
            .map_err(|e| {
                ExecutionError::InvalidOperation(format!(
                    "failed to connect to Flight server at {}: {e}",
                    self.address
                ))
            })?;
        let mut client = FlightServiceClient::new(channel);

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

                // Task failed — propagate the worker's error immediately.
                Err(status) if status.code() == tonic::Code::Internal => {
                    return Err(ExecutionError::InvalidOperation(format!(
                        "remote task {ticket_text} on {} failed: {}",
                        self.address,
                        status.message()
                    )));
                }

                Err(e) => {
                    return Err(ExecutionError::InvalidOperation(format!(
                        "Flight do_get failed: {e}"
                    )));
                }
            };

            return Self::collect_response(response, ticket_text).await;
        }
    }

    async fn collect_response(
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
        let mut flight_stream = FlightRecordBatchStream::new_from_flight_data(
            response
                .into_inner()
                .map_err(|e| arrow_flight::error::FlightError::Tonic(Box::new(e))),
        );

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
                return Err(ExecutionError::InvalidOperation(format!(
                    "Flight stream error: {e}"
                )));
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
    inner: FlightRecordBatchStream,
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
                    ExecutionError::InvalidOperation(format!("Flight stream error: {e}")),
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
