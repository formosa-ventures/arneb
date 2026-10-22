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

use crate::security::{cluster_security, ClusterSecurity, FlightPeer};

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
    /// reconnects and asks again, up to [`MAX_RESUME_ATTEMPTS`] times
    /// ([`classify`] has the whole policy). A
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

        let peer = self.peer()?;
        let mut client = connect(&peer).await?;
        let mut transport_attempts = 0usize;
        let mut transport_error: Option<tonic::Status> = None;

        loop {
            let ticket = Ticket::new(ticket_text.clone());

            let status = match client.do_get(ticket).await {
                Ok(response) => {
                    return self.collect_response(peer, response, ticket_text).await;
                }
                Err(status) => status,
            };
            match classify(&status) {
                StatusClass::Wait => {
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
                }
                StatusClass::Grace => {
                    if tokio::time::Instant::now() >= not_found_deadline {
                        return Err(ExecutionError::InvalidOperation(format!(
                            "Flight do_get: task {ticket_text} unknown on {} \
                             after 60s (task never started or already cleaned up)",
                            self.address
                        )));
                    }
                    tokio::time::sleep(backoff).await;
                    backoff = std::cmp::min(backoff * 2, backoff_max);
                }
                // Propagate the worker's error immediately, keeping a
                // memory-limit failure's class.
                StatusClass::Failed => {
                    return Err(crate::task_error::remote_error(
                        format!("remote task {ticket_text} on {} failed", self.address),
                        &status,
                    ));
                }
                StatusClass::Transport if transport_attempts < MAX_RESUME_ATTEMPTS => {
                    transport_attempts += 1;
                    tracing::warn!(
                        target: "arneb::exchange",
                        ticket = %ticket_text,
                        address = %self.address,
                        attempt = transport_attempts,
                        error = %status,
                        "exchange do_get broke before the response, reconnecting"
                    );
                    tokio::time::sleep(resume_backoff(transport_attempts)).await;
                    if let Ok(reconnected) = connect(&peer).await {
                        client = reconnected;
                    }
                    transport_error.get_or_insert(status);
                }
                StatusClass::Transport | StatusClass::Fatal => {
                    let e = transport_error.unwrap_or(status);
                    return Err(ExecutionError::InvalidOperation(format!(
                        "Flight do_get failed: {e}"
                    )));
                }
            }
        }
    }

    /// This client's peer with its cluster security (bearer secret, TLS),
    /// prepared once per `fetch_partition` for the first `do_get` and
    /// every reconnect.
    fn peer(&self) -> Result<FlightPeer, ExecutionError> {
        let security = self
            .security
            .as_deref()
            .unwrap_or_else(|| cluster_security());
        FlightPeer::new(&self.address, security)
            .map_err(|e| ExecutionError::InvalidOperation(format!("Flight server: {e}")))
    }

    /// Open `ticket` from row `delivered` after its stream broke with
    /// `err`, spending `attempts` from the [`MAX_RESUME_ATTEMPTS`] budget.
    /// `None` once the budget is spent or the output can't be read again
    /// (a partitioned buffer is consumed once and refuses a resume, or the
    /// output is gone).
    async fn reopen(
        &self,
        peer: &FlightPeer,
        ticket: &str,
        delivered: usize,
        attempts: &mut usize,
        err: &FlightError,
    ) -> Option<FlightBatchStream> {
        let resume_ticket = format!("{ticket}:{delivered}");
        while *attempts < MAX_RESUME_ATTEMPTS {
            *attempts += 1;
            tracing::warn!(
                target: "arneb::exchange",
                ticket = %ticket,
                address = %self.address,
                delivered_rows = delivered,
                attempt = *attempts,
                error = %err,
                "exchange stream broke, reopening"
            );
            tokio::time::sleep(resume_backoff(*attempts)).await;
            let Ok(mut client) = connect(peer).await else {
                continue;
            };
            match client.do_get(Ticket::new(resume_ticket.clone())).await {
                Ok(response) => return Some(decode(response)),
                Err(status) if classify(&status).resumes() => continue,
                Err(_) => return None,
            }
        }
        None
    }

    /// Wrap a response so a transport break mid-stream (#139) reopens the
    /// ticket from the first row not yet delivered (`ticket:from_row`), so
    /// the producer sends only the remainder. Only broadcast output can be
    /// read again, and the resume rests on two producer-side contracts:
    ///
    /// - every `subscribe` of a `BroadcastOutputBuffer` replays the buffer
    ///   from its first row, and `subscribe_from_row` skips exactly the
    ///   rows already delivered;
    /// - the task manager keeps a broadcast buffer registered until the
    ///   query's 600 s ceiling (its early cleanup removes only partitioned
    ///   buffers), so the reopened ticket still finds it.
    ///
    /// A partitioned ticket refuses the reopen, and the original error is
    /// surfaced as before.
    fn resumable(
        self,
        peer: FlightPeer,
        ticket: String,
        first: FlightBatchStream,
    ) -> FlightBatchStream {
        Box::pin(async_stream::stream! {
            use futures::StreamExt;
            let mut stream = first;
            let mut delivered = 0usize;
            let mut attempts = 0usize;
            while let Some(item) = stream.next().await {
                match item {
                    Ok(batch) => {
                        delivered += batch.num_rows();
                        yield Ok(batch);
                    }
                    Err(err) if is_transport_error(&err) => {
                        match self.reopen(&peer, &ticket, delivered, &mut attempts, &err).await {
                            Some(reopened) => stream = reopened,
                            None => {
                                yield Err(err);
                                return;
                            }
                        }
                    }
                    Err(err) => {
                        yield Err(err);
                        return;
                    }
                }
            }
        })
    }

    async fn collect_response(
        &self,
        peer: FlightPeer,
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
        let mut flight_stream = self
            .clone()
            .resumable(peer, ticket_text.clone(), decode(response));

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
                    format!("Flight stream error from {}", self.address),
                    &e,
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
            arneb_common::memory_pool::batch_memory_size(&first_batch) as u64,
        ));
        Ok(Box::pin(FlightPassthroughStream {
            schema,
            first: Some(first_batch),
            first_live,
            inner: flight_stream,
            address: self.address.clone(),
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
    /// The remote Flight address, named in errors.
    address: String,
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
                let bytes = arneb_common::memory_pool::batch_memory_size(&b) as u64;
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
                        format!("Flight stream error from {}", self.address),
                        &e,
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

async fn connect(peer: &FlightPeer) -> Result<crate::security::FlightClient, ExecutionError> {
    peer.connect()
        .await
        .map_err(|e| ExecutionError::InvalidOperation(format!("Flight server: {e}")))
}

/// Backoff before resume attempt `attempt` (1-based): 100, 200, 400 ms.
fn resume_backoff(attempt: usize) -> std::time::Duration {
    std::time::Duration::from_millis(50 << attempt)
}

/// How the exchange client treats a status from a `do_get` call or a
/// stream, in one place.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StatusClass {
    /// `Unavailable`: the producer's task is running but its buffer is not
    /// registered yet. tonic also reports a refused or dropped connection
    /// with this code, so on a stream (or a reopen, whose buffer was
    /// registered) it means transport trouble.
    Wait,
    /// `NotFound`: the task is unknown on the worker, which covers
    /// dispatch latency on a first call and a cleaned-up output on a
    /// reopen.
    Grace,
    /// The producer's task failed: `Internal`, or `ResourceExhausted`
    /// marked as a memory-limit failure.
    Failed,
    /// The transport broke: `Unknown` (connection closed or reset, h2
    /// error), or a `ResourceExhausted` the producer did not send, such as
    /// an h2 `ENHANCE_YOUR_CALM` GOAWAY. Two other unmarked
    /// `ResourceExhausted`s land here too, and cost at most
    /// [`MAX_RESUME_ATTEMPTS`] reopens before the error surfaces: a
    /// memory-limit failure from a worker older than the class marker
    /// (mixed versions; it then surfaces as a generic error, not `53200`),
    /// and tonic's message-too-large encode error, which the encoder's
    /// 2 MiB message split keeps far below the 64 MiB limit.
    Transport,
    /// Anything else (`AlreadyExists`: a partitioned buffer already taken;
    /// `FailedPrecondition`: a resume it refuses; auth failures).
    Fatal,
}

impl StatusClass {
    /// Whether a broken stream or a failed reopen is worth reopening.
    fn resumes(self) -> bool {
        matches!(self, Self::Wait | Self::Transport)
    }
}

fn classify(status: &tonic::Status) -> StatusClass {
    match status.code() {
        tonic::Code::Unavailable => StatusClass::Wait,
        tonic::Code::NotFound => StatusClass::Grace,
        tonic::Code::Internal => StatusClass::Failed,
        tonic::Code::ResourceExhausted if crate::task_error::is_memory_limit(status) => {
            StatusClass::Failed
        }
        tonic::Code::Unknown | tonic::Code::ResourceExhausted => StatusClass::Transport,
        _ => StatusClass::Fatal,
    }
}

fn is_transport_error(err: &FlightError) -> bool {
    matches!(err, FlightError::Tonic(status) if classify(status).resumes())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TaskFailure;

    #[test]
    fn classify_separates_producer_failures_from_transport() {
        use tonic::Status;
        assert_eq!(
            classify(&Status::unavailable("not ready")),
            StatusClass::Wait
        );
        assert_eq!(classify(&Status::not_found("unknown")), StatusClass::Grace);
        assert_eq!(classify(&Status::internal("boom")), StatusClass::Failed);
        let memory = TaskFailure::resource_exhausted("query memory limit exceeded")
            .to_status("producer task failed");
        assert_eq!(classify(&memory), StatusClass::Failed);
        // tonic's code for an h2 ENHANCE_YOUR_CALM GOAWAY, no producer marker.
        let goaway = Status::resource_exhausted("h2 protocol error: ENHANCE_YOUR_CALM");
        assert_eq!(classify(&goaway), StatusClass::Transport);
        assert_eq!(
            classify(&Status::unknown("broken pipe")),
            StatusClass::Transport
        );
        assert_eq!(
            classify(&Status::already_exists("taken")),
            StatusClass::Fatal
        );
        assert_eq!(
            classify(&Status::failed_precondition("no resume")),
            StatusClass::Fatal
        );
        assert!(StatusClass::Wait.resumes() && StatusClass::Transport.resumes());
        assert!(!StatusClass::Grace.resumes() && !StatusClass::Fatal.resumes());
    }
}
