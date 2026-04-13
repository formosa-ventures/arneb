//! Exchange client for fetching RecordBatches from remote Flight servers.

use std::sync::Arc;

use arneb_common::error::ExecutionError;
use arneb_common::stream::{stream_from_batches, SendableRecordBatchStream};
use arrow::datatypes::SchemaRef;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::Ticket;
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
    pub async fn fetch_partition(
        &self,
        task_id: &str,
        partition_id: usize,
    ) -> Result<SendableRecordBatchStream, ExecutionError> {
        let ticket = Ticket::new(format!("{task_id}:{partition_id}"));

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

        let response = client
            .do_get(ticket)
            .await
            .map_err(|e| ExecutionError::InvalidOperation(format!("Flight do_get failed: {e}")))?;

        let flight_stream = FlightRecordBatchStream::new_from_flight_data(
            response
                .into_inner()
                .map_err(|e| arrow_flight::error::FlightError::Tonic(Box::new(e))),
        );

        // Collect all batches (for simplicity — true streaming is a future optimization).
        let mut batches = Vec::new();
        let mut schema: Option<SchemaRef> = None;

        use futures::StreamExt;
        let mut stream = flight_stream;
        while let Some(result) = stream.next().await {
            let batch = result.map_err(|e| {
                ExecutionError::InvalidOperation(format!("Flight stream error: {e}"))
            })?;
            if schema.is_none() {
                schema = Some(batch.schema());
            }
            batches.push(batch);
        }

        let schema = schema.unwrap_or_else(|| Arc::new(arrow::datatypes::Schema::empty()));

        Ok(stream_from_batches(schema, batches))
    }
}
