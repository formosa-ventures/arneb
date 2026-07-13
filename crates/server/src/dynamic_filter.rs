//! Worker-side concrete [`DynamicFilterPublisher`] impl backed by
//! Flight RPC.
//!
//! A1.5 (2026-05-27): closes over `(coord_address, query_id, task_id)`
//! at task admission so HashJoinExec / SemiJoinExec only need to
//! surface `(df_id, domain)` from inside the build phase. Publishes
//! via `arneb_rpc::report_dynamic_filters`; transport errors are
//! logged but do not fail the build (soundness fallback at the scan
//! side handles "DF never arrived").

use arneb_common::{Domain, DynamicFilterId, QueryId, TaskId};
use arneb_execution::DynamicFilterPublisher;
use async_trait::async_trait;
use std::fmt;

fn dfrpc_domain_variant(domain: &Domain) -> String {
    match domain {
        Domain::DistinctValues(values) => format!("DistinctValues(len={})", values.len()),
        Domain::Range { .. } => "Range".to_string(),
        Domain::Bloom(_) => "Bloom".to_string(),
        Domain::All => "All".to_string(),
    }
}

/// Concrete publisher used by the worker's `TaskManager`. Each task
/// gets its own instance because `task_id` is task-scoped.
pub struct FlightDynamicFilterPublisher {
    coord_address: String,
    query_id: QueryId,
    task_id: TaskId,
}

impl fmt::Debug for FlightDynamicFilterPublisher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FlightDynamicFilterPublisher")
            .field("coord_address", &self.coord_address)
            .field("query_id", &self.query_id)
            .field("task_id", &self.task_id)
            .finish()
    }
}

impl FlightDynamicFilterPublisher {
    pub fn new(coord_address: String, query_id: QueryId, task_id: TaskId) -> Self {
        Self {
            coord_address,
            query_id,
            task_id,
        }
    }

    async fn publish_with_partition(
        &self,
        df_id: DynamicFilterId,
        partition_idx: u32,
        domain: Domain,
    ) {
        eprintln!(
            "[DFRPC] worker publish query_id={} task_id={} partition_idx={} df_id={} domain={}",
            self.query_id,
            self.task_id,
            partition_idx,
            df_id,
            dfrpc_domain_variant(&domain)
        );
        let req = arneb_rpc::ReportDynamicFilterRequest {
            query_id: self.query_id,
            task_id: self.task_id,
            df_id,
            partition_idx,
            domain,
        };
        match arneb_rpc::report_dynamic_filters(&self.coord_address, &req).await {
            Ok(()) => {
                if std::env::var_os("ARNEB_TRACE_DFPUB").is_some() {
                    eprintln!(
                        "[DFPUB] flight_send df_id={} ok=true query_id={} task_id={} partition_idx={}",
                        df_id, self.query_id, self.task_id, partition_idx
                    );
                }
            }
            Err(e) => {
                if std::env::var_os("ARNEB_TRACE_DFPUB").is_some() {
                    eprintln!(
                        "[DFPUB] flight_send df_id={} ok=false query_id={} task_id={} partition_idx={} error={}",
                        df_id, self.query_id, self.task_id, partition_idx, e
                    );
                }
                tracing::warn!(
                    query_id = %self.query_id,
                    task_id = %self.task_id,
                    df_id = %df_id,
                    error = %e,
                    "dynamic filter report failed (coord transport)"
                );
            }
        }
    }
}

#[async_trait]
impl DynamicFilterPublisher for FlightDynamicFilterPublisher {
    fn task_partition_idx(&self) -> u32 {
        self.task_id.partition_id
    }

    async fn publish(&self, df_id: DynamicFilterId, domain: Domain) {
        self.publish_with_partition(df_id, self.task_id.partition_id, domain)
            .await;
    }

    async fn publish_partition(&self, df_id: DynamicFilterId, partition_idx: u32, domain: Domain) {
        self.publish_with_partition(df_id, partition_idx, domain)
            .await;
    }
}
