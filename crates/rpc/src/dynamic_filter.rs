//! Wire types for cross-fragment dynamic filter transport.
//!
//! Two messages flow over Flight `do_action`:
//!
//! 1. **Worker → coord** ([`ReportDynamicFilterRequest`]): when a task
//!    ending in a HashJoin / SemiJoin build computes its partition's
//!    [`Domain`], it posts the result so the coordinator's
//!    `DynamicFilterService` can merge it with peer partitions.
//!
//! 2. **Coord → worker** ([`NotifyDynamicFilterRequest`]): once a DF
//!    resolves on the coordinator, the coordinator pushes the merged
//!    Domain to every worker task whose probe-side scan was waiting
//!    on this id. (For DFs that resolved BEFORE a task was dispatched,
//!    they ride on `TaskDescriptor::pending_dynamic_filters` and this
//!    Action is not used.)
//!
//! Both messages share a small serde-JSON encoding; `#[serde(default)]`
//! on new fields keeps the wire format forward-compatible.
//!
//! A1.3 (2026-05-27): these types compile and round-trip in unit tests
//! but the encode/decode helpers are not yet exercised on any hot
//! arneb codepath — `DynamicFilterService` does not receive reports
//! and `DynamicFilterCollector` is empty for every task. A1.4 wires
//! the consumer; A1.5 wires the producer.

use arneb_common::{Domain, DynamicFilterId, QueryId, TaskId};
use serde::{Deserialize, Serialize};

/// One partition's report from a worker to the coordinator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReportDynamicFilterRequest {
    /// Query this report belongs to.
    pub query_id: QueryId,
    /// The reporting task (also identifies the partition via
    /// `task_id.partition_id`).
    pub task_id: TaskId,
    /// The dynamic filter being reported.
    pub df_id: DynamicFilterId,
    /// Partition index of the reporting task. Redundant with
    /// `task_id.partition_id` but explicit for clarity at the
    /// `DynamicFilterService::report_partition` call site.
    pub partition_idx: u32,
    /// The partition's partial Domain (union of distinct build-side
    /// values for this DF id within this partition).
    pub domain: Domain,
}

impl ReportDynamicFilterRequest {
    /// JSON-encode for the Flight action body.
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("ReportDynamicFilterRequest serialization should not fail")
    }

    /// Decode from a Flight action body.
    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        serde_json::from_slice(bytes)
            .map_err(|e| format!("ReportDynamicFilterRequest decode error: {e}"))
    }
}

/// Late-arrival push from coordinator to one worker task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotifyDynamicFilterRequest {
    /// Query this notification belongs to.
    pub query_id: QueryId,
    /// Target task on the receiving worker. Routes the Domain to that
    /// task's [`crate::DynamicFilterCollector`]-equivalent.
    pub task_id: TaskId,
    /// The dynamic filter that just resolved.
    pub df_id: DynamicFilterId,
    /// The fully-merged Domain (union of every partition's report).
    pub domain: Domain,
}

impl NotifyDynamicFilterRequest {
    /// JSON-encode for the Flight action body.
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("NotifyDynamicFilterRequest serialization should not fail")
    }

    /// Decode from a Flight action body.
    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        serde_json::from_slice(bytes)
            .map_err(|e| format!("NotifyDynamicFilterRequest decode error: {e}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arneb_common::types::ScalarValue;
    use arneb_common::StageId;

    fn sample_domain() -> Domain {
        Domain::DistinctValues(vec![ScalarValue::Int64(1), ScalarValue::Int64(2)])
    }

    #[test]
    fn report_roundtrip() {
        let q = QueryId::new();
        let original = ReportDynamicFilterRequest {
            query_id: q,
            task_id: TaskId {
                stage_id: StageId(7),
                partition_id: 3,
            },
            df_id: DynamicFilterId(42),
            partition_idx: 3,
            domain: sample_domain(),
        };
        let bytes = original.encode();
        let decoded = ReportDynamicFilterRequest::decode(&bytes).unwrap();
        assert_eq!(decoded.query_id, q);
        assert_eq!(decoded.task_id.stage_id, StageId(7));
        assert_eq!(decoded.task_id.partition_id, 3);
        assert_eq!(decoded.df_id, DynamicFilterId(42));
        assert_eq!(decoded.partition_idx, 3);
        assert_eq!(decoded.domain, sample_domain());
    }

    #[test]
    fn notify_roundtrip() {
        let q = QueryId::new();
        let original = NotifyDynamicFilterRequest {
            query_id: q,
            task_id: TaskId {
                stage_id: StageId(0),
                partition_id: 0,
            },
            df_id: DynamicFilterId(1),
            domain: Domain::All,
        };
        let bytes = original.encode();
        let decoded = NotifyDynamicFilterRequest::decode(&bytes).unwrap();
        assert_eq!(decoded.query_id, q);
        assert_eq!(decoded.df_id, DynamicFilterId(1));
        assert!(decoded.domain.is_all());
    }

    #[test]
    fn report_decode_garbage_errors() {
        assert!(ReportDynamicFilterRequest::decode(b"not json").is_err());
    }

    #[test]
    fn notify_decode_garbage_errors() {
        assert!(NotifyDynamicFilterRequest::decode(b"{}").is_err());
    }
}
