//! Trait + helpers for the worker-side cross-fragment dynamic filter
//! producer path.
//!
//! HashJoinExec / SemiJoinExec build phases call
//! [`DynamicFilterPublisher::publish`] once per
//! [`arneb_planner::DynamicFilterProducer`] annotation after they
//! have finished accumulating the build-side values. The concrete
//! implementation (in `crates/server`) wraps `arneb_rpc::report_dynamic_filters`
//! so the Domain ships to the coordinator's `DynamicFilterService`,
//! where partition Domains are union-merged and pushed to probe-side
//! workers via `notify_dynamic_filter`.
//!
//! A1.5 (2026-05-27): producer side. The publisher is wired into
//! `ExecutionContext` by the worker's `TaskManager`; A1.4's scan-side
//! [`crate::DynamicFilterCollector`] is the consumer.

use std::sync::Arc;

use arneb_common::{bloom_dynamic_filter_enabled, BloomFilter, Domain, DynamicFilterId};
use async_trait::async_trait;
use std::fmt::Debug;

use crate::hash_join::{distinct_scalar_values, dynamic_filter_cap};

/// Worker-side hook for shipping a finalised partition Domain to the
/// coordinator. One implementation lives in `crates/server`
/// (`FlightDynamicFilterPublisher`); tests typically use a
/// `Mutex<Vec<...>>`-backed collector.
///
/// The publisher closes over the task's `(query_id, task_id,
/// partition_idx, coord_address)` at construction time so the build
/// phase only needs to surface `(df_id, domain)`.
#[async_trait]
pub trait DynamicFilterPublisher: Debug + Send + Sync {
    /// Task-scoped partition index captured by the concrete publisher.
    /// Broadcast joins use this as the coordinator report partition because
    /// their build-side bloom is replicated across probe partitions.
    fn task_partition_idx(&self) -> u32 {
        0
    }

    /// Publish one resolved partition Domain. Fire-and-forget — any
    /// transport error is logged but does not fail the build phase
    /// (correctness fallback: coord never sees this DF, probe-side
    /// scan times out and reads everything).
    async fn publish(&self, df_id: DynamicFilterId, domain: Domain);

    /// Publish one partition Domain with an explicit producer partition index.
    /// Most producers report the task partition captured by the concrete
    /// publisher; callers may override this when the producer partition differs.
    async fn publish_partition(&self, df_id: DynamicFilterId, _partition_idx: u32, domain: Domain) {
        self.publish(df_id, domain).await;
    }
}

/// Convenience type alias used in `ExecutionContext` and on
/// `HashJoinExec` / `SemiJoinExec`.
pub type DynamicFilterPublisherRef = Arc<dyn DynamicFilterPublisher>;

/// Build a [`Domain`] from one build-side key column for a single
/// partition.
///
/// Uses [`distinct_scalar_values`] up to the per-type cap from
/// [`dynamic_filter_cap`]. Over-cap arrays degrade to [`Domain::All`]
/// unless `ARNEB_BLOOM_DF=1`, in which case a fixed-size
/// [`Domain::Bloom`] is emitted for the probe scan.
pub fn build_partition_domain_for_column(arr: &arrow::array::ArrayRef) -> Domain {
    let cap = match dynamic_filter_cap(arr.data_type()) {
        Some(c) => c,
        // Unsupported type for distinct collection (Utf8 etc.): emit
        // `All` so the scan reads every row, same as today.
        None => return Domain::All,
    };
    match distinct_scalar_values(arr, cap) {
        Some(values) => Domain::DistinctValues(values),
        None if bloom_dynamic_filter_enabled() => build_bloom_domain_for_column(arr),
        None => Domain::All,
    }
}

fn build_bloom_domain_for_column(arr: &arrow::array::ArrayRef) -> Domain {
    use arneb_common::types::ScalarValue;
    use arrow::array::{Array, AsArray};
    use arrow::datatypes::DataType as ArrowDataType;

    let mut bloom = BloomFilter::with_fixed_params();
    match arr.data_type() {
        ArrowDataType::Int32 => {
            let a = arr.as_primitive::<arrow::datatypes::Int32Type>();
            for i in 0..a.len() {
                if !a.is_null(i) {
                    bloom.insert(&ScalarValue::Int32(a.value(i)));
                }
            }
        }
        ArrowDataType::Int64 => {
            let a = arr.as_primitive::<arrow::datatypes::Int64Type>();
            for i in 0..a.len() {
                if !a.is_null(i) {
                    bloom.insert(&ScalarValue::Int64(a.value(i)));
                }
            }
        }
        ArrowDataType::Date32 => {
            let a = arr.as_primitive::<arrow::datatypes::Date32Type>();
            for i in 0..a.len() {
                if !a.is_null(i) {
                    bloom.insert(&ScalarValue::Date32(a.value(i)));
                }
            }
        }
        _ => return Domain::All,
    }
    Domain::Bloom(bloom)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arneb_common::types::ScalarValue;
    use arrow::array::{ArrayRef, Int64Array, StringArray};

    #[test]
    fn build_domain_from_int64_yields_distinct_values() {
        let arr: ArrayRef = Arc::new(Int64Array::from(vec![1i64, 2, 2, 3]));
        match build_partition_domain_for_column(&arr) {
            Domain::DistinctValues(mut v) => {
                v.sort_by_key(|s| match s {
                    ScalarValue::Int64(x) => *x,
                    _ => unreachable!(),
                });
                assert_eq!(
                    v,
                    vec![
                        ScalarValue::Int64(1),
                        ScalarValue::Int64(2),
                        ScalarValue::Int64(3),
                    ]
                );
            }
            other => panic!("expected DistinctValues, got {other:?}"),
        }
    }

    #[test]
    fn build_domain_from_empty_int64_yields_empty_distinct() {
        let arr: ArrayRef = Arc::new(Int64Array::from(Vec::<i64>::new()));
        match build_partition_domain_for_column(&arr) {
            Domain::DistinctValues(v) => assert!(v.is_empty()),
            other => panic!("expected DistinctValues, got {other:?}"),
        }
    }

    #[test]
    fn build_domain_from_unsupported_type_yields_all() {
        // Utf8 is intentionally not handled by `dynamic_filter_cap` —
        // string collection is expensive and rarely useful for arneb's
        // workload. The publisher path degrades to `All`.
        let arr: ArrayRef = Arc::new(StringArray::from(vec!["a", "b"]));
        assert!(matches!(
            build_partition_domain_for_column(&arr),
            Domain::All
        ));
    }

    #[test]
    fn over_cap_with_bloom_env_yields_bloom() {
        std::env::set_var("ARNEB_BLOOM_DF", "1");
        let values: Vec<i64> = (0..1_000_001).collect();
        let arr: ArrayRef = Arc::new(Int64Array::from(values));
        match build_partition_domain_for_column(&arr) {
            Domain::Bloom(bloom) => {
                assert!(bloom.contains(&ScalarValue::Int64(0)));
                assert!(bloom.contains(&ScalarValue::Int64(1_000_000)));
            }
            other => panic!("expected Bloom, got {other:?}"),
        }
        std::env::remove_var("ARNEB_BLOOM_DF");
    }
}
