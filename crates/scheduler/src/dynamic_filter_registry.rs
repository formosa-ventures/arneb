//! Per-query registry of [`DynamicFilterService`] instances.
//!
//! `QueryCoordinator::execute` creates one service per query, registers
//! it here under the query's `QueryId`, and drops the registration on
//! return. RPC handlers (the Flight `report_dynamic_filters` action)
//! look up by `QueryId` and forward into the service.
//!
//! The registry exists so the RPC layer — which has no notion of
//! per-query state — can route incoming reports without holding a
//! direct reference to the coordinator.
//!
//! A1.3 (2026-05-27): types compile and have unit tests. No real
//! query yet registers a service (A1.5 wires producers, A1.4 wires
//! consumers; A1.6 flips the feature flag).

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use arneb_common::error::ExecutionError;
use arneb_common::{Domain, DynamicFilterId, QueryId};
use tokio::sync::Mutex;

use crate::dynamic_filter::DynamicFilterService;

fn dfrpc_domain_variant(domain: &Domain) -> String {
    match domain {
        Domain::DistinctValues(values) => format!("DistinctValues(len={})", values.len()),
        Domain::Range { .. } => "Range".to_string(),
        Domain::Bloom(_) => "Bloom".to_string(),
        Domain::All => "All".to_string(),
    }
}

/// Map of `QueryId → DynamicFilterService` shared across the coord's
/// RPC handlers and `QueryCoordinator`.
#[derive(Debug, Clone, Default)]
pub struct DynamicFilterServiceRegistry {
    inner: Arc<RwLock<HashMap<QueryId, Arc<Mutex<DynamicFilterService>>>>>,
}

impl DynamicFilterServiceRegistry {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts a freshly built service for a query. Replaces any
    /// existing entry (which would only happen if `register` ran
    /// twice for the same id; treated as a caller bug).
    pub fn register(&self, query_id: QueryId, service: DynamicFilterService) {
        self.inner
            .write()
            .unwrap()
            .insert(query_id, Arc::new(Mutex::new(service)));
    }

    /// Returns a handle to the service for `query_id`, if registered.
    pub fn get(&self, query_id: &QueryId) -> Option<Arc<Mutex<DynamicFilterService>>> {
        self.inner.read().unwrap().get(query_id).cloned()
    }

    /// Drops the registration. Pending subscribers see their sender
    /// dropped (`oneshot::Receiver` returns `RecvError`).
    pub fn unregister(&self, query_id: &QueryId) {
        let removed = self.inner.write().unwrap().remove(query_id);
        if let Some(svc) = removed {
            match svc.try_lock() {
                Ok(guard) => {
                    let waiting = guard.dfrpc_waiting_summary();
                    if waiting.is_empty() {
                        eprintln!(
                            "[DFRPC] coord registry unregister query_id={} waiting=none",
                            query_id
                        );
                    } else {
                        for entry in waiting {
                            eprintln!(
                                "[DFRPC] coord registry unregister query_id={} still_waiting {}",
                                query_id, entry
                            );
                        }
                    }
                }
                Err(_) => {
                    eprintln!(
                        "[DFRPC] coord registry unregister query_id={} waiting=unknown service_locked",
                        query_id
                    );
                }
            }
        }
    }

    /// Convenience: route a worker-reported partial Domain into the
    /// registered service for `query_id`. Async because the per-query
    /// service is guarded by `tokio::sync::Mutex`.
    ///
    /// Returns `InvalidOperation` if the query has no registered
    /// service (likely a late report after the query ended, which the
    /// coord can safely log + drop).
    pub async fn report_partition(
        &self,
        query_id: QueryId,
        df_id: DynamicFilterId,
        partition_idx: u32,
        domain: Domain,
    ) -> Result<(), ExecutionError> {
        let svc = self.get(&query_id).ok_or_else(|| {
            ExecutionError::InvalidOperation(format!(
                "no dynamic filter service registered for query {query_id}"
            ))
        })?;
        eprintln!(
            "[DFRPC] coord registry route_report query_id={} df_id={} partition_idx={} domain={}",
            query_id,
            df_id,
            partition_idx,
            dfrpc_domain_variant(&domain)
        );
        let mut guard = svc.lock().await;
        guard.report_partition(df_id, partition_idx, domain)
    }

    /// Number of currently registered queries. Useful for sanity
    /// assertions in tests (registry should reach zero between
    /// queries once `QueryCoordinator::execute` returns).
    pub fn len(&self) -> usize {
        self.inner.read().unwrap().len()
    }

    /// True when no query has a service registered.
    pub fn is_empty(&self) -> bool {
        self.inner.read().unwrap().is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arneb_common::types::ScalarValue;
    use arneb_common::StageId;

    fn distinct(vals: &[i64]) -> Domain {
        Domain::DistinctValues(vals.iter().copied().map(ScalarValue::Int64).collect())
    }

    #[tokio::test]
    async fn register_report_unregister() {
        let registry = DynamicFilterServiceRegistry::new();
        let q = QueryId::new();
        let df = DynamicFilterId(0);

        let mut svc = DynamicFilterService::new();
        svc.register_query(&[(df, StageId(7), 2)]);
        registry.register(q, svc);
        assert_eq!(registry.len(), 1);

        // First partition reports — DF still pending.
        registry
            .report_partition(q, df, 0, distinct(&[1, 2]))
            .await
            .unwrap();
        // Second partition reports — DF resolves inside the service.
        registry
            .report_partition(q, df, 1, distinct(&[3]))
            .await
            .unwrap();

        // Service still registered with merged state.
        let svc_handle = registry.get(&q).unwrap();
        let guard = svc_handle.lock().await;
        let resolved = guard.resolved(df).expect("DF should be resolved");
        match resolved {
            Domain::DistinctValues(v) => assert_eq!(v.len(), 3),
            other => panic!("expected DistinctValues, got {other:?}"),
        }
        drop(guard);

        registry.unregister(&q);
        assert!(registry.is_empty());
    }

    #[tokio::test]
    async fn report_to_unknown_query_errors() {
        let registry = DynamicFilterServiceRegistry::new();
        let q = QueryId::new();
        let err = registry
            .report_partition(q, DynamicFilterId(0), 0, Domain::All)
            .await
            .unwrap_err();
        assert!(matches!(err, ExecutionError::InvalidOperation(_)));
    }

    #[test]
    fn registry_is_clone_cheap() {
        // Behavioural assertion: cloning shares the inner Arc so two
        // handles see the same insertions.
        let r1 = DynamicFilterServiceRegistry::new();
        let r2 = r1.clone();
        let q = QueryId::new();
        r1.register(q, DynamicFilterService::new());
        assert_eq!(r2.len(), 1);
        assert!(r2.get(&q).is_some());
    }
}
