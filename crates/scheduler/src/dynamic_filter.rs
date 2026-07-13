//! Coordinator-side dynamic-filter merge service.
//!
//! Each query owns one [`DynamicFilterService`]. Producer-side tasks
//! (HashJoin / SemiJoin build phases) report their partition's partial
//! [`Domain`] to the coordinator via [`report_partition`]; consumer-side
//! tasks (probe-side scans) call [`subscribe`] to receive a `oneshot`
//! receiver that fires when every expected partition has reported.
//!
//! See `openspec/changes/cross-fragment-dynamic-filter/design.md` D2.
//!
//! Per-partition Domains are merged with [`Domain::union`] — when a join
//! is partitioned by the build key, each partition holds a disjoint slice
//! of build-side values, so the probe-side filter is the union of those
//! slices (the risks section of design.md spells out why union, not
//! intersection, is correct).
//!
//! Soundness contract: a subscriber's receiver only ever fires with a
//! valid resolved Domain, OR closes with `RecvError` (which the scan
//! treats as "no DF available, scan everything"). Either way the query
//! produces the correct answer; the DF is best-effort.
//!
//! This file is the A1.2 milestone — the service compiles and is
//! covered by unit tests, but is not yet wired into `QueryCoordinator`
//! or the Flight RPC layer (those are A1.3+).

use std::collections::HashMap;

use arneb_common::error::ExecutionError;
use arneb_common::{Domain, DynamicFilterId, StageId, DEFAULT_MAX_DISTINCT_VALUES};
use tokio::sync::oneshot;

fn dfrpc_domain_variant(domain: &Domain) -> String {
    match domain {
        Domain::DistinctValues(values) => format!("DistinctValues(len={})", values.len()),
        Domain::Range { .. } => "Range".to_string(),
        Domain::Bloom(_) => "Bloom".to_string(),
        Domain::All => "All".to_string(),
    }
}

/// Per-DF accumulation state.
#[derive(Debug)]
struct DfState {
    expected_partitions: u32,
    partitions: HashMap<u32, Domain>,
    resolved: Option<Domain>,
    notify: Vec<oneshot::Sender<Domain>>,
    max_distinct_values: usize,
}

/// Coordinator-side merge service for one query.
///
/// Owned by `QueryCoordinator` (A1.3 wiring); shared between the RPC
/// task-completion handler (calls [`report_partition`]) and the worker
/// `submit_task` / `notify_dynamic_filter` paths (call [`subscribe`]).
/// The service itself is `!Sync`; wrap it in `Arc<tokio::sync::Mutex<_>>`
/// at the call site.
#[derive(Debug)]
pub struct DynamicFilterService {
    states: HashMap<DynamicFilterId, DfState>,
    stage_partition_counts: HashMap<StageId, u32>,
    max_distinct_values: usize,
}

impl Default for DynamicFilterService {
    fn default() -> Self {
        Self::new()
    }
}

impl DynamicFilterService {
    /// Creates a service with the default per-DF cap on distinct values.
    pub fn new() -> Self {
        Self {
            states: HashMap::new(),
            stage_partition_counts: HashMap::new(),
            max_distinct_values: DEFAULT_MAX_DISTINCT_VALUES,
        }
    }

    /// Creates a service with a custom cap. Mostly useful in tests where
    /// the cap needs to fire predictably on small inputs.
    pub fn with_max_distinct_values(max_distinct_values: usize) -> Self {
        Self {
            states: HashMap::new(),
            stage_partition_counts: HashMap::new(),
            max_distinct_values,
        }
    }

    /// Registers every dynamic filter this query expects to resolve.
    ///
    /// Each tuple is `(filter id, producing stage id, partition count of
    /// that stage)`. Re-registering an existing id overwrites its state
    /// and drops any subscribers (treated as `RecvError` on the scan
    /// side); only intentional in test scenarios.
    pub fn register_query(&mut self, filter_ids: &[(DynamicFilterId, StageId, u32)]) {
        for (df_id, stage_id, expected) in filter_ids {
            eprintln!(
                "[DFRPC] coord service register df_id={} stage_id={} expected_task_count={}",
                df_id, stage_id, expected
            );
            self.states.insert(
                *df_id,
                DfState {
                    expected_partitions: *expected,
                    partitions: HashMap::new(),
                    resolved: None,
                    notify: Vec::new(),
                    max_distinct_values: self.max_distinct_values,
                },
            );
            self.stage_partition_counts.insert(*stage_id, *expected);
        }
    }

    /// Records one partition's partial Domain for a DF. When every
    /// expected partition has reported, the accumulated Domains are
    /// union-merged and the result is sent to all pending subscribers.
    ///
    /// Returns `InvalidOperation` if the DF id was never registered or if
    /// `partition_idx` is out of range. Multiple reports for the same
    /// partition are unioned; this keeps the protocol best-effort when an
    /// early empty report is followed by a fuller domain for the same task.
    pub fn report_partition(
        &mut self,
        df_id: DynamicFilterId,
        partition_idx: u32,
        partial: Domain,
    ) -> Result<(), ExecutionError> {
        let state = self.states.get_mut(&df_id).ok_or_else(|| {
            ExecutionError::InvalidOperation(format!("dynamic filter {df_id} not registered"))
        })?;

        eprintln!(
            "[DFRPC] coord service report df_id={} partition_idx={} domain={} reported_before={} expected_task_count={}",
            df_id,
            partition_idx,
            dfrpc_domain_variant(&partial),
            state.partitions.len(),
            state.expected_partitions
        );

        if partition_idx >= state.expected_partitions {
            return Err(ExecutionError::InvalidOperation(format!(
                "partition {partition_idx} out of range for {df_id} (expected {})",
                state.expected_partitions
            )));
        }
        if state.resolved.is_some() {
            return Ok(());
        }

        let max = state.max_distinct_values;
        state
            .partitions
            .entry(partition_idx)
            .and_modify(|existing| {
                *existing = existing.clone().union(partial.clone(), max);
            })
            .or_insert(partial);

        if (state.partitions.len() as u32) == state.expected_partitions {
            eprintln!(
                "[DFRPC] coord service merge_start df_id={} reports={} expected_task_count={}",
                df_id,
                state.partitions.len(),
                state.expected_partitions
            );
            let max = state.max_distinct_values;
            let merged = state
                .partitions
                .values()
                .cloned()
                .reduce(|acc, next| acc.union(next, max))
                .unwrap_or_else(Domain::all);
            eprintln!(
                "[DFRPC] coord service resolved df_id={} domain={} subscribers={}",
                df_id,
                dfrpc_domain_variant(&merged),
                state.notify.len()
            );
            state.resolved = Some(merged.clone());
            for tx in state.notify.drain(..) {
                let _ = tx.send(merged.clone());
            }
        } else {
            eprintln!(
                "[DFRPC] coord service waiting df_id={} reports={} expected_task_count={}",
                df_id,
                state.partitions.len(),
                state.expected_partitions
            );
        }
        Ok(())
    }

    /// Returns a `oneshot::Receiver` that fires when the DF resolves.
    ///
    /// If the DF has already resolved (fast path: build stage finished
    /// before the probe task subscribed), the receiver is pre-fired with
    /// the resolved Domain and `recv` returns immediately.
    ///
    /// If the service is later dropped or [`drop_query`] is called
    /// before resolution, the sender side is dropped and the receiver
    /// will see `RecvError`; the scan layer treats that as "no DF" and
    /// proceeds with static filters only.
    pub fn subscribe(
        &mut self,
        df_id: DynamicFilterId,
    ) -> Result<oneshot::Receiver<Domain>, ExecutionError> {
        let state = self.states.get_mut(&df_id).ok_or_else(|| {
            ExecutionError::InvalidOperation(format!("dynamic filter {df_id} not registered"))
        })?;

        let (tx, rx) = oneshot::channel();
        if let Some(resolved) = &state.resolved {
            let _ = tx.send(resolved.clone());
        } else {
            state.notify.push(tx);
        }
        Ok(rx)
    }

    /// Returns the resolved Domain if every partition has reported.
    /// Useful for direct inspection (e.g. EXPLAIN ANALYZE in A1.6); the
    /// subscribe path is the canonical way for workers to consume DFs.
    pub fn resolved(&self, df_id: DynamicFilterId) -> Option<&Domain> {
        self.states.get(&df_id).and_then(|s| s.resolved.as_ref())
    }

    /// Returns how many partitions the given DF expects in total.
    pub fn expected_partitions(&self, df_id: DynamicFilterId) -> Option<u32> {
        self.states.get(&df_id).map(|s| s.expected_partitions)
    }

    /// Clears all DF state and drops every pending sender. Receivers
    /// already handed out via [`subscribe`] will see `RecvError`.
    /// Called from `QueryCoordinator` on query end (A1.3 wiring).
    pub fn drop_query(&mut self) {
        self.states.clear();
        self.stage_partition_counts.clear();
    }

    /// Temporary diagnostic summary for unregister-time DF state.
    pub fn dfrpc_waiting_summary(&self) -> Vec<String> {
        self.states
            .iter()
            .filter_map(|(df_id, state)| {
                if state.resolved.is_some() {
                    None
                } else {
                    let mut reported: Vec<_> = state.partitions.keys().copied().collect();
                    reported.sort_unstable();
                    Some(format!(
                        "df_id={} reports={}/{} partitions={:?}",
                        df_id,
                        reported.len(),
                        state.expected_partitions,
                        reported
                    ))
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arneb_common::types::ScalarValue;

    fn distinct(vals: &[i64]) -> Domain {
        Domain::DistinctValues(vals.iter().copied().map(ScalarValue::Int64).collect())
    }

    #[test]
    fn normal_merge_subscribe_first_then_report() {
        let df = DynamicFilterId(0);
        let stage = StageId(7);
        let mut svc = DynamicFilterService::new();
        svc.register_query(&[(df, stage, 2)]);

        let mut rx = svc.subscribe(df).unwrap();
        // Receiver is pending until the second partition reports.
        assert!(matches!(
            rx.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));

        svc.report_partition(df, 0, distinct(&[1, 2])).unwrap();
        // Still pending — only 1 of 2 partitions in.
        assert!(matches!(
            rx.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));

        svc.report_partition(df, 1, distinct(&[2, 3])).unwrap();

        match rx.try_recv() {
            Ok(Domain::DistinctValues(mut vs)) => {
                vs.sort_by_key(|s| match s {
                    ScalarValue::Int64(x) => *x,
                    _ => unreachable!(),
                });
                assert_eq!(
                    vs,
                    vec![
                        ScalarValue::Int64(1),
                        ScalarValue::Int64(2),
                        ScalarValue::Int64(3)
                    ]
                );
            }
            other => panic!("expected merged DistinctValues, got {other:?}"),
        }

        // resolved() exposes the same value.
        assert!(svc.resolved(df).is_some());
        assert_eq!(svc.expected_partitions(df), Some(2));
    }

    #[test]
    fn subscribe_after_resolution_is_fast_path() {
        let df = DynamicFilterId(1);
        let mut svc = DynamicFilterService::new();
        svc.register_query(&[(df, StageId(0), 2)]);

        svc.report_partition(df, 0, distinct(&[10])).unwrap();
        svc.report_partition(df, 1, distinct(&[20])).unwrap();

        // Subscribe AFTER resolution; receiver must already be loaded.
        let mut rx = svc.subscribe(df).unwrap();
        let domain = rx.try_recv().expect("fast path receiver pre-fired");
        match domain {
            Domain::DistinctValues(vs) => assert_eq!(vs.len(), 2),
            other => panic!("expected DistinctValues, got {other:?}"),
        }
    }

    #[test]
    fn distinct_values_overflow_degrades_to_range() {
        let df = DynamicFilterId(2);
        // cap = 3 so the union (1..=4) exceeds cap and must degrade.
        let mut svc = DynamicFilterService::with_max_distinct_values(3);
        svc.register_query(&[(df, StageId(0), 2)]);

        svc.report_partition(df, 0, distinct(&[1, 2])).unwrap();
        svc.report_partition(df, 1, distinct(&[3, 4])).unwrap();

        let mut rx = svc.subscribe(df).unwrap();
        match rx.try_recv() {
            Ok(Domain::Range { min, max, .. }) => {
                assert_eq!(min, ScalarValue::Int64(1));
                assert_eq!(max, ScalarValue::Int64(4));
            }
            other => panic!("expected Range degradation, got {other:?}"),
        }
    }

    #[test]
    fn drop_query_closes_pending_subscribers() {
        let df = DynamicFilterId(3);
        let mut svc = DynamicFilterService::new();
        svc.register_query(&[(df, StageId(0), 2)]);

        let mut rx = svc.subscribe(df).unwrap();
        svc.drop_query();

        // Sender side is gone → receiver sees Closed (Err).
        assert!(matches!(
            rx.try_recv(),
            Err(oneshot::error::TryRecvError::Closed)
        ));
        // State is wiped.
        assert!(svc.resolved(df).is_none());
        assert!(svc.expected_partitions(df).is_none());
    }

    #[test]
    fn unknown_filter_id_errors() {
        let df = DynamicFilterId(99);
        let mut svc = DynamicFilterService::new();

        let err = svc.report_partition(df, 0, distinct(&[1])).unwrap_err();
        assert!(matches!(err, ExecutionError::InvalidOperation(_)));
        let err = svc.subscribe(df).unwrap_err();
        assert!(matches!(err, ExecutionError::InvalidOperation(_)));
    }

    #[test]
    fn duplicate_partition_reports_union_before_resolution() {
        let df = DynamicFilterId(4);
        let mut svc = DynamicFilterService::new();
        svc.register_query(&[(df, StageId(0), 2)]);

        svc.report_partition(df, 0, distinct(&[1])).unwrap();
        svc.report_partition(df, 0, distinct(&[2])).unwrap();
        svc.report_partition(df, 1, distinct(&[3])).unwrap();

        let mut rx = svc.subscribe(df).unwrap();
        match rx.try_recv() {
            Ok(Domain::DistinctValues(mut vs)) => {
                vs.sort_by_key(|s| match s {
                    ScalarValue::Int64(x) => *x,
                    _ => unreachable!(),
                });
                assert_eq!(
                    vs,
                    vec![
                        ScalarValue::Int64(1),
                        ScalarValue::Int64(2),
                        ScalarValue::Int64(3)
                    ]
                );
            }
            other => panic!("expected unioned DistinctValues, got {other:?}"),
        }
    }

    #[test]
    fn partition_index_out_of_range_errors() {
        let df = DynamicFilterId(5);
        let mut svc = DynamicFilterService::new();
        svc.register_query(&[(df, StageId(0), 2)]);

        // expected_partitions = 2 means valid indices are 0 and 1.
        let err = svc.report_partition(df, 2, distinct(&[1])).unwrap_err();
        assert!(matches!(err, ExecutionError::InvalidOperation(_)));
    }

    #[test]
    fn multiple_dfs_resolve_independently() {
        let a = DynamicFilterId(10);
        let b = DynamicFilterId(11);
        let mut svc = DynamicFilterService::new();
        svc.register_query(&[(a, StageId(0), 1), (b, StageId(1), 2)]);

        let mut rx_a = svc.subscribe(a).unwrap();
        let mut rx_b = svc.subscribe(b).unwrap();

        // a has only one partition so it resolves on the first report.
        svc.report_partition(a, 0, distinct(&[42])).unwrap();
        match rx_a.try_recv() {
            Ok(Domain::DistinctValues(vs)) => assert_eq!(vs, vec![ScalarValue::Int64(42)]),
            other => panic!("expected DistinctValues(42), got {other:?}"),
        }

        // b is still pending until its second partition arrives.
        assert!(matches!(
            rx_b.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        svc.report_partition(b, 0, distinct(&[100])).unwrap();
        svc.report_partition(b, 1, distinct(&[200])).unwrap();
        assert!(matches!(rx_b.try_recv(), Ok(Domain::DistinctValues(_))));
    }

    #[test]
    fn empty_then_full_same_partition_resolves_to_full() {
        let df = DynamicFilterId(12);
        let mut svc = DynamicFilterService::new();
        svc.register_query(&[(df, StageId(0), 2)]);

        svc.report_partition(df, 0, Domain::DistinctValues(Vec::new()))
            .unwrap();
        svc.report_partition(df, 0, distinct(&[7, 8])).unwrap();
        svc.report_partition(df, 1, Domain::DistinctValues(Vec::new()))
            .unwrap();

        let mut rx = svc.subscribe(df).unwrap();
        match rx.try_recv() {
            Ok(Domain::DistinctValues(mut vs)) => {
                vs.sort_by_key(|s| match s {
                    ScalarValue::Int64(x) => *x,
                    _ => unreachable!(),
                });
                assert_eq!(vs, vec![ScalarValue::Int64(7), ScalarValue::Int64(8)]);
            }
            other => panic!("expected non-empty DistinctValues, got {other:?}"),
        }
    }

    #[test]
    fn all_partitions_empty_resolves_to_empty_distinct_values() {
        let df = DynamicFilterId(13);
        let mut svc = DynamicFilterService::new();
        svc.register_query(&[(df, StageId(0), 2)]);

        svc.report_partition(df, 1, Domain::DistinctValues(Vec::new()))
            .unwrap();
        svc.report_partition(df, 0, Domain::DistinctValues(Vec::new()))
            .unwrap();

        let mut rx = svc.subscribe(df).unwrap();
        match rx.try_recv() {
            Ok(Domain::DistinctValues(vs)) => assert!(vs.is_empty()),
            other => panic!("expected empty DistinctValues, got {other:?}"),
        }
    }
}
