//! Per-task storage and subscription point for cross-fragment dynamic
//! filter Domains.
//!
//! Each worker task that has a probe-side scan with
//! `dynamic_filters_consumed` annotations owns one
//! [`DynamicFilterCollector`]. The collector is fed from two sources:
//!
//! 1. **At dispatch time** — DFs that resolved on the coordinator
//!    before the task was sent ride on
//!    `TaskDescriptor::pending_dynamic_filters`; the worker pre-loads
//!    them via [`DynamicFilterCollector::with_pending`].
//!
//! 2. **At runtime** — DFs that resolve later arrive via the
//!    `notify_dynamic_filter` Flight Action; the worker's notify
//!    callback finds the matching collector by `(QueryId, TaskId)`
//!    and calls [`DynamicFilterCollector::insert`].
//!
//! Consumers (e.g. `ScanExec` in A1.4) call
//! [`DynamicFilterCollector::take_receiver`] for each DF id they care
//! about. The returned `oneshot::Receiver<Domain>` either fires
//! immediately (if the Domain already arrived) or fires when the
//! producer calls `insert` (if the Domain is still pending). Multiple
//! scan partitions may subscribe to the same DF id; all pending
//! subscribers must receive the same resolved Domain. Consumers wrap
//! the receiver in `tokio::time::timeout` so they never block
//! indefinitely — soundness fallback is "no filter, scan everything".
//!
//! Storage is `tokio::sync::Mutex<HashMap<DfId, Slot>>`. The mutex is
//! per-task so contention is limited to the few notify pushes and
//! probe-side subscribes for that one task.

use std::collections::HashMap;
use std::sync::Arc;

use arneb_common::{Domain, DynamicFilterId};
use arneb_planner::PlanExpr;
use arneb_sql_parser::ast::BinaryOp;
use tokio::sync::{oneshot, Mutex};

fn dfrpc_domain_variant(domain: &Domain) -> String {
    match domain {
        Domain::DistinctValues(values) => format!("DistinctValues(len={})", values.len()),
        Domain::Range { .. } => "Range".to_string(),
        Domain::Bloom(_) => "Bloom".to_string(),
        Domain::All => "All".to_string(),
    }
}

/// Converts a resolved [`Domain`] into a [`PlanExpr`] filter that
/// `ScanExec` can hand to its underlying `DataSource` along with any
/// static filters.
///
/// Returns `None` when the Domain is trivially the no-op filter
/// (`Domain::All`) — the scan proceeds without an extra predicate.
///
/// Mapping (mirrors design.md D5 conversion rules):
/// - `DistinctValues(vs)` → `col IN (v1, v2, …)` via [`PlanExpr::InList`]
/// - `Range { min, max, .. }` → `col >= min AND col <= max`
/// - `Bloom` → `None` (carried separately through [`crate::ScanContext`])
/// - `All` → `None`
///
/// An empty `DistinctValues` (the build side produced zero rows) is
/// treated like `Domain::All` — the join's result is empty regardless,
/// so the probe scan is allowed to read freely. This keeps the
/// emitted predicates simple and avoids relying on `x IN ()` having
/// a well-defined truthiness in the expression evaluator. A1.5 may
/// revisit this when it ships a "scan-side skip" optimisation.
pub(crate) fn domain_to_filter_expr(
    domain: &Domain,
    column_index: usize,
    column_name: &str,
) -> Option<PlanExpr> {
    match domain {
        Domain::All => None,
        Domain::Bloom(_) => None,
        Domain::DistinctValues(values) if values.is_empty() => None,
        Domain::DistinctValues(values) => {
            let literals: Vec<PlanExpr> = values
                .iter()
                .map(|v| PlanExpr::Literal {
                    value: v.clone(),
                    span: None,
                })
                .collect();
            Some(PlanExpr::InList {
                expr: Box::new(PlanExpr::Column {
                    index: column_index,
                    name: column_name.to_string(),
                    span: None,
                }),
                list: literals,
                negated: false,
                span: None,
            })
        }
        Domain::Range { min, max, .. } => {
            let col = PlanExpr::Column {
                index: column_index,
                name: column_name.to_string(),
                span: None,
            };
            let lower = PlanExpr::BinaryOp {
                left: Box::new(col.clone()),
                op: BinaryOp::GtEq,
                right: Box::new(PlanExpr::Literal {
                    value: min.clone(),
                    span: None,
                }),
                span: None,
            };
            let upper = PlanExpr::BinaryOp {
                left: Box::new(col),
                op: BinaryOp::LtEq,
                right: Box::new(PlanExpr::Literal {
                    value: max.clone(),
                    span: None,
                }),
                span: None,
            };
            Some(PlanExpr::BinaryOp {
                left: Box::new(lower),
                op: BinaryOp::And,
                right: Box::new(upper),
                span: None,
            })
        }
    }
}

/// Per-task dynamic filter store. Cheap to `clone()` — shares the
/// inner `Arc<Mutex<_>>` so the FlightState notify callback and the
/// scan operator both see the same set.
#[derive(Debug, Clone, Default)]
pub struct DynamicFilterCollector {
    inner: Arc<Mutex<HashMap<DynamicFilterId, Slot>>>,
}

#[derive(Debug)]
enum Slot {
    /// Consumers subscribed before the Domain arrived; every sender
    /// held here fires when [`DynamicFilterCollector::insert`] is called.
    Pending(Vec<oneshot::Sender<Domain>>),
    /// The Domain has arrived. Cached so any future subscriber gets
    /// the value synchronously via a pre-fired receiver.
    Resolved(Domain),
}

impl DynamicFilterCollector {
    /// Creates an empty collector.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a collector pre-loaded with DFs that resolved before
    /// the task was dispatched (from `TaskDescriptor::pending_dynamic_filters`).
    pub fn with_pending(initial: impl IntoIterator<Item = (DynamicFilterId, Domain)>) -> Self {
        let map: HashMap<_, _> = initial
            .into_iter()
            .map(|(id, d)| (id, Slot::Resolved(d)))
            .collect();
        Self {
            inner: Arc::new(Mutex::new(map)),
        }
    }

    /// Records a Domain for `df_id`. If consumers subscribed first
    /// via [`take_receiver`](Self::take_receiver), this fires all of
    /// their senders. Otherwise the Domain is cached for future
    /// subscribers.
    ///
    /// Replacing an already-resolved Domain is treated as last-wins;
    /// the coord only sends one notify per resolution, so this should
    /// not happen in normal flows.
    pub async fn insert(&self, df_id: DynamicFilterId, domain: Domain) {
        let mut inner = self.inner.lock().await;
        let prior_slot = match inner.get(&df_id) {
            Some(Slot::Pending(_)) => "pending",
            Some(Slot::Resolved(_)) => "resolved",
            None => "none",
        };
        eprintln!(
            "[DFRPC] worker collector insert df_id={} domain={} prior_slot={}",
            df_id,
            dfrpc_domain_variant(&domain),
            prior_slot
        );
        match inner.remove(&df_id) {
            Some(Slot::Pending(txs)) => {
                for tx in txs {
                    eprintln!(
                        "[DFRPC] worker collector fire_receiver df_id={} domain={}",
                        df_id,
                        dfrpc_domain_variant(&domain)
                    );
                    let _ = tx.send(domain.clone());
                }
                inner.insert(df_id, Slot::Resolved(domain));
            }
            Some(Slot::Resolved(_)) | None => {
                inner.insert(df_id, Slot::Resolved(domain));
            }
        }
    }

    /// Returns a `oneshot::Receiver<Domain>` for `df_id`.
    ///
    /// - If the Domain has already arrived, the receiver is pre-fired
    ///   and `recv` returns immediately.
    /// - Otherwise the collector reserves a slot and returns a
    ///   receiver that fires when [`insert`](Self::insert) is called.
    ///
    /// The collector keeps every pending sender per DF id so
    /// data-parallel scan partitions all receive the same resolved
    /// Domain. Late subscribers after resolution get a pre-fired
    /// receiver from the cached Domain.
    pub async fn take_receiver(&self, df_id: DynamicFilterId) -> oneshot::Receiver<Domain> {
        let mut inner = self.inner.lock().await;
        let prior_slot = match inner.get(&df_id) {
            Some(Slot::Pending(_)) => "pending",
            Some(Slot::Resolved(_)) => "resolved",
            None => "none",
        };
        eprintln!(
            "[DFRPC] worker collector take_receiver subscribed_df_id={} prior_slot={}",
            df_id, prior_slot
        );
        match inner.get_mut(&df_id) {
            Some(Slot::Resolved(d)) => {
                // Fast path: pre-fire the receiver with the cached value.
                let (tx, rx) = oneshot::channel();
                let _ = tx.send(d.clone());
                rx
            }
            Some(Slot::Pending(txs)) => {
                let (tx, rx) = oneshot::channel();
                txs.push(tx);
                rx
            }
            None => {
                let (tx, rx) = oneshot::channel();
                inner.insert(df_id, Slot::Pending(vec![tx]));
                rx
            }
        }
    }

    /// Returns the cached Domain if the DF has resolved, else `None`.
    /// Useful for tests and EXPLAIN ANALYZE (A1.6); the consumer path
    /// for ScanExec uses [`take_receiver`] so it can `tokio::timeout`.
    pub async fn get(&self, df_id: DynamicFilterId) -> Option<Domain> {
        let inner = self.inner.lock().await;
        match inner.get(&df_id) {
            Some(Slot::Resolved(d)) => Some(d.clone()),
            _ => None,
        }
    }

    /// Number of slots (resolved or pending). Useful for tests.
    pub async fn len(&self) -> usize {
        self.inner.lock().await.len()
    }

    /// True when no slot has been created yet.
    pub async fn is_empty(&self) -> bool {
        self.inner.lock().await.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arneb_common::types::ScalarValue;

    fn distinct(vals: &[i64]) -> Domain {
        Domain::DistinctValues(vals.iter().copied().map(ScalarValue::Int64).collect())
    }

    #[tokio::test]
    async fn empty_collector() {
        let c = DynamicFilterCollector::new();
        assert!(c.is_empty().await);
        assert_eq!(c.len().await, 0);
        assert!(c.get(DynamicFilterId(0)).await.is_none());
    }

    #[tokio::test]
    async fn insert_then_get() {
        let c = DynamicFilterCollector::new();
        c.insert(DynamicFilterId(0), distinct(&[1, 2])).await;
        assert_eq!(c.len().await, 1);
        assert_eq!(c.get(DynamicFilterId(0)).await, Some(distinct(&[1, 2])));
    }

    #[tokio::test]
    async fn with_pending_preloads_as_resolved() {
        let c = DynamicFilterCollector::with_pending([
            (DynamicFilterId(0), Domain::All),
            (DynamicFilterId(1), distinct(&[5])),
        ]);
        assert_eq!(c.len().await, 2);
        assert!(matches!(c.get(DynamicFilterId(0)).await, Some(Domain::All)));
    }

    #[tokio::test]
    async fn insert_replaces_existing() {
        let c = DynamicFilterCollector::new();
        c.insert(DynamicFilterId(0), distinct(&[1])).await;
        c.insert(DynamicFilterId(0), distinct(&[2, 3])).await;
        assert_eq!(c.len().await, 1);
        assert_eq!(c.get(DynamicFilterId(0)).await, Some(distinct(&[2, 3])));
    }

    #[tokio::test]
    async fn clone_shares_state() {
        let c1 = DynamicFilterCollector::new();
        let c2 = c1.clone();
        c1.insert(DynamicFilterId(0), Domain::All).await;
        assert_eq!(c2.len().await, 1);
    }

    #[tokio::test]
    async fn take_receiver_fast_path_when_resolved() {
        let c = DynamicFilterCollector::with_pending([(DynamicFilterId(0), distinct(&[7]))]);
        let mut rx = c.take_receiver(DynamicFilterId(0)).await;
        assert_eq!(
            rx.try_recv().expect("fast-path receiver pre-fired"),
            distinct(&[7])
        );
    }

    #[tokio::test]
    async fn take_receiver_then_insert_fires() {
        let c = DynamicFilterCollector::new();
        let mut rx = c.take_receiver(DynamicFilterId(0)).await;
        // No Domain yet — receiver is pending.
        assert!(matches!(
            rx.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        c.insert(DynamicFilterId(0), distinct(&[99])).await;
        assert_eq!(rx.try_recv().unwrap(), distinct(&[99]));
    }

    #[tokio::test]
    async fn pending_receivers_fan_out_and_late_subscriber_gets_cached_domain() {
        let c = DynamicFilterCollector::new();
        let domain = distinct(&[11, 13, 17]);
        let mut receivers = Vec::new();

        for _ in 0..8 {
            receivers.push(c.take_receiver(DynamicFilterId(0)).await);
        }
        for rx in &mut receivers {
            assert!(matches!(
                rx.try_recv(),
                Err(oneshot::error::TryRecvError::Empty)
            ));
        }

        c.insert(DynamicFilterId(0), domain.clone()).await;
        for rx in &mut receivers {
            assert_eq!(rx.try_recv().unwrap(), domain);
        }

        let mut late_rx = c.take_receiver(DynamicFilterId(0)).await;
        assert_eq!(
            late_rx.try_recv().expect("late subscriber pre-fired"),
            domain
        );
    }

    #[tokio::test]
    async fn drop_collector_closes_pending_receiver() {
        let c = DynamicFilterCollector::new();
        let mut rx = c.take_receiver(DynamicFilterId(0)).await;
        drop(c); // Sender goes away.
        assert!(matches!(
            rx.try_recv(),
            Err(oneshot::error::TryRecvError::Closed)
        ));
    }

    // --- domain_to_filter_expr tests --------------------------------

    #[test]
    fn convert_all_yields_no_filter() {
        assert!(domain_to_filter_expr(&Domain::All, 0, "c").is_none());
    }

    #[test]
    fn convert_empty_distinct_yields_no_filter() {
        // Defensive: empty build side → join is empty regardless;
        // skipping the filter is sound.
        assert!(domain_to_filter_expr(&Domain::DistinctValues(vec![]), 0, "c").is_none());
    }

    #[test]
    fn convert_distinct_yields_inlist() {
        let d = distinct(&[1, 2, 3]);
        let expr = domain_to_filter_expr(&d, 4, "l_partkey").unwrap();
        match expr {
            PlanExpr::InList {
                expr,
                list,
                negated,
                ..
            } => {
                assert!(!negated);
                assert_eq!(list.len(), 3);
                match *expr {
                    PlanExpr::Column { index, name, .. } => {
                        assert_eq!(index, 4);
                        assert_eq!(name, "l_partkey");
                    }
                    other => panic!("expected Column, got {other:?}"),
                }
            }
            other => panic!("expected InList, got {other:?}"),
        }
    }

    #[test]
    fn convert_range_yields_conjoined_inequalities() {
        let d = Domain::Range {
            min: ScalarValue::Int64(10),
            max: ScalarValue::Int64(20),
            nullable: false,
        };
        let expr = domain_to_filter_expr(&d, 2, "o_orderkey").unwrap();
        // Outer is AND
        match expr {
            PlanExpr::BinaryOp {
                op, left, right, ..
            } => {
                assert!(matches!(op, BinaryOp::And));
                // Lower bound: col >= min
                match *left {
                    PlanExpr::BinaryOp {
                        op: BinaryOp::GtEq, ..
                    } => {}
                    other => panic!("expected col >= min, got {other:?}"),
                }
                // Upper bound: col <= max
                match *right {
                    PlanExpr::BinaryOp {
                        op: BinaryOp::LtEq, ..
                    } => {}
                    other => panic!("expected col <= max, got {other:?}"),
                }
            }
            other => panic!("expected BinaryOp(And), got {other:?}"),
        }
    }
}
