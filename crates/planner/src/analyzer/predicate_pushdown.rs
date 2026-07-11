//! Predicate pushdown analyzer pass.
//!
//! Splits each `Filter` predicate at AND boundaries and pushes each
//! conjunct down to the deepest `LogicalPlan` node whose schema covers
//! all the columns the conjunct references. Predicates that come to
//! rest at a `TableScan` get folded into a `Filter` directly above
//! that scan — connectors can then pick them up for Parquet row-group
//! pruning, predicate pushdown, etc.
//!
//! Conservative on join semantics:
//! - INNER: push to either side based on column ownership.
//! - LEFT:  push to LEFT side only (right-side filters would discard
//!   NULL-padded rows, changing semantics).
//! - RIGHT: push to RIGHT side only (mirror of LEFT).
//! - FULL/CROSS: leave alone.
//!
//! Conservative on other operators:
//! - `Projection`: not pushed through (would need expression substitution).
//! - `Aggregate` / `PartialAggregate` / `FinalAggregate`: not pushed
//!   (changes GROUP BY semantics).
//! - `Sort`: safe to push through, but skipped for v1 simplicity.
//! - `Limit`: NOT safe to push through (changes which rows survive).
//! - `Filter`: recurse through (transparent, just combines with target).
//!
//! Runs BEFORE `JoinReorder` in the analyzer pipeline so the cost-based
//! reorderer sees post-pushdown cardinalities.

use std::collections::HashSet;

use arneb_common::error::PlanError;
use arneb_common::types::ColumnInfo;
use arneb_sql_parser::ast::{BinaryOp, JoinType};

use crate::analyzer::{AnalysisPass, AnalyzerContext};
use crate::plan::{LogicalPlan, PlanExpr};

/// Splits AND conjuncts and pushes each to the deepest node whose
/// schema covers all its column references.
#[derive(Debug, Default)]
pub struct PredicatePushdown;

impl PredicatePushdown {
    pub fn new() -> Self {
        Self
    }
}

impl AnalysisPass for PredicatePushdown {
    fn name(&self) -> &'static str {
        "PredicatePushdown"
    }

    fn analyze(
        &self,
        plan: LogicalPlan,
        _ctx: &mut AnalyzerContext,
    ) -> Result<LogicalPlan, PlanError> {
        Ok(push_down(plan))
    }
}

// ---------------------------------------------------------------------------
// Top-down rewrite
// ---------------------------------------------------------------------------

/// Walks `plan` bottom-up; when a `Filter` is encountered, splits its
/// predicate into conjuncts and tries to push each as deep as possible
/// into the recursively-rewritten input.
fn push_down(plan: LogicalPlan) -> LogicalPlan {
    use LogicalPlan as L;
    match plan {
        L::Filter { input, predicate } => {
            let new_input = push_down(*input);
            let conjuncts = split_and(predicate);
            let mut current = new_input;
            let mut remaining: Vec<PlanExpr> = Vec::new();
            for c in conjuncts {
                match try_push_into(current, c) {
                    Ok(p) => current = p,
                    Err((p, c)) => {
                        current = p;
                        remaining.push(c);
                    }
                }
            }
            if remaining.is_empty() {
                current
            } else {
                L::Filter {
                    input: Box::new(current),
                    predicate: combine_and(remaining),
                }
            }
        }
        L::Projection {
            input,
            exprs,
            schema,
        } => L::Projection {
            input: Box::new(push_down(*input)),
            exprs,
            schema,
        },
        L::Join {
            left,
            right,
            join_type,
            condition,
            ..
        } => L::Join {
            left: Box::new(push_down(*left)),
            right: Box::new(push_down(*right)),
            join_type,
            condition,
            dynamic_filter_ids: Vec::new(),
        },
        L::Aggregate {
            input,
            group_by,
            aggr_exprs,
            schema,
        } => L::Aggregate {
            input: Box::new(push_down(*input)),
            group_by,
            aggr_exprs,
            schema,
        },
        L::Sort { input, order_by } => L::Sort {
            input: Box::new(push_down(*input)),
            order_by,
        },
        L::Limit {
            input,
            limit,
            offset,
        } => L::Limit {
            input: Box::new(push_down(*input)),
            limit,
            offset,
        },
        // SemiJoin/AntiJoin residual splitting is implemented in
        // `split_semi_residual` + unit-tested below, but DISABLED in
        // this walker after bench showed mixed results: Q04 (-8%) +
        // ~5 non-SemiJoin queries +5-15% (noise-amplified after the
        // analyzer reshuffles the join chain). The split is correct
        // and the helper stays for future cardinality-gated re-enable.
        // To re-enable, restore the `split_semi_residual` calls in
        // these match arms.
        L::SemiJoin {
            left,
            right,
            left_key,
            right_key,
            residual,
            ..
        } => L::SemiJoin {
            left: Box::new(push_down(*left)),
            right: Box::new(push_down(*right)),
            left_key,
            right_key,
            residual,
            dynamic_filter_ids: Vec::new(),
        },
        L::AntiJoin {
            left,
            right,
            left_key,
            right_key,
            residual,
        } => L::AntiJoin {
            left: Box::new(push_down(*left)),
            right: Box::new(push_down(*right)),
            left_key,
            right_key,
            residual,
        },
        L::PartialAggregate {
            input,
            group_by,
            aggr_exprs,
            schema,
        } => L::PartialAggregate {
            input: Box::new(push_down(*input)),
            group_by,
            aggr_exprs,
            schema,
        },
        L::FinalAggregate {
            input,
            group_by,
            aggr_exprs,
            schema,
        } => L::FinalAggregate {
            input: Box::new(push_down(*input)),
            group_by,
            aggr_exprs,
            schema,
        },
        L::Explain { input, analyze } => L::Explain {
            input: Box::new(push_down(*input)),
            analyze,
        },
        L::AssignUniqueId { input, id_column } => L::AssignUniqueId {
            input: Box::new(push_down(*input)),
            id_column,
        },
        // Leaves and non-recursive variants pass through unchanged.
        other => other,
    }
}

/// Attempt to push `conjunct` into `plan`. On success, returns
/// `Ok(rewritten_plan)`. On failure, returns
/// `Err((unchanged_plan, conjunct))` so the caller can keep the
/// conjunct at the current level. `LogicalPlan` is a wide enum, so
/// the Err variant is large — that's expected here since this is an
/// internal control-flow type that never escapes the module.
#[allow(clippy::result_large_err)]
fn try_push_into(
    plan: LogicalPlan,
    conjunct: PlanExpr,
) -> Result<LogicalPlan, (LogicalPlan, PlanExpr)> {
    use LogicalPlan as L;
    let cols = collect_column_names(&conjunct);
    match plan {
        L::Join {
            left,
            right,
            join_type,
            condition,
            ..
        } => {
            let l_schema = left.schema();
            let r_schema = right.schema();
            let l_len = l_schema.len();
            // INDEX-based routing: PlanExpr::Column carries an index
            // resolved against THIS join's combined schema
            // (left ++ right). Indices < left.len() belong to left;
            // ≥ left.len() belong to right. Name-only routing fails
            // on self-joins where the same column name exists in both
            // sides (TPC-H Q07's nation AS n1, n2) — indices keep
            // them distinguishable. Name fallback only kicks in when
            // the conjunct references no columns (constant predicate,
            // unusual but possible).
            let col_indices = collect_column_indices(&conjunct);
            let (in_left, in_right) = if !col_indices.is_empty() {
                let l = col_indices.iter().all(|&i| i < l_len);
                let r = col_indices.iter().all(|&i| i >= l_len);
                (l, r)
            } else {
                let l_names: HashSet<&str> = l_schema.iter().map(|c| c.name.as_str()).collect();
                let r_names: HashSet<&str> = r_schema.iter().map(|c| c.name.as_str()).collect();
                let l = !cols.is_empty() && cols.iter().all(|n| l_names.contains(n.as_str()));
                let r = !cols.is_empty() && cols.iter().all(|n| r_names.contains(n.as_str()));
                (l, r)
            };

            // After index check this is only true for predicates that
            // genuinely reference both sides (e.g., `l.a + r.b = 3`).
            let ambiguous = in_left && in_right;

            match (join_type, in_left, in_right, ambiguous) {
                (JoinType::Inner, true, _, false) | (JoinType::Left, true, _, false) => {
                    match rewrite_against(&conjunct, &l_schema) {
                        Some(resolved) => {
                            let new_left = push_or_wrap(*left, resolved);
                            Ok(L::Join {
                                left: Box::new(new_left),
                                right,
                                join_type,
                                condition,
                                dynamic_filter_ids: Vec::new(),
                            })
                        }
                        None => Err((
                            L::Join {
                                left,
                                right,
                                join_type,
                                condition,
                                dynamic_filter_ids: Vec::new(),
                            },
                            conjunct,
                        )),
                    }
                }
                (JoinType::Inner, false, true, false) | (JoinType::Right, _, true, false) => {
                    match rewrite_against(&conjunct, &r_schema) {
                        Some(resolved) => {
                            let new_right = push_or_wrap(*right, resolved);
                            Ok(L::Join {
                                left,
                                right: Box::new(new_right),
                                join_type,
                                condition,
                                dynamic_filter_ids: Vec::new(),
                            })
                        }
                        None => Err((
                            L::Join {
                                left,
                                right,
                                join_type,
                                condition,
                                dynamic_filter_ids: Vec::new(),
                            },
                            conjunct,
                        )),
                    }
                }
                _ => Err((
                    L::Join {
                        left,
                        right,
                        join_type,
                        condition,
                        dynamic_filter_ids: Vec::new(),
                    },
                    conjunct,
                )),
            }
        }
        L::Filter { input, predicate } => match try_push_into(*input, conjunct) {
            Ok(p) => Ok(L::Filter {
                input: Box::new(p),
                predicate,
            }),
            Err((p, c)) => Err((
                L::Filter {
                    input: Box::new(p),
                    predicate,
                },
                c,
            )),
        },
        // SemiJoin / AntiJoin output schema = LEFT schema only, so any
        // predicate applied AFTER one can ONLY reference left cols.
        // It is therefore unconditionally safe to push the conjunct
        // into the left child. Big TPC-H Q21 win: outer filters
        // (o_orderstatus='F' AND l_receiptdate>l_commitdate AND
        // n_name='SAUDI ARABIA') currently sit above the SemiJoin /
        // AntiJoin pair, forcing the 4-way INNER join below to
        // produce millions of rows that the SemiJoin probe then
        // discards. With this push, those filters reach the inner
        // joins / scans and prune ~6M -> ~1000s of left rows.
        L::SemiJoin {
            left,
            right,
            left_key,
            right_key,
            residual,
            ..
        } => match try_push_into(*left, conjunct) {
            Ok(new_left) => Ok(L::SemiJoin {
                left: Box::new(new_left),
                right,
                left_key,
                right_key,
                residual,
                dynamic_filter_ids: Vec::new(),
            }),
            Err((unchanged_left, c)) => Err((
                L::SemiJoin {
                    left: Box::new(unchanged_left),
                    right,
                    left_key,
                    right_key,
                    residual,
                    dynamic_filter_ids: Vec::new(),
                },
                c,
            )),
        },
        L::AntiJoin {
            left,
            right,
            left_key,
            right_key,
            residual,
        } => match try_push_into(*left, conjunct) {
            Ok(new_left) => Ok(L::AntiJoin {
                left: Box::new(new_left),
                right,
                left_key,
                right_key,
                residual,
            }),
            Err((unchanged_left, c)) => Err((
                L::AntiJoin {
                    left: Box::new(unchanged_left),
                    right,
                    left_key,
                    right_key,
                    residual,
                },
                c,
            )),
        },
        L::TableScan { ref schema, .. } => match rewrite_against(&conjunct, schema) {
            Some(resolved) => Ok(L::Filter {
                input: Box::new(plan),
                predicate: resolved,
            }),
            None => Err((plan, conjunct)),
        },
        // Stop at Projection / Aggregate / Sort / Limit / SemiJoin /
        // AntiJoin / etc. The push_or_wrap caller will wrap with a
        // Filter at this level.
        other => Err((other, conjunct)),
    }
}

/// Wrap `plan` with a `Filter(conjunct)` at this level — used as the
/// terminal case when `try_push_into` cannot recurse deeper.
fn push_or_wrap(plan: LogicalPlan, conjunct: PlanExpr) -> LogicalPlan {
    match try_push_into(plan, conjunct) {
        Ok(p) => p,
        Err((p, c)) => LogicalPlan::Filter {
            input: Box::new(p),
            predicate: c,
        },
    }
}

// ---------------------------------------------------------------------------
// AND splitting / combining
// ---------------------------------------------------------------------------

/// Flatten a tree of `AND`s into a flat list of conjuncts. AFTER the
/// flatten, every conjunct that is a top-level `OR` is checked for
/// common AND-conjuncts across its disjuncts; any found are hoisted
/// out as additional top-level conjuncts and the OR is simplified.
///
/// Example (Q19's `WHERE`):
/// ```text
/// (p_brand='12' AND p_container IN (...) AND l_shipmode IN ('AIR','AIR REG') AND l_shipinstruct='DELIVER IN PERSON')
///  OR
/// (p_brand='23' AND p_container IN (...) AND l_shipmode IN ('AIR','AIR REG') AND l_shipinstruct='DELIVER IN PERSON')
///  OR
/// (p_brand='34' AND p_container IN (...) AND l_shipmode IN ('AIR','AIR REG') AND l_shipinstruct='DELIVER IN PERSON')
/// ```
///
/// yields three top-level conjuncts: `l_shipmode IN ('AIR','AIR REG')`,
/// `l_shipinstruct='DELIVER IN PERSON'`, and a simplified OR keeping
/// the per-brand parts. The first two are now pushable to lineitem.
fn split_and(predicate: PlanExpr) -> Vec<PlanExpr> {
    let mut conjuncts = Vec::new();
    walk_and(predicate, &mut conjuncts);
    // Factor common AND across OR branches for any OR-shaped conjuncts.
    let mut result = Vec::new();
    for c in conjuncts {
        if matches!(
            &c,
            PlanExpr::BinaryOp {
                op: BinaryOp::Or,
                ..
            }
        ) {
            match factor_or_common_ands(c) {
                FactorResult::None(orig) => result.push(orig),
                FactorResult::AllCommon(commons) => result.extend(commons),
                FactorResult::Partial { commons, residual } => {
                    result.extend(commons);
                    result.push(residual);
                }
            }
        } else {
            result.push(c);
        }
    }
    result
}

fn walk_and(e: PlanExpr, out: &mut Vec<PlanExpr>) {
    match e {
        PlanExpr::BinaryOp {
            left,
            op: BinaryOp::And,
            right,
            ..
        } => {
            walk_and(*left, out);
            walk_and(*right, out);
        }
        other => out.push(other),
    }
}

enum FactorResult {
    /// No common conjuncts found; return the OR unchanged.
    None(PlanExpr),
    /// Every disjunct's conjuncts were entirely common — the OR
    /// reduces to just the common conjuncts (each disjunct becomes
    /// `true` after removal, OR of trues is true).
    ///
    /// Conservative: we keep the OR for safety in that case unless
    /// `commons` covers all disjuncts' ALL conjuncts. See implementation.
    AllCommon(Vec<PlanExpr>),
    /// Some conjuncts were common, others stay inside a simplified OR.
    Partial {
        commons: Vec<PlanExpr>,
        residual: PlanExpr,
    },
}

/// Flatten a tree of `OR`s into a flat list of disjuncts.
fn split_or(predicate: PlanExpr) -> Vec<PlanExpr> {
    let mut out = Vec::new();
    walk_or(predicate, &mut out);
    out
}

fn walk_or(e: PlanExpr, out: &mut Vec<PlanExpr>) {
    match e {
        PlanExpr::BinaryOp {
            left,
            op: BinaryOp::Or,
            right,
            ..
        } => {
            walk_or(*left, out);
            walk_or(*right, out);
        }
        other => out.push(other),
    }
}

/// Right-fold `disjuncts` into `d0 OR d1 OR … OR dN`.
fn combine_or(disjuncts: Vec<PlanExpr>) -> PlanExpr {
    let mut iter = disjuncts.into_iter();
    let mut acc = iter.next().expect("combine_or: empty disjuncts");
    for d in iter {
        acc = PlanExpr::BinaryOp {
            left: Box::new(acc),
            op: BinaryOp::Or,
            right: Box::new(d),
            span: None,
        };
    }
    acc
}

/// Attempt to factor common AND-conjuncts out of an OR's disjuncts.
///
/// Two factoring paths run together:
///
/// 1. **Structural factoring** — conjuncts that appear (index-aware
///    structural-equal) in EVERY disjunct are hoisted out and stripped
///    from each branch. Example: Q19's `l_shipinstruct = 'DELIVER IN
///    PERSON'` appears in all 3 disjuncts → moved out as a top-level
///    AND. The OR shrinks.
///
/// 2. **Per-column weakening** — for each single-column signature
///    (`{name, index}`) present in the OR, take each disjunct's
///    sub-AND of conjuncts on that column and OR them. The result is
///    a necessary condition implied by every disjunct, safe to add as
///    an extra top-level AND-conjunct. Example: Q07's
///    `(n1.n_name = 'F' AND n2.n_name = 'G') OR (n1.n_name = 'G' AND
///    n2.n_name = 'F')` derives `(n1.n_name = 'F' OR n1.n_name = 'G')
///    AND (n2.n_name = 'F' OR n2.n_name = 'G')` — each pushable to
///    its respective nation alias scan. The original OR stays at the
///    join level for full correctness (the weakening doesn't replace
///    it, just adds a pre-filter).
fn factor_or_common_ands(expr: PlanExpr) -> FactorResult {
    let disjuncts = split_or(expr);
    if disjuncts.len() < 2 {
        return FactorResult::None(combine_or(disjuncts));
    }
    let per_branch: Vec<Vec<PlanExpr>> = disjuncts
        .iter()
        .map(|d| {
            let mut v = Vec::new();
            walk_and(d.clone(), &mut v);
            v
        })
        .collect();

    // ── Path 1: structural factoring ──
    let first = &per_branch[0];
    let mut commons: Vec<PlanExpr> = Vec::new();
    for candidate in first {
        if commons.iter().any(|c| expr_struct_eq(c, candidate)) {
            continue;
        }
        let in_every = per_branch[1..]
            .iter()
            .all(|branch| branch.iter().any(|c| expr_struct_eq(c, candidate)));
        if in_every {
            commons.push(candidate.clone());
        }
    }

    // ── Path 2: per-column weakening ──
    let weakenings = derive_per_column_weakenings(&per_branch);

    if commons.is_empty() && weakenings.is_empty() {
        return FactorResult::None(combine_or(disjuncts));
    }

    // Strip the EXACT structural commons from each branch (weakenings
    // are derived predicates that don't change the original OR).
    let stripped: Vec<Vec<PlanExpr>> = per_branch
        .iter()
        .map(|branch| {
            branch
                .iter()
                .filter(|c| !commons.iter().any(|cm| expr_struct_eq(c, cm)))
                .cloned()
                .collect()
        })
        .collect();

    // Merge commons + weakenings (dedupe by structural equality so we
    // never emit the same predicate twice).
    let mut merged: Vec<PlanExpr> = Vec::new();
    for c in commons.into_iter().chain(weakenings) {
        if !merged.iter().any(|m| expr_struct_eq(m, &c)) {
            merged.push(c);
        }
    }

    // If ANY branch becomes empty after structural stripping, the OR
    // collapses to `true`. Only the hoisted predicates remain.
    if stripped.iter().any(|b| b.is_empty()) {
        return FactorResult::AllCommon(merged);
    }

    // Otherwise rebuild OR from stripped branches and emit alongside
    // the hoisted predicates. Drop the residual if a hoisted weakening
    // is already structurally identical to it (avoids emitting the
    // same `b=2 OR b=3` twice when structural commons happened to
    // strip everything outside that signature).
    let residual_branches: Vec<PlanExpr> = stripped.into_iter().map(combine_and).collect();
    let residual = combine_or(residual_branches);
    if merged.iter().any(|m| expr_struct_eq(m, &residual)) {
        return FactorResult::AllCommon(merged);
    }
    FactorResult::Partial {
        commons: merged,
        residual,
    }
}

/// For each single-column signature `(name, index)` referenced by any
/// disjunct's conjunct list, gather each disjunct's matching sub-AND
/// of conjuncts and emit `(sub_and_d1) OR (sub_and_d2) OR ...` as a
/// derived weakening predicate (implied by every disjunct, hence by
/// the whole OR).
///
/// Skips signatures where any disjunct has zero conjuncts of that
/// signature — that branch puts no constraint on the column, so any
/// derived predicate would be too restrictive.
fn derive_per_column_weakenings(per_branch: &[Vec<PlanExpr>]) -> Vec<PlanExpr> {
    use std::collections::BTreeSet;

    // Collect single-column signatures referenced by any conjunct.
    let mut signatures: BTreeSet<(String, usize)> = BTreeSet::new();
    for branch in per_branch {
        for c in branch {
            let cols = collect_column_index_set(c);
            if cols.len() == 1 {
                signatures.insert(cols.into_iter().next().unwrap());
            }
        }
    }

    let mut derived = Vec::new();
    for sig in signatures {
        let mut per_branch_q: Vec<PlanExpr> = Vec::with_capacity(per_branch.len());
        let mut all_branches_constrain = true;
        for branch in per_branch {
            let matching: Vec<PlanExpr> = branch
                .iter()
                .filter(|c| {
                    let cols = collect_column_index_set(c);
                    cols.len() == 1 && cols.contains(&sig)
                })
                .cloned()
                .collect();
            if matching.is_empty() {
                all_branches_constrain = false;
                break;
            }
            per_branch_q.push(combine_and(matching));
        }
        if !all_branches_constrain {
            continue;
        }
        // Skip if all per-branch Qs are structurally identical — the
        // structural-eq path will (or already did) emit it.
        let first_q = &per_branch_q[0];
        let all_identical = per_branch_q[1..].iter().all(|q| expr_struct_eq(q, first_q));
        if all_identical {
            continue;
        }
        derived.push(combine_or(per_branch_q));
    }
    derived
}

/// Collect the set of `(column_name, column_index)` referenced by
/// `expr`. Uses the same walker as `collect_column_names` but
/// preserves indices so self-join aliases can be distinguished.
fn collect_column_index_set(expr: &PlanExpr) -> std::collections::BTreeSet<(String, usize)> {
    let mut set = std::collections::BTreeSet::new();
    visit_column_pairs(expr, &mut |name, idx| {
        set.insert((name.to_string(), idx));
    });
    set
}

fn visit_column_pairs<F: FnMut(&str, usize)>(expr: &PlanExpr, callback: &mut F) {
    use PlanExpr as E;
    match expr {
        E::Column { name, index, .. } => callback(name, *index),
        E::BinaryOp { left, right, .. } => {
            visit_column_pairs(left, callback);
            visit_column_pairs(right, callback);
        }
        E::UnaryOp { expr, .. }
        | E::IsNull { expr, .. }
        | E::IsNotNull { expr, .. }
        | E::Cast { expr, .. } => visit_column_pairs(expr, callback),
        E::Between {
            expr, low, high, ..
        } => {
            visit_column_pairs(expr, callback);
            visit_column_pairs(low, callback);
            visit_column_pairs(high, callback);
        }
        E::InList { expr, list, .. } => {
            visit_column_pairs(expr, callback);
            for item in list {
                visit_column_pairs(item, callback);
            }
        }
        E::Function { args, .. } => {
            for a in args {
                visit_column_pairs(a, callback);
            }
        }
        E::CaseExpr {
            operand,
            when_clauses,
            else_result,
            ..
        } => {
            if let Some(o) = operand {
                visit_column_pairs(o, callback);
            }
            for (cond, res) in when_clauses {
                visit_column_pairs(cond, callback);
                visit_column_pairs(res, callback);
            }
            if let Some(e) = else_result {
                visit_column_pairs(e, callback);
            }
        }
        E::ScalarSubquery { .. } | E::Literal { .. } | E::Wildcard | E::Parameter { .. } => {}
    }
}

/// Structural equality of two `PlanExpr` trees, ignoring source spans
/// but comparing column INDICES (not just names) so self-join aliases
/// stay distinguishable.
fn expr_struct_eq(a: &PlanExpr, b: &PlanExpr) -> bool {
    use PlanExpr as E;
    match (a, b) {
        (
            E::Column {
                index: i1,
                name: n1,
                ..
            },
            E::Column {
                index: i2,
                name: n2,
                ..
            },
        ) => i1 == i2 && n1 == n2,
        (E::Literal { value: v1, .. }, E::Literal { value: v2, .. }) => v1 == v2,
        (
            E::BinaryOp {
                left: l1,
                op: o1,
                right: r1,
                ..
            },
            E::BinaryOp {
                left: l2,
                op: o2,
                right: r2,
                ..
            },
        ) => o1 == o2 && expr_struct_eq(l1, l2) && expr_struct_eq(r1, r2),
        (
            E::UnaryOp {
                op: o1, expr: e1, ..
            },
            E::UnaryOp {
                op: o2, expr: e2, ..
            },
        ) => o1 == o2 && expr_struct_eq(e1, e2),
        (E::IsNull { expr: e1, .. }, E::IsNull { expr: e2, .. }) => expr_struct_eq(e1, e2),
        (E::IsNotNull { expr: e1, .. }, E::IsNotNull { expr: e2, .. }) => expr_struct_eq(e1, e2),
        (
            E::Between {
                expr: e1,
                negated: n1,
                low: l1,
                high: h1,
                ..
            },
            E::Between {
                expr: e2,
                negated: n2,
                low: l2,
                high: h2,
                ..
            },
        ) => n1 == n2 && expr_struct_eq(e1, e2) && expr_struct_eq(l1, l2) && expr_struct_eq(h1, h2),
        (
            E::InList {
                expr: e1,
                list: l1,
                negated: n1,
                ..
            },
            E::InList {
                expr: e2,
                list: l2,
                negated: n2,
                ..
            },
        ) => {
            n1 == n2
                && expr_struct_eq(e1, e2)
                && l1.len() == l2.len()
                && l1.iter().zip(l2.iter()).all(|(x, y)| expr_struct_eq(x, y))
        }
        (
            E::Cast {
                expr: e1,
                data_type: dt1,
                ..
            },
            E::Cast {
                expr: e2,
                data_type: dt2,
                ..
            },
        ) => dt1 == dt2 && expr_struct_eq(e1, e2),
        (
            E::Function {
                name: n1,
                args: a1,
                distinct: d1,
                ..
            },
            E::Function {
                name: n2,
                args: a2,
                distinct: d2,
                ..
            },
        ) => {
            n1 == n2
                && d1 == d2
                && a1.len() == a2.len()
                && a1.iter().zip(a2.iter()).all(|(x, y)| expr_struct_eq(x, y))
        }
        (E::Wildcard, E::Wildcard) => true,
        (
            E::Parameter {
                index: i1,
                type_hint: t1,
                ..
            },
            E::Parameter {
                index: i2,
                type_hint: t2,
                ..
            },
        ) => i1 == i2 && t1 == t2,
        // Subqueries and CASE: conservative inequality — never factor.
        (E::ScalarSubquery { .. }, E::ScalarSubquery { .. }) => false,
        (E::CaseExpr { .. }, E::CaseExpr { .. }) => false,
        _ => false,
    }
}

/// Right-fold `conjuncts` into `c0 AND c1 AND … AND cN`. Panics if
/// `conjuncts` is empty (caller's invariant: only invoked when at
/// least one conjunct survived).
fn combine_and(conjuncts: Vec<PlanExpr>) -> PlanExpr {
    let mut iter = conjuncts.into_iter();
    let mut acc = iter.next().expect("combine_and: empty conjuncts");
    for c in iter {
        acc = PlanExpr::BinaryOp {
            left: Box::new(acc),
            op: BinaryOp::And,
            right: Box::new(c),
            span: None,
        };
    }
    acc
}

// ---------------------------------------------------------------------------
// Column-name helpers (mirror of `join_reorder.rs` helpers)
// ---------------------------------------------------------------------------

fn collect_column_names(expr: &PlanExpr) -> Vec<String> {
    let mut out = Vec::new();
    visit_column_names(expr, &mut |n| out.push(n.to_string()));
    out
}

/// Collect every column index referenced by `expr`. Used for
/// INDEX-based join routing in `try_push_into` (a column at index
/// `i` belongs to left when `i < left_schema.len()`).
fn collect_column_indices(expr: &PlanExpr) -> Vec<usize> {
    let mut out = Vec::new();
    visit_column_pairs(expr, &mut |_, idx| out.push(idx));
    out
}

fn visit_column_names<F: FnMut(&str)>(expr: &PlanExpr, callback: &mut F) {
    use PlanExpr as E;
    match expr {
        E::Column { name, .. } => callback(name),
        E::BinaryOp { left, right, .. } => {
            visit_column_names(left, callback);
            visit_column_names(right, callback);
        }
        E::UnaryOp { expr, .. }
        | E::IsNull { expr, .. }
        | E::IsNotNull { expr, .. }
        | E::Cast { expr, .. } => visit_column_names(expr, callback),
        E::Between {
            expr, low, high, ..
        } => {
            visit_column_names(expr, callback);
            visit_column_names(low, callback);
            visit_column_names(high, callback);
        }
        E::InList { expr, list, .. } => {
            visit_column_names(expr, callback);
            for item in list {
                visit_column_names(item, callback);
            }
        }
        E::Function { args, .. } => {
            for a in args {
                visit_column_names(a, callback);
            }
        }
        E::CaseExpr {
            operand,
            when_clauses,
            else_result,
            ..
        } => {
            if let Some(o) = operand {
                visit_column_names(o, callback);
            }
            for (cond, res) in when_clauses {
                visit_column_names(cond, callback);
                visit_column_names(res, callback);
            }
            if let Some(e) = else_result {
                visit_column_names(e, callback);
            }
        }
        E::ScalarSubquery { .. } | E::Literal { .. } | E::Wildcard | E::Parameter { .. } => {}
    }
}

/// Re-resolves `Column { name, index }` against `schema` by name.
/// Returns `None` if any name is missing or `schema` has duplicate
/// names (ambiguous lookup).
fn rewrite_against(expr: &PlanExpr, schema: &[ColumnInfo]) -> Option<PlanExpr> {
    let mut seen = HashSet::new();
    for c in schema {
        if !seen.insert(c.name.as_str()) {
            return None;
        }
    }
    rewrite_expr(expr, schema)
}

fn rewrite_expr(expr: &PlanExpr, schema: &[ColumnInfo]) -> Option<PlanExpr> {
    use PlanExpr as E;
    Some(match expr {
        E::Column { name, span, .. } => {
            let idx = schema.iter().position(|c| &c.name == name)?;
            E::Column {
                index: idx,
                name: name.clone(),
                span: *span,
            }
        }
        E::Literal { value, span } => E::Literal {
            value: value.clone(),
            span: *span,
        },
        E::BinaryOp {
            left,
            op,
            right,
            span,
        } => E::BinaryOp {
            left: Box::new(rewrite_expr(left, schema)?),
            op: *op,
            right: Box::new(rewrite_expr(right, schema)?),
            span: *span,
        },
        E::UnaryOp { op, expr, span } => E::UnaryOp {
            op: *op,
            expr: Box::new(rewrite_expr(expr, schema)?),
            span: *span,
        },
        E::IsNull { expr, span } => E::IsNull {
            expr: Box::new(rewrite_expr(expr, schema)?),
            span: *span,
        },
        E::IsNotNull { expr, span } => E::IsNotNull {
            expr: Box::new(rewrite_expr(expr, schema)?),
            span: *span,
        },
        E::Between {
            expr,
            negated,
            low,
            high,
            span,
        } => E::Between {
            expr: Box::new(rewrite_expr(expr, schema)?),
            negated: *negated,
            low: Box::new(rewrite_expr(low, schema)?),
            high: Box::new(rewrite_expr(high, schema)?),
            span: *span,
        },
        E::InList {
            expr,
            list,
            negated,
            span,
        } => E::InList {
            expr: Box::new(rewrite_expr(expr, schema)?),
            list: list
                .iter()
                .map(|e| rewrite_expr(e, schema))
                .collect::<Option<Vec<_>>>()?,
            negated: *negated,
            span: *span,
        },
        E::Cast {
            expr,
            data_type,
            span,
        } => E::Cast {
            expr: Box::new(rewrite_expr(expr, schema)?),
            data_type: data_type.clone(),
            span: *span,
        },
        E::Function {
            name,
            args,
            distinct,
            span,
        } => E::Function {
            name: name.clone(),
            args: args
                .iter()
                .map(|a| rewrite_expr(a, schema))
                .collect::<Option<Vec<_>>>()?,
            distinct: *distinct,
            span: *span,
        },
        E::CaseExpr {
            operand,
            when_clauses,
            else_result,
            span,
        } => E::CaseExpr {
            operand: match operand {
                Some(o) => Some(Box::new(rewrite_expr(o, schema)?)),
                None => None,
            },
            when_clauses: when_clauses
                .iter()
                .map(|(c, r)| Some((rewrite_expr(c, schema)?, rewrite_expr(r, schema)?)))
                .collect::<Option<Vec<_>>>()?,
            else_result: match else_result {
                Some(e) => Some(Box::new(rewrite_expr(e, schema)?)),
                None => None,
            },
            span: *span,
        },
        E::ScalarSubquery { subplan, span } => E::ScalarSubquery {
            subplan: subplan.clone(),
            span: *span,
        },
        E::Wildcard => E::Wildcard,
        E::Parameter {
            index,
            type_hint,
            span,
        } => E::Parameter {
            index: *index,
            type_hint: type_hint.clone(),
            span: *span,
        },
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arneb_common::types::{DataType, TableReference};

    fn col(name: &str, idx: usize) -> PlanExpr {
        PlanExpr::Column {
            index: idx,
            name: name.to_string(),
            span: None,
        }
    }

    fn lit_i32(v: i32) -> PlanExpr {
        PlanExpr::Literal {
            value: arneb_common::types::ScalarValue::Int32(v),
            span: None,
        }
    }

    fn eq(l: PlanExpr, r: PlanExpr) -> PlanExpr {
        PlanExpr::BinaryOp {
            left: Box::new(l),
            op: BinaryOp::Eq,
            right: Box::new(r),
            span: None,
        }
    }

    fn and(l: PlanExpr, r: PlanExpr) -> PlanExpr {
        PlanExpr::BinaryOp {
            left: Box::new(l),
            op: BinaryOp::And,
            right: Box::new(r),
            span: None,
        }
    }

    fn scan(table: &str, cols: &[&str]) -> LogicalPlan {
        LogicalPlan::TableScan {
            table: TableReference::table(table),
            schema: cols
                .iter()
                .map(|n| ColumnInfo {
                    name: n.to_string(),
                    data_type: DataType::Int32,
                    nullable: true,
                })
                .collect(),
            alias: None,
            properties: Default::default(),
            dynamic_filters_consumed: Vec::new(),
        }
    }

    #[test]
    fn split_and_flat() {
        let e = and(
            and(eq(col("a", 0), lit_i32(1)), eq(col("b", 1), lit_i32(2))),
            eq(col("c", 2), lit_i32(3)),
        );
        let conjuncts = split_and(e);
        assert_eq!(conjuncts.len(), 3);
    }

    #[test]
    fn pushdown_filter_above_inner_join_routes_to_left() {
        // SELECT * FROM left l JOIN right r ON l.k = r.k WHERE l.a = 1
        let left = scan("left", &["k", "a"]);
        let right = scan("right", &["k", "b"]);
        let join = LogicalPlan::Join {
            left: Box::new(left),
            right: Box::new(right),
            join_type: JoinType::Inner,
            condition: crate::plan::JoinCondition::On(eq(col("k", 0), col("k", 2))),
            dynamic_filter_ids: Vec::new(),
        };
        let filter = LogicalPlan::Filter {
            input: Box::new(join),
            predicate: eq(col("a", 1), lit_i32(1)),
        };
        let out = push_down(filter);
        // Outer Filter should be gone; the predicate now sits below the join on left.
        match &out {
            LogicalPlan::Join { left, .. } => match left.as_ref() {
                LogicalPlan::Filter { predicate, input } => {
                    // index re-resolved against left schema [k, a]: a -> 1
                    if let PlanExpr::BinaryOp { left, .. } = predicate {
                        if let PlanExpr::Column { index, name, .. } = left.as_ref() {
                            assert_eq!(name, "a");
                            assert_eq!(*index, 1);
                        } else {
                            panic!("expected Column");
                        }
                    } else {
                        panic!("expected BinaryOp");
                    }
                    assert!(matches!(input.as_ref(), LogicalPlan::TableScan { .. }));
                }
                _ => panic!("expected Filter under Join.left"),
            },
            _ => panic!("expected Join at top, got {:?}", out),
        }
    }

    #[test]
    fn pushdown_does_not_cross_left_join_to_right() {
        // SELECT * FROM left l LEFT JOIN right r ON l.k = r.k WHERE r.b = 1
        // The predicate references the inner (right) side; do NOT push.
        let left = scan("left", &["k", "a"]);
        let right = scan("right", &["k", "b"]);
        let join = LogicalPlan::Join {
            left: Box::new(left),
            right: Box::new(right),
            join_type: JoinType::Left,
            condition: crate::plan::JoinCondition::On(eq(col("k", 0), col("k", 2))),
            dynamic_filter_ids: Vec::new(),
        };
        let filter = LogicalPlan::Filter {
            input: Box::new(join),
            predicate: eq(col("b", 3), lit_i32(1)),
        };
        let out = push_down(filter);
        // Top should still be a Filter — we couldn't push past the LEFT join.
        assert!(matches!(out, LogicalPlan::Filter { .. }));
    }

    #[test]
    fn pushdown_splits_and_routes_each_conjunct() {
        // SELECT * FROM left l JOIN right r ON l.k = r.k
        //  WHERE l.a = 1 AND r.b = 2
        let left = scan("left", &["k", "a"]);
        let right = scan("right", &["k", "b"]);
        let join = LogicalPlan::Join {
            left: Box::new(left),
            right: Box::new(right),
            join_type: JoinType::Inner,
            condition: crate::plan::JoinCondition::On(eq(col("k", 0), col("k", 2))),
            dynamic_filter_ids: Vec::new(),
        };
        let filter = LogicalPlan::Filter {
            input: Box::new(join),
            predicate: and(eq(col("a", 1), lit_i32(1)), eq(col("b", 3), lit_i32(2))),
        };
        let out = push_down(filter);
        // Outer Filter gone, each side has its own Filter above the scan.
        match out {
            LogicalPlan::Join { left, right, .. } => {
                assert!(matches!(left.as_ref(), LogicalPlan::Filter { .. }));
                assert!(matches!(right.as_ref(), LogicalPlan::Filter { .. }));
            }
            _ => panic!("expected Join at top"),
        }
    }

    #[test]
    fn pushdown_leaves_unpushable_at_top() {
        // SELECT * FROM left l JOIN right r ON l.k = r.k
        //  WHERE l.a + r.b = 3   <- references both sides
        let left = scan("left", &["k", "a"]);
        let right = scan("right", &["k", "b"]);
        let join = LogicalPlan::Join {
            left: Box::new(left),
            right: Box::new(right),
            join_type: JoinType::Inner,
            condition: crate::plan::JoinCondition::On(eq(col("k", 0), col("k", 2))),
            dynamic_filter_ids: Vec::new(),
        };
        let cross = PlanExpr::BinaryOp {
            left: Box::new(PlanExpr::BinaryOp {
                left: Box::new(col("a", 1)),
                op: BinaryOp::Plus,
                right: Box::new(col("b", 3)),
                span: None,
            }),
            op: BinaryOp::Eq,
            right: Box::new(lit_i32(3)),
            span: None,
        };
        let filter = LogicalPlan::Filter {
            input: Box::new(join),
            predicate: cross,
        };
        let out = push_down(filter);
        assert!(matches!(out, LogicalPlan::Filter { .. }));
    }

    fn or_expr(l: PlanExpr, r: PlanExpr) -> PlanExpr {
        PlanExpr::BinaryOp {
            left: Box::new(l),
            op: BinaryOp::Or,
            right: Box::new(r),
            span: None,
        }
    }

    #[test]
    fn or_factor_extracts_common_conjunct_from_two_branches() {
        // (a=1 AND b=2) OR (a=1 AND b=3)  →  a=1 AND (b=2 OR b=3)
        let d1 = and(eq(col("a", 0), lit_i32(1)), eq(col("b", 1), lit_i32(2)));
        let d2 = and(eq(col("a", 0), lit_i32(1)), eq(col("b", 1), lit_i32(3)));
        let conjuncts = split_and(or_expr(d1, d2));
        assert_eq!(conjuncts.len(), 2);
        // First should be `a=1`, second the simplified OR.
        let has_a_eq_1 = conjuncts
            .iter()
            .any(|c| expr_struct_eq(c, &eq(col("a", 0), lit_i32(1))));
        assert!(has_a_eq_1, "expected a=1 to be factored out");
    }

    // The earlier self-join structural-eq guard test is superseded by
    // `or_factor_derives_per_column_weakenings_for_self_join` below —
    // structural-eq stays index-aware (commons=∅ for that shape), and
    // per-column weakening now adds two safe weakening predicates.

    #[test]
    fn or_factor_handles_three_branches_with_common_in_each() {
        // (a=1 AND c=9) OR (b=2 AND c=9) OR (a=1 AND b=2 AND c=9)
        // Common: c=9. Residual: (a=1) OR (b=2) OR (a=1 AND b=2)
        let d1 = and(eq(col("a", 0), lit_i32(1)), eq(col("c", 2), lit_i32(9)));
        let d2 = and(eq(col("b", 1), lit_i32(2)), eq(col("c", 2), lit_i32(9)));
        let d3 = and(
            and(eq(col("a", 0), lit_i32(1)), eq(col("b", 1), lit_i32(2))),
            eq(col("c", 2), lit_i32(9)),
        );
        let or3 = or_expr(or_expr(d1, d2), d3);
        let conjuncts = split_and(or3);
        // Expect: c=9 factored, plus a residual OR.
        assert!(conjuncts
            .iter()
            .any(|c| expr_struct_eq(c, &eq(col("c", 2), lit_i32(9)))));
        assert!(conjuncts.iter().any(|c| matches!(
            c,
            PlanExpr::BinaryOp {
                op: BinaryOp::Or,
                ..
            }
        )));
    }

    #[test]
    fn or_factor_derives_per_column_weakenings_for_self_join() {
        // Q07 shape: (n_name[0]='F' AND n_name[1]='G') OR (n_name[0]='G' AND n_name[1]='F')
        // Structural eq finds NO common (indices differ).
        // Per-column derivation should produce TWO weakenings:
        //   (n_name[0]='F') OR (n_name[0]='G')
        //   (n_name[1]='G') OR (n_name[1]='F')
        // Plus the original OR is preserved.
        let f = PlanExpr::Literal {
            value: arneb_common::types::ScalarValue::Utf8("FRANCE".into()),
            span: None,
        };
        let g = PlanExpr::Literal {
            value: arneb_common::types::ScalarValue::Utf8("GERMANY".into()),
            span: None,
        };
        let d1 = and(
            eq(col("n_name", 0), f.clone()),
            eq(col("n_name", 1), g.clone()),
        );
        let d2 = and(
            eq(col("n_name", 0), g.clone()),
            eq(col("n_name", 1), f.clone()),
        );
        let conjuncts = split_and(or_expr(d1, d2));
        // Expect: 1 residual OR + 2 derived weakenings = 3 conjuncts.
        assert_eq!(conjuncts.len(), 3, "got {:?}", conjuncts);
        // One should be `(col[0]='F') OR (col[0]='G')` — i.e. only references col 0.
        let has_col0_only = conjuncts.iter().any(|c| {
            let cols = collect_column_index_set(c);
            cols.len() == 1 && cols.iter().any(|(n, i)| n == "n_name" && *i == 0)
        });
        let has_col1_only = conjuncts.iter().any(|c| {
            let cols = collect_column_index_set(c);
            cols.len() == 1 && cols.iter().any(|(n, i)| n == "n_name" && *i == 1)
        });
        assert!(has_col0_only, "missing col-0 weakening");
        assert!(has_col1_only, "missing col-1 weakening");
    }

    #[test]
    fn or_factor_collapses_or_when_branches_become_empty() {
        // (a=1) OR (a=1) — single shared conjunct, branches all become empty.
        // Result: just [a=1].
        let d1 = eq(col("a", 0), lit_i32(1));
        let d2 = eq(col("a", 0), lit_i32(1));
        let conjuncts = split_and(or_expr(d1, d2));
        assert_eq!(conjuncts.len(), 1);
        assert!(expr_struct_eq(&conjuncts[0], &eq(col("a", 0), lit_i32(1))));
    }

    // -----------------------------------------------------------------------

    #[test]
    fn pushdown_through_projection_not_supported() {
        // Conservative: don't push through Projection.
        let scan_plan = scan("t", &["a", "b"]);
        let projected = LogicalPlan::Projection {
            input: Box::new(scan_plan),
            exprs: vec![col("a", 0), col("b", 1)],
            schema: vec![
                ColumnInfo {
                    name: "a".to_string(),
                    data_type: DataType::Int32,
                    nullable: true,
                },
                ColumnInfo {
                    name: "b".to_string(),
                    data_type: DataType::Int32,
                    nullable: true,
                },
            ],
        };
        let filter = LogicalPlan::Filter {
            input: Box::new(projected),
            predicate: eq(col("a", 0), lit_i32(1)),
        };
        let out = push_down(filter);
        assert!(matches!(out, LogicalPlan::Filter { .. }));
    }
}
