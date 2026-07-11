//! Cross-fragment column pruning (2026-05-26).
//!
//! Each `PlanFragment` is planned independently by `convert_with_pruning`,
//! which prunes columns within the fragment but treats `ExchangeNode`
//! leaves as opaque (the producer always ships its full schema). For
//! Q09 this means a worker fragment with a 49-column output sends all
//! 49 columns to coord even when only 5 are referenced — the wasted 44
//! columns dominate worker peak RSS and inter-stage network bytes.
//!
//! `prune_fragment_tree` runs as a coordinator-side post-pass over the
//! `PlanFragment` tree. For each fragment, it uses `prune_for_columns`
//! (re-exported from `arneb_execution`) to compute which columns of each
//! child `ExchangeNode` are actually referenced in the parent's plan,
//! then PUSHES that projection into the matching child fragment by
//! wrapping `child.root` with a `Projection`. The parent's wrapping
//! `Projection` becomes redundant (its input already arrives pruned)
//! so we replace `Projection > ExchangeNode(full)` with
//! `ExchangeNode(pruned)` directly. Hash-partition columns on the
//! child's `output_partitioning` are always included in the projection
//! and re-indexed to the new positions.
//!
//! Algorithm:
//!
//! 1. For the root fragment, the needed-at-root set is identity.
//! 2. For the current fragment:
//!    1. Run `prune_for_columns(frag.root, needed)`.
//!    2. Walk the pruned plan to find `Projection > ExchangeNode` pairs.
//!       Each pair tells us which `child_id` needs which indices.
//!    3. For each source-fragment child: build its needed set from
//!       (a) the pushed projection's indices and (b) its own
//!       output_partitioning hash columns. Wrap `child.root` with a
//!       `Projection`, remap output partition hash columns.
//!    4. Strip the now-redundant `Projection > ExchangeNode` wrapper
//!       from this fragment's plan (replace with `ExchangeNode(pruned)`).
//!    5. Recurse into each child with its new needed set.

use std::collections::{BTreeSet, HashMap};

use arneb_common::identifiers::StageId;
use arneb_common::types::ColumnInfo;
use arneb_execution::prune_for_columns;
use arneb_planner::{LogicalPlan, PartitioningScheme, PlanExpr, PlanFragment};

/// Entry point: prune the entire `PlanFragment` tree from the root down.
/// Mutates each fragment's `root` and `output_partitioning` in place.
pub fn prune_fragment_tree(root: &mut PlanFragment) {
    let initial_needed: BTreeSet<usize> = (0..root.root.schema().len()).collect();
    prune_recursive(root, &initial_needed);
}

fn prune_recursive(frag: &mut PlanFragment, needed_at_root: &BTreeSet<usize>) {
    // Ensure output_partitioning hash columns are kept (downstream
    // shuffling needs them as data columns, not just metadata).
    let mut needed = needed_at_root.clone();
    if let PartitioningScheme::Hash { columns, .. } = &frag.output_partitioning {
        for &c in columns {
            needed.insert(c);
        }
    }

    // Prune this fragment's logical plan. The pruning algorithm inserts
    // `Projection > ExchangeNode(full)` wrappers — those tell us which
    // columns each child fragment needs to send.
    let (pruned, output_mapping) = prune_for_columns(&frag.root, &needed);

    // Re-index output_partitioning hash columns through the mapping
    // (their positions shifted because the root's schema shrank).
    if let PartitioningScheme::Hash { columns, .. } = &mut frag.output_partitioning {
        for c in columns.iter_mut() {
            *c = *output_mapping
                .get(c)
                .expect("hash partition column must be in needed set");
        }
    }

    // Collect the pushed projections by stage_id so we know what to
    // wrap each child with.
    let mut pushed: HashMap<StageId, Vec<usize>> = HashMap::new();
    collect_pushed_projections(&pruned, &mut pushed);

    // Now rewrite the parent's plan: strip `Projection > ExchangeNode`
    // wrappers and replace with `ExchangeNode(pruned_schema)` directly.
    // The parent's downstream operators already reference the projected
    // (smaller) schema positions because `prune_for_columns` did the
    // remap during step (a).
    let collapsed = collapse_exchange_projections(pruned, &pushed);
    frag.root = collapsed;

    // For each source-fragment child, build its needed set + wrap +
    // recurse.
    for child in &mut frag.source_fragments {
        // Default needed: child's output_partitioning hash cols only.
        // If the parent referenced this child, take its pushed indices.
        let child_schema_len = child.root.schema().len();
        let mut child_needed: BTreeSet<usize> = match pushed.get(&child.id) {
            Some(indices) => indices.iter().copied().collect(),
            // If parent doesn't reference this exchange at all (rare —
            // dead exchange — keep all to avoid building an empty-schema
            // batch which Arrow rejects).
            None => (0..child_schema_len).collect(),
        };
        if let PartitioningScheme::Hash { columns, .. } = &child.output_partitioning {
            for &c in columns {
                child_needed.insert(c);
            }
        }

        // Wrap child.root with Projection that selects only the cols
        // in child_needed. This means the WORKER produces only those
        // cols, saving worker memory + network bytes.
        wrap_child_with_projection(child, &child_needed);

        // Recurse: the child's own children might be prunable too.
        prune_recursive(child, &child_needed);
    }
}

/// Walk `plan` looking for the pattern `Projection { input: ExchangeNode { stage_id, .. }, exprs }`.
/// For each, record the column indices the exprs reference into
/// `out[stage_id]`. The exprs are guaranteed to be `Column { index }`
/// by construction (`prune_for_columns` only emits column refs in
/// these wrapping Projections).
fn collect_pushed_projections(plan: &LogicalPlan, out: &mut HashMap<StageId, Vec<usize>>) {
    use LogicalPlan as L;
    match plan {
        L::Projection { input, exprs, .. } => {
            if let L::ExchangeNode { stage_id, .. } = input.as_ref() {
                let mut indices = Vec::with_capacity(exprs.len());
                for e in exprs {
                    if let PlanExpr::Column { index, .. } = e {
                        indices.push(*index);
                    } else {
                        // Mixed expr — fall back to "all cols" by
                        // recording None. We don't have a structural
                        // representation of "all"; the lookup will
                        // miss this stage_id and the child will keep
                        // its full output. Conservatively safe.
                        return;
                    }
                }
                out.insert(*stage_id, indices);
                return;
            }
            collect_pushed_projections(input, out);
        }
        L::Filter { input, .. }
        | L::Sort { input, .. }
        | L::Limit { input, .. }
        | L::Distinct { input }
        | L::Explain { input, .. }
        | L::Aggregate { input, .. }
        | L::PartialAggregate { input, .. }
        | L::FinalAggregate { input, .. }
        | L::Window { input, .. }
        | L::AssignUniqueId { input, .. } => collect_pushed_projections(input, out),
        L::Join { left, right, .. } => {
            collect_pushed_projections(left, out);
            collect_pushed_projections(right, out);
        }
        L::SemiJoin { left, right, .. } | L::AntiJoin { left, right, .. } => {
            collect_pushed_projections(left, out);
            collect_pushed_projections(right, out);
        }
        L::UnionAll { inputs } => {
            for i in inputs {
                collect_pushed_projections(i, out);
            }
        }
        L::Intersect { left, right } | L::Except { left, right } => {
            collect_pushed_projections(left, out);
            collect_pushed_projections(right, out);
        }
        L::ScalarSubquery { subplan } => collect_pushed_projections(subplan, out),
        _ => {}
    }
}

/// Replace `Projection { input: ExchangeNode { stage_id, schema }, exprs }` with
/// `ExchangeNode { stage_id, schema: pruned }` directly — the child fragment now
/// produces the pruned schema so the projection is redundant.
fn collapse_exchange_projections(
    plan: LogicalPlan,
    pushed: &HashMap<StageId, Vec<usize>>,
) -> LogicalPlan {
    use LogicalPlan as L;
    match plan {
        L::Projection {
            input,
            exprs,
            schema,
        } => {
            if let L::ExchangeNode {
                stage_id,
                schema: full_schema,
            } = input.as_ref()
            {
                if let Some(indices) = pushed.get(stage_id) {
                    let pruned_schema: Vec<ColumnInfo> =
                        indices.iter().map(|&i| full_schema[i].clone()).collect();
                    return L::ExchangeNode {
                        stage_id: *stage_id,
                        schema: pruned_schema,
                    };
                }
            }
            L::Projection {
                input: Box::new(collapse_exchange_projections(*input, pushed)),
                exprs,
                schema,
            }
        }
        L::Filter { input, predicate } => L::Filter {
            input: Box::new(collapse_exchange_projections(*input, pushed)),
            predicate,
        },
        L::Sort { input, order_by } => L::Sort {
            input: Box::new(collapse_exchange_projections(*input, pushed)),
            order_by,
        },
        L::Limit {
            input,
            limit,
            offset,
        } => L::Limit {
            input: Box::new(collapse_exchange_projections(*input, pushed)),
            limit,
            offset,
        },
        L::Distinct { input } => L::Distinct {
            input: Box::new(collapse_exchange_projections(*input, pushed)),
        },
        L::Explain { input, analyze } => L::Explain {
            input: Box::new(collapse_exchange_projections(*input, pushed)),
            analyze,
        },
        L::Aggregate {
            input,
            group_by,
            aggr_exprs,
            schema,
        } => L::Aggregate {
            input: Box::new(collapse_exchange_projections(*input, pushed)),
            group_by,
            aggr_exprs,
            schema,
        },
        L::PartialAggregate {
            input,
            group_by,
            aggr_exprs,
            schema,
        } => L::PartialAggregate {
            input: Box::new(collapse_exchange_projections(*input, pushed)),
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
            input: Box::new(collapse_exchange_projections(*input, pushed)),
            group_by,
            aggr_exprs,
            schema,
        },
        L::Window { input, functions } => L::Window {
            input: Box::new(collapse_exchange_projections(*input, pushed)),
            functions,
        },
        L::AssignUniqueId { input, id_column } => L::AssignUniqueId {
            input: Box::new(collapse_exchange_projections(*input, pushed)),
            id_column,
        },
        L::Join {
            left,
            right,
            join_type,
            condition,
            dynamic_filter_ids,
        } => L::Join {
            left: Box::new(collapse_exchange_projections(*left, pushed)),
            right: Box::new(collapse_exchange_projections(*right, pushed)),
            join_type,
            condition,
            dynamic_filter_ids,
        },
        L::SemiJoin {
            left,
            right,
            left_key,
            right_key,
            residual,
            dynamic_filter_ids,
        } => L::SemiJoin {
            left: Box::new(collapse_exchange_projections(*left, pushed)),
            right: Box::new(collapse_exchange_projections(*right, pushed)),
            left_key,
            right_key,
            residual,
            dynamic_filter_ids,
        },
        L::AntiJoin {
            left,
            right,
            left_key,
            right_key,
            residual,
        } => L::AntiJoin {
            left: Box::new(collapse_exchange_projections(*left, pushed)),
            right: Box::new(collapse_exchange_projections(*right, pushed)),
            left_key,
            right_key,
            residual,
        },
        L::UnionAll { inputs } => L::UnionAll {
            inputs: inputs
                .into_iter()
                .map(|i| collapse_exchange_projections(i, pushed))
                .collect(),
        },
        L::Intersect { left, right } => L::Intersect {
            left: Box::new(collapse_exchange_projections(*left, pushed)),
            right: Box::new(collapse_exchange_projections(*right, pushed)),
        },
        L::Except { left, right } => L::Except {
            left: Box::new(collapse_exchange_projections(*left, pushed)),
            right: Box::new(collapse_exchange_projections(*right, pushed)),
        },
        L::ScalarSubquery { subplan } => L::ScalarSubquery {
            subplan: Box::new(collapse_exchange_projections(*subplan, pushed)),
        },
        // Leaf nodes — no recursion needed.
        other => other,
    }
}

/// Wrap `child.root` with a `Projection` that selects only the columns
/// in `needed`. Updates `child.output_partitioning` hash columns to
/// reference new positions.
fn wrap_child_with_projection(child: &mut PlanFragment, needed: &BTreeSet<usize>) {
    let n_out = child.root.schema().len();
    if needed.len() >= n_out {
        return;
    }
    let indices: Vec<usize> = needed.iter().copied().collect();
    let schema = child.root.schema();
    let proj_exprs: Vec<PlanExpr> = indices
        .iter()
        .map(|&i| PlanExpr::Column {
            index: i,
            name: schema[i].name.clone(),
            span: None,
        })
        .collect();
    let projected_schema: Vec<ColumnInfo> = indices.iter().map(|&i| schema[i].clone()).collect();

    let mapping: HashMap<usize, usize> = indices
        .iter()
        .enumerate()
        .map(|(new, &old)| (old, new))
        .collect();

    // Move child.root into the projection's input.
    let original = std::mem::replace(&mut child.root, LogicalPlan::OneRow);
    child.root = LogicalPlan::Projection {
        input: Box::new(original),
        exprs: proj_exprs,
        schema: projected_schema,
    };

    // Re-index output_partitioning hash columns.
    if let PartitioningScheme::Hash { columns, .. } = &mut child.output_partitioning {
        for c in columns.iter_mut() {
            *c = *mapping
                .get(c)
                .expect("hash column must be retained in projection");
        }
    }
}
