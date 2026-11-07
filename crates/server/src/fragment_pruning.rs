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

    // Collect the columns the parent reads from each child, by stage_id.
    let mut pushed: HashMap<StageId, BTreeSet<usize>> = HashMap::new();
    collect_pushed_projections(&pruned, &mut pushed);

    // What each child will produce, in order: the columns the parent
    // reads plus its hash-partition columns, ascending (as
    // `wrap_child_with_projection` emits them), or its full schema when
    // that is all of them or the parent doesn't reference the exchange
    // (rare — a dead exchange — kept whole so no batch has an empty
    // schema, which Arrow rejects).
    let child_outputs: HashMap<StageId, Vec<usize>> = frag
        .source_fragments
        .iter()
        .map(|child| {
            let n_out = child.root.schema().len();
            let mut cols = pushed
                .get(&child.id)
                .cloned()
                .unwrap_or_else(|| (0..n_out).collect());
            if let PartitioningScheme::Hash { columns, .. } = &child.output_partitioning {
                cols.extend(columns.iter().copied());
            }
            let cols = if cols.len() >= n_out {
                (0..n_out).collect()
            } else {
                cols.into_iter().collect()
            };
            (child.id, cols)
        })
        .collect();

    // Now rewrite the parent's plan: each `Projection > ExchangeNode`
    // reads the child's pruned output, and is dropped when it would only
    // forward that output as is. The parent's downstream operators
    // already reference the projected (smaller) schema positions because
    // `prune_for_columns` did the remap during step (a).
    let collapsed = collapse_exchange_projections(pruned, &child_outputs);
    frag.root = collapsed;

    // For each source-fragment child: wrap + recurse.
    for child in &mut frag.source_fragments {
        let child_needed: BTreeSet<usize> = child_outputs[&child.id].iter().copied().collect();

        // Wrap child.root with Projection that selects only the cols
        // in child_needed. This means the WORKER produces only those
        // cols, saving worker memory + network bytes.
        wrap_child_with_projection(child, &child_needed);

        // Recurse: the child's own children might be prunable too.
        prune_recursive(child, &child_needed);
    }
}

/// Walk `plan` looking for the pattern `Projection { input: ExchangeNode { stage_id, .. }, exprs }`.
/// For each, add the column indices the exprs reference to
/// `out[stage_id]`. A projection that is not only column refs (one the
/// query planned, not `prune_for_columns`) asks for every column.
fn collect_pushed_projections(plan: &LogicalPlan, out: &mut HashMap<StageId, BTreeSet<usize>>) {
    use LogicalPlan as L;
    match plan {
        L::Projection { input, exprs, .. } => {
            if let L::ExchangeNode { stage_id, schema } = input.as_ref() {
                let indices = out.entry(*stage_id).or_default();
                for e in exprs {
                    if let PlanExpr::Column { index, .. } = e {
                        indices.insert(*index);
                    } else {
                        indices.extend(0..schema.len());
                        return;
                    }
                }
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

/// Point each `Projection { input: ExchangeNode { stage_id, schema }, exprs }`
/// at the columns the child fragment now produces (`pushed[stage_id]`, in
/// order): the exchange gets the pruned schema and the exprs index into
/// it. The projection is dropped when it forwards that output exactly; it
/// stays when it reorders or repeats columns, which the child's ascending
/// output does not.
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
                let remapped: Option<Vec<PlanExpr>> = pushed.get(stage_id).and_then(|indices| {
                    exprs
                        .iter()
                        .map(|e| match e {
                            PlanExpr::Column { index, name, span } => {
                                let index = indices.iter().position(|i| i == index)?;
                                Some(PlanExpr::Column {
                                    index,
                                    name: name.clone(),
                                    span: *span,
                                })
                            }
                            _ => None,
                        })
                        .collect()
                });
                if let Some(remapped) = remapped {
                    let indices = &pushed[stage_id];
                    let exchange = L::ExchangeNode {
                        stage_id: *stage_id,
                        schema: indices.iter().map(|&i| full_schema[i].clone()).collect(),
                    };
                    let forwards_as_is = remapped.len() == indices.len()
                        && remapped.iter().enumerate().all(
                            |(pos, e)| matches!(e, PlanExpr::Column { index, .. } if *index == pos),
                        );
                    if forwards_as_is {
                        return exchange;
                    }
                    return L::Projection {
                        input: Box::new(exchange),
                        exprs: remapped,
                        schema,
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
            probe_side_filter_ids,
        } => L::SemiJoin {
            left: Box::new(collapse_exchange_projections(*left, pushed)),
            right: Box::new(collapse_exchange_projections(*right, pushed)),
            left_key,
            right_key,
            residual,
            dynamic_filter_ids,
            probe_side_filter_ids,
        },
        L::AntiJoin {
            left,
            right,
            left_key,
            right_key,
            residual,
            probe_side_filter_ids,
        } => L::AntiJoin {
            left: Box::new(collapse_exchange_projections(*left, pushed)),
            right: Box::new(collapse_exchange_projections(*right, pushed)),
            left_key,
            right_key,
            residual,
            probe_side_filter_ids,
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

#[cfg(test)]
mod tests {
    use super::*;
    use arneb_common::types::DataType;
    use arneb_planner::FragmentType;

    fn cols(names: &[&str]) -> Vec<ColumnInfo> {
        names
            .iter()
            .map(|n| ColumnInfo {
                name: (*n).to_string(),
                data_type: DataType::Int64,
                nullable: true,
            })
            .collect()
    }

    fn column(index: usize, name: &str) -> PlanExpr {
        PlanExpr::Column {
            index,
            name: name.to_string(),
            span: None,
        }
    }

    /// A child producing `c0..c3`, read by the parent through `exprs`.
    fn tree(exprs: &[usize], child_partitioning: PartitioningScheme) -> PlanFragment {
        let full = cols(&["c0", "c1", "c2", "c3"]);
        let child = PlanFragment {
            id: StageId(1),
            fragment_type: FragmentType::Source,
            root: LogicalPlan::ExchangeNode {
                stage_id: StageId(9),
                schema: full.clone(),
            },
            output_partitioning: child_partitioning,
            source_fragments: vec![],
        };
        let exprs: Vec<PlanExpr> = exprs.iter().map(|&i| column(i, &full[i].name)).collect();
        let schema = exprs
            .iter()
            .map(|e| match e {
                PlanExpr::Column { index, .. } => full[*index].clone(),
                _ => unreachable!(),
            })
            .collect();
        PlanFragment {
            id: StageId(2),
            fragment_type: FragmentType::Fixed,
            root: LogicalPlan::Projection {
                input: Box::new(LogicalPlan::ExchangeNode {
                    stage_id: StageId(1),
                    schema: full,
                }),
                exprs,
                schema,
            },
            output_partitioning: PartitioningScheme::Single,
            source_fragments: vec![child],
        }
    }

    /// The column names the parent's root returns, read through the
    /// child's actual output (not the schema the parent's exchange claims).
    fn root_output(root: &PlanFragment) -> Vec<String> {
        fn names_of(plan: &LogicalPlan, child: &[ColumnInfo]) -> Vec<String> {
            let names = |s: &[ColumnInfo]| s.iter().map(|c| c.name.clone()).collect::<Vec<_>>();
            match plan {
                LogicalPlan::ExchangeNode { schema, .. } => {
                    assert_eq!(names(schema), names(child), "exchange schema");
                    names(child)
                }
                LogicalPlan::Projection { input, exprs, .. } => {
                    let input = names_of(input, child);
                    exprs
                        .iter()
                        .map(|e| match e {
                            PlanExpr::Column { index, .. } => input[*index].clone(),
                            other => panic!("unexpected expr: {other:?}"),
                        })
                        .collect()
                }
                other => panic!("unexpected plan: {other:?}"),
            }
        }
        names_of(&root.root, &root.source_fragments[0].root.schema())
    }

    fn pruned(exprs: &[usize], child_partitioning: PartitioningScheme) -> PlanFragment {
        let mut root = tree(exprs, child_partitioning);
        prune_fragment_tree(&mut root);
        root
    }

    #[test]
    fn a_reordering_projection_over_an_exchange_keeps_its_order() {
        // #225: every column, reordered (the two sides of a self-join).
        let root = pruned(&[2, 3, 0, 1], PartitioningScheme::Single);
        assert_eq!(root_output(&root), ["c2", "c3", "c0", "c1"]);
        // Some columns, reordered.
        let root = pruned(&[3, 1], PartitioningScheme::Single);
        assert_eq!(root.source_fragments[0].root.schema(), cols(&["c1", "c3"]));
        assert_eq!(root_output(&root), ["c3", "c1"]);
    }

    #[test]
    fn a_repeated_column_over_an_exchange_is_kept() {
        let root = pruned(&[2, 2], PartitioningScheme::Single);
        assert_eq!(root_output(&root), ["c2", "c2"]);
    }

    #[test]
    fn a_projection_forwarding_the_childs_output_is_dropped() {
        let root = pruned(&[0, 1, 2, 3], PartitioningScheme::Single);
        assert!(matches!(root.root, LogicalPlan::ExchangeNode { .. }));
        assert_eq!(root_output(&root), ["c0", "c1", "c2", "c3"]);
        let root = pruned(&[0, 2], PartitioningScheme::Single);
        assert_eq!(root.source_fragments[0].root.schema(), cols(&["c0", "c2"]));
        assert_eq!(root_output(&root), ["c0", "c2"]);
    }

    #[test]
    fn a_hash_column_the_parent_does_not_read_stays_out_of_its_output() {
        let hash = PartitioningScheme::Hash {
            columns: vec![3],
            partition_count: 2,
        };
        let root = pruned(&[0], hash);
        assert_eq!(root_output(&root), ["c0"]);
        assert!(matches!(
            root.source_fragments[0].output_partitioning,
            PartitioningScheme::Hash { ref columns, .. } if columns == &[1]
        ));
    }
}
