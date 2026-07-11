//! Plan property derivation for partition propagation.
//!
//! Vendor-inspired by DataFusion's `EquivalenceProperties` (Apache-2.0).
//! Source:
//!   <https://github.com/apache/datafusion/blob/main/datafusion/physical-expr/src/equivalence/properties/mod.rs>
//!
//! Stripped to the minimum arneb needs to unblock composed-join partition
//! propagation (Q05/Q09 in the TPC-H suite): a `PartitioningScheme` plus
//! equivalence classes of column indices. Ordering-equivalence, constants,
//! and constraints are intentionally omitted; they belong to later phases.

use std::collections::HashMap;

use arneb_common::identifiers::StageId;

use crate::fragment::{
    extract_partitioning_equi_keys, Distribution, PartitioningScheme, PlanFragment,
};
use crate::plan::{LogicalPlan, PlanExpr};

/// Properties that a plan node's output stream is known to satisfy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActualProperties {
    /// How the data is partitioned across worker tasks.
    pub partitioning: PartitioningScheme,
    /// Sets of column indices guaranteed to hold the same value within
    /// each row. Populated by equi-joins (left.k ⇔ right.k after the join)
    /// and by projections that alias columns. Each class is sorted and
    /// deduplicated; classes are pairwise disjoint.
    equivalence_classes: Vec<Vec<usize>>,
}

impl ActualProperties {
    /// New properties with no equivalence relations.
    pub fn new(partitioning: PartitioningScheme) -> Self {
        Self {
            partitioning,
            equivalence_classes: Vec::new(),
        }
    }

    /// View of the equivalence classes (test/debug).
    pub fn equivalence_classes(&self) -> &[Vec<usize>] {
        &self.equivalence_classes
    }

    /// Merge `a` and `b` into the same equivalence class (union-find).
    /// Idempotent: calling twice with the same pair is a no-op.
    pub fn with_equivalence(mut self, a: usize, b: usize) -> Self {
        let mut class_a = None;
        let mut class_b = None;
        for (i, cls) in self.equivalence_classes.iter().enumerate() {
            if cls.contains(&a) {
                class_a = Some(i);
            }
            if cls.contains(&b) {
                class_b = Some(i);
            }
        }
        match (class_a, class_b) {
            (Some(i), Some(j)) if i == j => {
                // already in same class
            }
            (Some(i), Some(j)) => {
                // merge the higher-indexed class into the lower-indexed one
                let (keep, drop) = if i < j { (i, j) } else { (j, i) };
                let to_merge = self.equivalence_classes.remove(drop);
                let surviving = &mut self.equivalence_classes[keep];
                surviving.extend(to_merge);
                surviving.sort_unstable();
                surviving.dedup();
            }
            (Some(i), None) => {
                let cls = &mut self.equivalence_classes[i];
                cls.push(b);
                cls.sort_unstable();
                cls.dedup();
            }
            (None, Some(j)) => {
                let cls = &mut self.equivalence_classes[j];
                cls.push(a);
                cls.sort_unstable();
                cls.dedup();
            }
            (None, None) => {
                let mut cls = vec![a, b];
                cls.sort_unstable();
                cls.dedup();
                self.equivalence_classes.push(cls);
            }
        }
        self
    }

    /// Returns the set of column indices equivalent to `col`, including
    /// `col` itself. If `col` has no recorded equivalences, returns `[col]`.
    pub fn equivalent_columns(&self, col: usize) -> Vec<usize> {
        for cls in &self.equivalence_classes {
            if cls.contains(&col) {
                return cls.clone();
            }
        }
        vec![col]
    }

    /// Returns true if this property set satisfies the required
    /// distribution. Equivalence-aware: `Hash{[a]}` with `a ≡ b` satisfies
    /// `HashPartitioned([b])`, eliding an unnecessary `RepartitionExec` in
    /// composed joins like Q05/Q09.
    pub fn satisfy(&self, required: &Distribution) -> bool {
        match required {
            Distribution::Unspecified | Distribution::SinglePartition => {
                self.partitioning.satisfy(required)
            }
            Distribution::HashPartitioned(required_cols) => {
                if self.partitioning.satisfy(required) {
                    return true;
                }
                let part_cols: Vec<usize> = match &self.partitioning {
                    PartitioningScheme::Hash { columns, .. } => columns.clone(),
                    _ => return false,
                };
                // Expand `required` with each column's equivalence class,
                // then check `part_cols ⊆ expanded_required`. Equivalent
                // to mapping each part column through its equivalence
                // class and asking if any image lies in `required`.
                let mut expanded: Vec<usize> = required_cols.clone();
                for c in required_cols {
                    for eq in self.equivalent_columns(*c) {
                        if !expanded.contains(&eq) {
                            expanded.push(eq);
                        }
                    }
                }
                part_cols.iter().all(|c| expanded.contains(c))
            }
        }
    }
}

// ===========================================================================
// Property derivation
// ===========================================================================
//
// Walks a `PlanFragment` and computes its [`ActualProperties`]. The
// partitioning comes from `fragment.output_partitioning` (set by the
// fragmenter); the equivalence classes are derived by visiting the
// fragment's `root` logical plan and recursing through `ExchangeNode`s
// into the corresponding `source_fragments`.
//
// This is the arneb analogue of Trino's `PropertyDerivations` visitor
// (Apache-2.0). Unlike Trino's, it returns nothing for plan-node shapes
// where partitioning derivation would require physical-operator knowledge
// (Aggregate, set ops) — arneb tracks partitioning at the fragment level
// instead, so the visitor only contributes equivalence classes.

/// Compute [`ActualProperties`] for a fragment: combine its declared
/// output partitioning with column-equivalence classes derived from its
/// plan tree (recursing through `ExchangeNode`s into source fragments).
pub fn derive_properties(fragment: &PlanFragment) -> ActualProperties {
    let source_props: HashMap<StageId, ActualProperties> = fragment
        .source_fragments
        .iter()
        .map(|sf| (sf.id, derive_properties(sf)))
        .collect();

    let mut pairs = Vec::new();
    derive_equivalences(&fragment.root, &source_props, &mut pairs);

    let mut props = ActualProperties::new(fragment.output_partitioning.clone());
    for (a, b) in pairs {
        props = props.with_equivalence(a, b);
    }
    props
}

/// Column indices in `plan`'s output schema that are join-equal to `col`
/// (its equivalence class), including `col` itself. Same-fragment only:
/// an `ExchangeNode` in the subtree truncates the class (conservative — the
/// result is always a SUBSET of the true equality set, never a superset, so
/// callers that prune by it stay correct).
///
/// Used by the dynamic-filter injection to find the exact probe-side columns
/// a build-key filter may be pushed onto: the equivalence class includes a
/// transitively-equal cross-table column (the q18 sibling) but EXCLUDES a
/// merely same-named column from an unrelated table (the q08 self-join twin),
/// which name-based routing cannot distinguish.
pub fn equivalent_output_columns(plan: &LogicalPlan, col: usize) -> Vec<usize> {
    let source_props: HashMap<StageId, ActualProperties> = HashMap::new();
    let mut pairs = Vec::new();
    derive_equivalences(plan, &source_props, &mut pairs);
    let mut props = ActualProperties::new(PartitioningScheme::Single);
    for (a, b) in pairs {
        props = props.with_equivalence(a, b);
    }
    props.equivalent_columns(col)
}

/// Append equivalence pairs `(a, b)` (column indices into `plan`'s output
/// schema) discovered by walking `plan`. `source_props` maps each
/// `ExchangeNode`'s upstream stage to its already-derived properties.
fn derive_equivalences(
    plan: &LogicalPlan,
    source_props: &HashMap<StageId, ActualProperties>,
    out: &mut Vec<(usize, usize)>,
) {
    match plan {
        LogicalPlan::TableScan { .. } => {}

        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Distinct { input, .. } => {
            derive_equivalences(input, source_props, out);
        }

        LogicalPlan::Projection { input, exprs, .. } => {
            let mut child = Vec::new();
            derive_equivalences(input, source_props, &mut child);

            // Direct column references in `exprs` define the projection's
            // column mapping. Anything else (arithmetic, casts) drops the
            // partition lineage for that column.
            let mut input_to_outputs: HashMap<usize, Vec<usize>> = HashMap::new();
            for (out_idx, expr) in exprs.iter().enumerate() {
                if let PlanExpr::Column { index, .. } = expr {
                    input_to_outputs.entry(*index).or_default().push(out_idx);
                }
            }
            // Translate child equivalences forward.
            for (a, b) in child {
                if let (Some(a_outs), Some(b_outs)) =
                    (input_to_outputs.get(&a), input_to_outputs.get(&b))
                {
                    for a_o in a_outs {
                        for b_o in b_outs {
                            out.push((*a_o, *b_o));
                        }
                    }
                }
            }
            // A column projected twice (e.g. `SELECT k AS a, k AS b`)
            // makes its two output positions equivalent.
            for outs in input_to_outputs.values() {
                for i in 0..outs.len() {
                    for j in (i + 1)..outs.len() {
                        out.push((outs[i], outs[j]));
                    }
                }
            }
        }

        LogicalPlan::Join {
            left,
            right,
            condition,
            ..
        } => {
            let left_count = left.schema().len();
            // Left equivalences carry directly into the joined schema
            // (left columns occupy 0..left_count).
            derive_equivalences(left, source_props, out);
            // Right equivalences shift by left_count.
            let mut right_pairs = Vec::new();
            derive_equivalences(right, source_props, &mut right_pairs);
            for (a, b) in right_pairs {
                out.push((a + left_count, b + left_count));
            }
            // Equi-keys: each `(l, r)` becomes `(l, left_count + r)` in
            // the joined schema. Trino's `PropertyDerivations` does the
            // same in `PlanNodeProperties.translate`.
            if let Some((left_keys, right_keys)) =
                extract_partitioning_equi_keys(condition, left_count)
            {
                for (l, r) in left_keys.iter().zip(right_keys.iter()) {
                    out.push((*l, *r + left_count));
                }
            }
        }

        LogicalPlan::SemiJoin { left, .. } | LogicalPlan::AntiJoin { left, .. } => {
            // SemiJoin/AntiJoin output schema = left schema. Right
            // equivalences are not visible downstream.
            derive_equivalences(left, source_props, out);
        }

        LogicalPlan::ExchangeNode { stage_id, .. } => {
            // Pull equivalences across the stage boundary from the source
            // fragment's already-computed properties.
            if let Some(p) = source_props.get(stage_id) {
                for cls in p.equivalence_classes() {
                    for i in 0..cls.len() {
                        for j in (i + 1)..cls.len() {
                            out.push((cls[i], cls[j]));
                        }
                    }
                }
            }
        }

        // Aggregate, set ops, DDL, etc: conservative — no equivalences
        // derived. The fragmenter sets `output_partitioning` for these
        // explicitly when relevant.
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hash(cols: Vec<usize>, n: usize) -> PartitioningScheme {
        PartitioningScheme::Hash {
            columns: cols,
            partition_count: n,
        }
    }

    #[test]
    fn equivalence_pair_creates_class() {
        let p = ActualProperties::new(PartitioningScheme::Single).with_equivalence(1, 2);
        assert_eq!(p.equivalence_classes(), &[vec![1, 2]]);
    }

    #[test]
    fn equivalence_transitive_chains_into_one_class() {
        let p = ActualProperties::new(PartitioningScheme::Single)
            .with_equivalence(1, 2)
            .with_equivalence(2, 3);
        assert_eq!(p.equivalence_classes(), &[vec![1, 2, 3]]);
    }

    #[test]
    fn equivalence_merge_two_disjoint_classes() {
        let p = ActualProperties::new(PartitioningScheme::Single)
            .with_equivalence(1, 2)
            .with_equivalence(5, 6)
            .with_equivalence(2, 5);
        assert_eq!(p.equivalence_classes(), &[vec![1, 2, 5, 6]]);
    }

    #[test]
    fn equivalence_idempotent() {
        let p = ActualProperties::new(PartitioningScheme::Single)
            .with_equivalence(1, 2)
            .with_equivalence(1, 2);
        assert_eq!(p.equivalence_classes(), &[vec![1, 2]]);
    }

    #[test]
    fn equivalent_columns_returns_self_when_none_recorded() {
        let p = ActualProperties::new(PartitioningScheme::Single);
        assert_eq!(p.equivalent_columns(7), vec![7]);
    }

    #[test]
    fn satisfy_hash_via_equivalence() {
        // Hash on [0]; 0 is equivalent to 5 (e.g., post-join column alias).
        let p = ActualProperties::new(hash(vec![0], 4)).with_equivalence(0, 5);
        assert!(p.satisfy(&Distribution::HashPartitioned(vec![0])));
        assert!(
            p.satisfy(&Distribution::HashPartitioned(vec![5])),
            "equivalence (0,5) lets Hash{{[0]}} satisfy HashPartitioned([5])"
        );
        assert!(!p.satisfy(&Distribution::HashPartitioned(vec![3])));
    }

    #[test]
    fn satisfy_hash_via_transitive_equivalence() {
        let p = ActualProperties::new(hash(vec![0], 4))
            .with_equivalence(0, 7)
            .with_equivalence(7, 12);
        // Through transitivity, 0 ≡ 7 ≡ 12.
        assert!(p.satisfy(&Distribution::HashPartitioned(vec![12])));
    }

    #[test]
    fn satisfy_unspecified_passes_through() {
        let p = ActualProperties::new(hash(vec![0], 4));
        assert!(p.satisfy(&Distribution::Unspecified));
    }

    #[test]
    fn satisfy_single_partition_no_equivalence_shortcut() {
        // Equivalence does NOT turn Hash{N>1} into SinglePartition.
        let p = ActualProperties::new(hash(vec![0], 4)).with_equivalence(0, 5);
        assert!(!p.satisfy(&Distribution::SinglePartition));
    }

    // ----- derive_properties / derive_equivalences -----

    use crate::fragment::PlanFragmenter;
    use crate::plan::{JoinCondition, LogicalPlan, PlanExpr};
    use arneb_common::types::{ColumnInfo, DataType, TableReference};
    use arneb_sql_parser::ast::{self, BinaryOp};

    fn scan_with_cols(name: &str, cols: &[&str]) -> LogicalPlan {
        let schema = cols
            .iter()
            .map(|c| ColumnInfo {
                name: (*c).into(),
                data_type: DataType::Int32,
                nullable: false,
            })
            .collect();
        LogicalPlan::TableScan {
            table: TableReference::table(name),
            schema,
            alias: None,
            properties: Default::default(),
            dynamic_filters_consumed: Vec::new(),
        }
    }

    fn col(idx: usize, name: &str) -> PlanExpr {
        PlanExpr::Column {
            index: idx,
            name: name.into(),
            span: None,
        }
    }

    #[test]
    fn derive_properties_on_scan_fragment_has_no_equivalences() {
        let plan = scan_with_cols("t", &["k"]);
        let mut frag = PlanFragmenter::new();
        let result = frag.fragment(plan);
        let source = &result.source_fragments[0];
        let props = derive_properties(source);
        assert!(props.equivalence_classes().is_empty());
    }

    #[test]
    fn derive_properties_on_inner_join_extracts_equi_pair() {
        // L(k) ⋈ R(k) ON L.k = R.k. In the joined schema indices 0,1 are
        // L.k and R.k respectively, so the equi-key equivalence is (0, 1).
        let cond = JoinCondition::On(PlanExpr::BinaryOp {
            left: Box::new(col(0, "L.k")),
            op: BinaryOp::Eq,
            right: Box::new(col(1, "R.k")),
            span: None,
        });
        let plan = LogicalPlan::Join {
            left: Box::new(scan_with_cols("L", &["k"])),
            right: Box::new(scan_with_cols("R", &["k"])),
            join_type: ast::JoinType::Inner,
            condition: cond,
            dynamic_filter_ids: Vec::new(),
        };
        let mut frag = PlanFragmenter::new();
        let result = frag.fragment(plan);

        // result.source_fragments[0] is the Join fragment.
        let join_frag = &result.source_fragments[0];
        let props = derive_properties(join_frag);
        assert_eq!(
            props.equivalent_columns(0),
            vec![0, 1],
            "L.k and R.k must be equivalent after equi-join"
        );
    }

    #[test]
    fn equivalent_output_columns_excludes_samename_twin_includes_transitive_sibling() {
        // Mirrors the q08-vs-q18 distinction for dynamic-filter targeting.
        //   t1[a@0, b@1] ⋈ t3[d@2] ON t1.b = t3.d        -> b ≡ d
        //   (..) ⋈ t2[a@3, c@4]    ON t3.d = t2.c         -> d ≡ c  (transitive b≡d≡c)
        // t1.a (idx 0) and t2.a (idx 3) share the name "a" but are NOT
        // join-equal: the class of idx 0 must be {0} only (the q08 twin that
        // name-based routing would wrongly include). The class of idx 1 must
        // be {1,2,4} across three tables (the q18 transitive sibling).
        let join1 = LogicalPlan::Join {
            left: Box::new(scan_with_cols("t1", &["a", "b"])),
            right: Box::new(scan_with_cols("t3", &["d"])),
            join_type: ast::JoinType::Inner,
            condition: JoinCondition::On(PlanExpr::BinaryOp {
                left: Box::new(col(1, "b")),
                op: BinaryOp::Eq,
                right: Box::new(col(2, "d")),
                span: None,
            }),
            dynamic_filter_ids: Vec::new(),
        };
        let plan = LogicalPlan::Join {
            left: Box::new(join1),
            right: Box::new(scan_with_cols("t2", &["a", "c"])),
            join_type: ast::JoinType::Inner,
            condition: JoinCondition::On(PlanExpr::BinaryOp {
                left: Box::new(col(2, "d")),
                op: BinaryOp::Eq,
                right: Box::new(col(4, "c")),
                span: None,
            }),
            dynamic_filter_ids: Vec::new(),
        };

        let mut a_class = equivalent_output_columns(&plan, 0);
        a_class.sort_unstable();
        assert_eq!(
            a_class,
            vec![0],
            "t1.a must not be equivalent to the same-named t2.a (q08 twin must be excluded)"
        );

        let mut b_class = equivalent_output_columns(&plan, 1);
        b_class.sort_unstable();
        assert_eq!(
            b_class,
            vec![1, 2, 4],
            "t1.b ≡ t3.d ≡ t2.c transitively across joins (q18 sibling must be included)"
        );
    }

    #[test]
    fn derive_properties_propagates_equivalences_through_exchange() {
        // Same plan as above, but inspect the ROOT (outer) fragment. Its
        // root is an ExchangeNode pointing at the join fragment, so the
        // equi-key equivalence must propagate across the stage boundary
        // via `source_props` lookup.
        let cond = JoinCondition::On(PlanExpr::BinaryOp {
            left: Box::new(col(0, "L.k")),
            op: BinaryOp::Eq,
            right: Box::new(col(1, "R.k")),
            span: None,
        });
        let plan = LogicalPlan::Join {
            left: Box::new(scan_with_cols("L", &["k"])),
            right: Box::new(scan_with_cols("R", &["k"])),
            join_type: ast::JoinType::Inner,
            condition: cond,
            dynamic_filter_ids: Vec::new(),
        };
        let mut frag = PlanFragmenter::new();
        let result = frag.fragment(plan);

        let props = derive_properties(&result);
        assert_eq!(
            props.equivalent_columns(0),
            vec![0, 1],
            "equivalence must cross the ExchangeNode boundary"
        );
    }
}
