//! A join that runs one task per bucket only finds a match when both of its
//! inputs put equal keys in the same bucket: the two inputs have to be
//! bucketed on the columns the join compares, pair by pair.
//!
//! Measured on two workers with `inventory` rows of one warehouse (5,220,261
//! rows, one per item and date) joined to themselves on item and date: with
//! both sides in table order the join returned all 5,220,261 rows. With one
//! side read through a subquery or CTE that lists the same columns in another
//! order (`inv_item_sk, inv_date_sk, ..`) it returned 2,609,598, and a `LEFT
//! JOIN` reported 2,610,663 of its rows as unmatched. That side was bucketed
//! on (date, item) and the other on (item, date). The same happened with the
//! reordered side written second, with the keys compared in table order, with
//! a key computed in the subquery (`inv_item_sk + 0 AS k`: 2,609,719), with a
//! single key joined to `customer` (2,609,719), and below `EXISTS`
//! (2,609,719) and `NOT EXISTS` (2,610,542 rows where none is correct).
//!
//! A subquery that computes a key and gives it the name and position of the
//! column it replaces lost rows the same way: `CAST(inv_date_sk + 7 AS
//! INTEGER) AS inv_date_sk` returned 2,589,167 rows where 5,180,261 is
//! correct, `CAST(inv_item_sk + 1 AS INTEGER) AS inv_item_sk` 696,378 where
//! 1,393,455 is correct, and below `EXISTS` 2,623,250 where 5,220,000 is
//! correct. That side was bucketed on the column the key was computed from.
//!
//! Two subqueries, one inside the other, that between them swap the key
//! columns lost rows the same way: 2,609,598 joined on both keys, 2,609,719
//! joined to `customer` on one key, 2,609,719 below `EXISTS`.
//!
//! So did a `GROUP BY` subquery that lists its two keys in another order than
//! its `GROUP BY` has them. `inventory` has 5,220,261 groups per date and
//! item. Joined on both keys to the same groups listed in `GROUP BY` order,
//! the join returned 2,609,598 rows, whichever side was written first, and
//! 2,609,719 below `EXISTS`. That side was bucketed on (date, item) below
//! the final step of its aggregate, the other on (item, date).
//!
//! Subqueries that leave the key columns where the table or the `GROUP BY`
//! has them, or that reorder both sides the same way, returned the correct
//! 5,220,261 rows from a join with one task per bucket, and keep that shape.

use std::collections::HashMap;
use std::sync::Arc;

use arneb_catalog::{
    CatalogManager, ColumnStatistics, MemoryCatalog, MemorySchema, TableProvider, TableStatistics,
};
use arneb_common::types::{ColumnInfo, DataType};
use arneb_planner::{
    plan_children, FragmentType, JoinCondition, LogicalOptimizer, LogicalPlan, PartitioningScheme,
    PlanExpr, PlanFragment, PlanFragmenter, QueryPlanner,
};
use arneb_sql_parser::ast::BinaryOp;

const WORKERS: usize = 2;
const BROADCAST_MAX_BUILD_BYTES: usize = 100 * 1024 * 1024;

/// One warehouse of `inventory`, columns in table order.
const IN_TABLE_ORDER: &str = "(SELECT * FROM inventory WHERE inv_warehouse_sk = 1)";
/// The same rows with the two key columns swapped.
const KEYS_SWAPPED: &str =
    "(SELECT inv_item_sk, inv_date_sk, inv_warehouse_sk, inv_quantity_on_hand \
     FROM inventory WHERE inv_warehouse_sk = 1)";

/// The same rows through two subqueries: the inner one lists the key columns
/// last, the outer one first, item before date.
const KEYS_SWAPPED_IN_TWO_STEPS: &str = "(SELECT inv_item_sk, inv_date_sk, w, q \
     FROM (SELECT inv_warehouse_sk w, inv_quantity_on_hand q, inv_date_sk, inv_item_sk \
           FROM inventory WHERE inv_warehouse_sk = 1) t)";
/// Rows per date and item, the keys listed as the `GROUP BY` has them.
const COUNTS_IN_GROUP_ORDER: &str = "(SELECT inv_date_sk, inv_item_sk, count(*) c \
     FROM inventory GROUP BY inv_date_sk, inv_item_sk)";
/// The same groups, the keys listed the other way round.
const COUNTS_KEYS_SWAPPED: &str = "(SELECT inv_item_sk, inv_date_sk, count(*) c \
     FROM inventory GROUP BY inv_date_sk, inv_item_sk)";
const ON_ITEM_AND_DATE: &str = "ON a.inv_item_sk = b.inv_item_sk AND a.inv_date_sk = b.inv_date_sk";
const ITEM_IN_WAREHOUSE_TWO: &str = "(SELECT 1 FROM inventory b \
     WHERE b.inv_warehouse_sk = 2 AND b.inv_item_sk = a.inv_item_sk)";

#[tokio::test]
async fn subquery_with_swapped_key_columns_joined_on_both_keys() {
    let fragments = fragment(&format!(
        "SELECT count(*) FROM {KEYS_SWAPPED} a JOIN {IN_TABLE_ORDER} b \
         ON a.inv_item_sk = b.inv_item_sk AND a.inv_date_sk = b.inv_date_sk"
    ))
    .await;
    assert_one_join_task_per_bucket(&fragments);
}

#[tokio::test]
async fn subquery_with_swapped_key_columns_written_second() {
    let fragments = fragment(&format!(
        "SELECT count(*) FROM {IN_TABLE_ORDER} b JOIN {KEYS_SWAPPED} a \
         ON a.inv_item_sk = b.inv_item_sk AND a.inv_date_sk = b.inv_date_sk"
    ))
    .await;
    assert_one_join_task_per_bucket(&fragments);
}

#[tokio::test]
async fn subquery_with_swapped_key_columns_compared_in_table_order() {
    let fragments = fragment(&format!(
        "SELECT count(*) FROM {KEYS_SWAPPED} a JOIN {IN_TABLE_ORDER} b \
         ON a.inv_date_sk = b.inv_date_sk AND a.inv_item_sk = b.inv_item_sk"
    ))
    .await;
    assert_one_join_task_per_bucket(&fragments);
}

#[tokio::test]
async fn cte_with_swapped_key_columns_joined_on_both_keys() {
    let fragments = fragment(&format!(
        "WITH a AS {KEYS_SWAPPED}, b AS {IN_TABLE_ORDER} \
         SELECT count(*) FROM a JOIN b \
         ON a.inv_item_sk = b.inv_item_sk AND a.inv_date_sk = b.inv_date_sk"
    ))
    .await;
    assert_one_join_task_per_bucket(&fragments);
}

#[tokio::test]
async fn subquery_with_swapped_key_columns_left_joined() {
    let fragments = fragment(&format!(
        "SELECT count(*), count(*) - count(b.inv_item_sk) \
         FROM {KEYS_SWAPPED} a LEFT JOIN {IN_TABLE_ORDER} b \
         ON a.inv_item_sk = b.inv_item_sk AND a.inv_date_sk = b.inv_date_sk"
    ))
    .await;
    assert_one_join_task_per_bucket(&fragments);
}

#[tokio::test]
async fn subquery_with_swapped_key_columns_joined_on_one_key() {
    let fragments = fragment(&format!(
        "SELECT count(*) FROM {KEYS_SWAPPED} a JOIN customer ON a.inv_item_sk = c_customer_sk"
    ))
    .await;
    assert_one_join_task_per_bucket(&fragments);
}

#[tokio::test]
async fn subquery_with_swapped_key_columns_filtered_by_exists() {
    let fragments = fragment(&format!(
        "SELECT count(*) FROM {KEYS_SWAPPED} a WHERE EXISTS \
         (SELECT 1 FROM inventory b \
          WHERE b.inv_warehouse_sk = 2 AND b.inv_item_sk = a.inv_item_sk)"
    ))
    .await;
    assert_one_join_task_per_bucket(&fragments);
}

#[tokio::test]
async fn subquery_with_swapped_key_columns_filtered_by_not_exists() {
    let fragments = fragment(&format!(
        "SELECT count(*) FROM {KEYS_SWAPPED} a WHERE NOT EXISTS \
         (SELECT 1 FROM inventory b \
          WHERE b.inv_warehouse_sk = 2 AND b.inv_item_sk = a.inv_item_sk)"
    ))
    .await;
    assert_one_join_task_per_bucket(&fragments);
}

/// The subquery computes its key, so the key is not a column of the table the
/// subquery reads. Whatever shape the join takes, it must not compare buckets
/// built on different columns.
#[tokio::test]
async fn subquery_with_a_computed_key_where_the_table_has_another_column() {
    let fragments = fragment(&format!(
        "SELECT count(*) \
         FROM (SELECT inv_item_sk + 0 AS k, inv_date_sk, inv_warehouse_sk, inv_quantity_on_hand \
               FROM inventory WHERE inv_warehouse_sk = 1) a \
         JOIN {IN_TABLE_ORDER} b ON a.k = b.inv_item_sk AND a.inv_date_sk = b.inv_date_sk"
    ))
    .await;
    assert_join_sides_bucket_on_the_join_keys(&fragments);
}

/// Measured: 2,589,167 rows where 5,180,261 is correct.
#[tokio::test]
async fn subquery_with_a_computed_key_named_like_the_column_it_replaces() {
    let fragments = fragment(&format!(
        "SELECT count(*) \
         FROM (SELECT CAST(inv_date_sk + 7 AS INTEGER) AS inv_date_sk, inv_item_sk, \
                      inv_warehouse_sk, inv_quantity_on_hand \
               FROM inventory WHERE inv_warehouse_sk = 1) a \
         JOIN {IN_TABLE_ORDER} b \
         ON a.inv_item_sk = b.inv_item_sk AND a.inv_date_sk = b.inv_date_sk"
    ))
    .await;
    assert_join_sides_bucket_on_the_join_keys(&fragments);
}

/// Measured: 696,378 rows where 1,393,455 is correct.
#[tokio::test]
async fn subquery_with_a_second_computed_key_named_like_the_column_it_replaces() {
    let fragments = fragment(&format!(
        "SELECT count(*) \
         FROM (SELECT inv_date_sk, CAST(inv_item_sk + 1 AS INTEGER) AS inv_item_sk, \
                      inv_warehouse_sk, inv_quantity_on_hand \
               FROM inventory WHERE inv_warehouse_sk = 1) a \
         JOIN {IN_TABLE_ORDER} b \
         ON a.inv_item_sk = b.inv_item_sk AND a.inv_date_sk = b.inv_date_sk"
    ))
    .await;
    assert_join_sides_bucket_on_the_join_keys(&fragments);
}

/// Measured: 2,623,250 rows where 5,220,000 is correct.
#[tokio::test]
async fn subquery_with_a_computed_key_named_like_its_column_filtered_by_exists() {
    let fragments = fragment(
        "SELECT count(*) \
         FROM (SELECT inv_date_sk, CAST(inv_item_sk + 1 AS INTEGER) AS inv_item_sk, \
                      inv_warehouse_sk, inv_quantity_on_hand \
               FROM inventory WHERE inv_warehouse_sk = 1) a \
         WHERE EXISTS (SELECT 1 FROM inventory b \
                       WHERE b.inv_warehouse_sk = 2 AND b.inv_item_sk = a.inv_item_sk)",
    )
    .await;
    assert_join_sides_bucket_on_the_join_keys(&fragments);
}

/// The subquery gives each key column the other's name. Not measured: item
/// and date numbers do not overlap in the data set, so the correct result is
/// empty.
#[tokio::test]
async fn subquery_naming_each_key_column_after_the_other() {
    let fragments = fragment(&format!(
        "SELECT count(*) \
         FROM (SELECT inv_item_sk AS inv_date_sk, inv_date_sk AS inv_item_sk, \
                      inv_warehouse_sk, inv_quantity_on_hand \
               FROM inventory WHERE inv_warehouse_sk = 1) a \
         JOIN {IN_TABLE_ORDER} b \
         ON a.inv_item_sk = b.inv_item_sk AND a.inv_date_sk = b.inv_date_sk"
    ))
    .await;
    assert_join_sides_bucket_on_the_join_keys(&fragments);
}

/// Not measured on the cluster: there the planner broadcast the third table
/// and the query returned the correct count. With broadcast joins off, all
/// three inputs are hash-partitioned.
#[tokio::test]
async fn subquery_with_swapped_key_columns_over_a_join_joined_again() {
    let fragments = fragment_with_broadcast_limit(
        None,
        "SELECT count(*) \
         FROM (SELECT x.inv_item_sk, x.inv_date_sk, x.inv_warehouse_sk, x.inv_quantity_on_hand, \
                      y.inv_date_sk AS d2, y.inv_item_sk AS i2, y.inv_warehouse_sk AS w2, \
                      y.inv_quantity_on_hand AS q2 \
               FROM inventory x JOIN inventory y \
                 ON x.inv_item_sk = y.inv_item_sk AND x.inv_date_sk = y.inv_date_sk \
               WHERE x.inv_warehouse_sk = 1 AND y.inv_warehouse_sk = 2) c \
         JOIN (SELECT * FROM inventory WHERE inv_warehouse_sk = 3) z \
           ON c.inv_item_sk = z.inv_item_sk AND c.inv_date_sk = z.inv_date_sk",
    )
    .await;
    assert_join_sides_bucket_on_the_join_keys(&fragments);
}

/// Measured: 2,609,598 rows where 5,220,261 are correct.
#[tokio::test]
async fn nested_subqueries_swapping_the_key_columns_joined_on_both_keys() {
    let fragments = fragment(&format!(
        "SELECT count(*) FROM {KEYS_SWAPPED_IN_TWO_STEPS} a JOIN {IN_TABLE_ORDER} b \
         {ON_ITEM_AND_DATE}"
    ))
    .await;
    assert_one_join_task_per_bucket(&fragments);
}

/// Measured: 2,609,719 rows where 5,220,261 are correct.
#[tokio::test]
async fn nested_subqueries_swapping_the_key_columns_joined_on_one_key() {
    let fragments = fragment(&format!(
        "SELECT count(*) FROM {KEYS_SWAPPED_IN_TWO_STEPS} a \
         JOIN customer c ON a.inv_item_sk = c.c_customer_sk"
    ))
    .await;
    assert_one_join_task_per_bucket(&fragments);
}

/// Measured: 2,609,719 rows where 5,220,261 are correct.
#[tokio::test]
async fn nested_subqueries_swapping_the_key_columns_filtered_by_exists() {
    let fragments = fragment(&format!(
        "SELECT count(*) FROM {KEYS_SWAPPED_IN_TWO_STEPS} a WHERE EXISTS {ITEM_IN_WAREHOUSE_TWO}"
    ))
    .await;
    assert_one_join_task_per_bucket(&fragments);
}

/// The inner subquery computes a column that is not a key and the outer one
/// filters on it and swaps the keys. Not measured: on the cluster the planner
/// broadcast one side. With broadcast joins off, both inputs are
/// hash-partitioned.
#[tokio::test]
async fn nested_subqueries_swapping_the_key_columns_around_a_filter() {
    let fragments = fragment_with_broadcast_limit(
        None,
        &format!(
            "SELECT count(*) \
             FROM (SELECT inv_item_sk, inv_date_sk, inv_warehouse_sk, q \
                   FROM (SELECT inv_date_sk, inv_item_sk, inv_warehouse_sk, \
                                inv_quantity_on_hand + 1 AS q \
                         FROM inventory WHERE inv_warehouse_sk = 1) t \
                   WHERE q > 5) a \
             JOIN {IN_TABLE_ORDER} b {ON_ITEM_AND_DATE}"
        ),
    )
    .await;
    assert_join_sides_bucket_on_the_join_keys(&fragments);
}

/// Measured: 2,609,598 rows where 5,220,261 are correct.
#[tokio::test]
async fn aggregate_listing_its_keys_in_another_order_joined_to_an_aggregate() {
    let fragments = fragment(&format!(
        "SELECT count(*) FROM {COUNTS_KEYS_SWAPPED} a JOIN {COUNTS_IN_GROUP_ORDER} b \
         {ON_ITEM_AND_DATE}"
    ))
    .await;
    assert_one_join_task_per_bucket(&fragments);
}

/// Measured: 2,609,598 rows where 5,220,261 are correct.
#[tokio::test]
async fn aggregate_listing_its_keys_in_another_order_written_second() {
    let fragments = fragment(&format!(
        "SELECT count(*) FROM {COUNTS_IN_GROUP_ORDER} a JOIN {COUNTS_KEYS_SWAPPED} b \
         {ON_ITEM_AND_DATE}"
    ))
    .await;
    assert_one_join_task_per_bucket(&fragments);
}

/// The select list is in table order and the `GROUP BY` is not. Measured:
/// 2,609,598 rows where 5,220,261 are correct.
#[tokio::test]
async fn aggregate_grouping_by_its_keys_in_another_order_than_it_lists_them() {
    let fragments = fragment(&format!(
        "SELECT count(*) \
         FROM (SELECT inv_date_sk, inv_item_sk, count(*) c \
               FROM inventory GROUP BY inv_item_sk, inv_date_sk) a \
         JOIN {COUNTS_IN_GROUP_ORDER} b {ON_ITEM_AND_DATE}"
    ))
    .await;
    assert_one_join_task_per_bucket(&fragments);
}

/// Measured: 2,609,719 rows where 5,220,261 are correct.
#[tokio::test]
async fn aggregate_listing_its_keys_in_another_order_filtered_by_exists() {
    let fragments = fragment(&format!(
        "SELECT count(*) FROM {COUNTS_KEYS_SWAPPED} a WHERE EXISTS {ITEM_IN_WAREHOUSE_TWO}"
    ))
    .await;
    assert_one_join_task_per_bucket(&fragments);
}

/// Not measured in this shape: on the cluster the planner broadcast the rows
/// of the one warehouse instead of hash-partitioning both inputs.
#[tokio::test]
async fn aggregate_listing_its_keys_in_another_order_joined_to_table_rows() {
    let fragments = fragment(&format!(
        "SELECT count(*) FROM {COUNTS_KEYS_SWAPPED} a JOIN {IN_TABLE_ORDER} b {ON_ITEM_AND_DATE}"
    ))
    .await;
    assert_join_sides_bucket_on_the_join_keys(&fragments);
}

/// Measured correct (5,220,261 rows) from a join with one task per bucket.
#[tokio::test]
async fn aggregates_in_group_order_keep_one_join_task_per_bucket() {
    let fragments = fragment(&format!(
        "SELECT count(*) FROM {COUNTS_IN_GROUP_ORDER} a JOIN {COUNTS_IN_GROUP_ORDER} b \
         {ON_ITEM_AND_DATE}"
    ))
    .await;
    assert_one_join_task_per_bucket(&fragments);
}

/// Measured correct (5,220,261 rows) from a join with one task per bucket.
#[tokio::test]
async fn aggregates_listing_their_keys_the_same_other_way_keep_one_join_task_per_bucket() {
    let fragments = fragment(&format!(
        "SELECT count(*) FROM {COUNTS_KEYS_SWAPPED} a JOIN {COUNTS_KEYS_SWAPPED} b \
         {ON_ITEM_AND_DATE}"
    ))
    .await;
    assert_one_join_task_per_bucket(&fragments);
}

/// Measured correct (5,220,261 rows) from a join with one task per bucket.
#[tokio::test]
async fn exists_over_an_aggregate_in_group_order_keeps_one_join_task_per_bucket() {
    let fragments = fragment(&format!(
        "SELECT count(*) FROM {COUNTS_IN_GROUP_ORDER} a WHERE EXISTS {ITEM_IN_WAREHOUSE_TWO}"
    ))
    .await;
    assert_one_join_task_per_bucket(&fragments);
}

/// Measured correct (5,220,261 rows) from a join with one task per bucket.
#[tokio::test]
async fn subqueries_in_table_order_keep_one_join_task_per_bucket() {
    let fragments = fragment(&format!(
        "SELECT count(*) FROM {IN_TABLE_ORDER} a JOIN {IN_TABLE_ORDER} b \
         ON a.inv_item_sk = b.inv_item_sk AND a.inv_date_sk = b.inv_date_sk"
    ))
    .await;
    assert_one_join_task_per_bucket(&fragments);
}

/// Measured correct (5,220,261 rows) from a join with one task per bucket:
/// the subquery moves columns, but not the keys.
#[tokio::test]
async fn subquery_swapping_other_columns_keeps_one_join_task_per_bucket() {
    let fragments = fragment(&format!(
        "SELECT count(*) \
         FROM (SELECT inv_date_sk, inv_item_sk, inv_quantity_on_hand, inv_warehouse_sk \
               FROM inventory WHERE inv_warehouse_sk = 1) a \
         JOIN {IN_TABLE_ORDER} b \
         ON a.inv_item_sk = b.inv_item_sk AND a.inv_date_sk = b.inv_date_sk"
    ))
    .await;
    assert_one_join_task_per_bucket(&fragments);
}

/// Measured correct (5,220,261 rows) from a join with one task per bucket.
#[tokio::test]
async fn both_sides_swapped_the_same_way_keep_one_join_task_per_bucket() {
    let fragments = fragment(&format!(
        "SELECT count(*) FROM {KEYS_SWAPPED} a JOIN {KEYS_SWAPPED} b \
         ON a.inv_item_sk = b.inv_item_sk AND a.inv_date_sk = b.inv_date_sk"
    ))
    .await;
    assert_one_join_task_per_bucket(&fragments);
}

/// Measured correct (5,220,261 rows) from a join with one task per bucket.
#[tokio::test]
async fn exists_over_a_subquery_in_table_order_keeps_one_join_task_per_bucket() {
    let fragments = fragment(&format!(
        "SELECT count(*) FROM {IN_TABLE_ORDER} a WHERE EXISTS \
         (SELECT 1 FROM inventory b \
          WHERE b.inv_warehouse_sk = 2 AND b.inv_item_sk = a.inv_item_sk)"
    ))
    .await;
    assert_one_join_task_per_bucket(&fragments);
}

/// Measured correct (5,220,261 rows) from a join with one task per bucket.
#[tokio::test]
async fn table_in_table_order_joined_to_customer_keeps_one_join_task_per_bucket() {
    let fragments = fragment(&format!(
        "SELECT count(*) FROM {IN_TABLE_ORDER} a JOIN customer ON a.inv_item_sk = c_customer_sk"
    ))
    .await;
    assert_one_join_task_per_bucket(&fragments);
}

async fn fragment(sql: &str) -> PlanFragment {
    fragment_with_broadcast_limit(Some(BROADCAST_MAX_BUILD_BYTES), sql).await
}

async fn fragment_with_broadcast_limit(max_build_bytes: Option<usize>, sql: &str) -> PlanFragment {
    let catalog = tpcds_catalog();
    let stmt = arneb_sql_parser::parse(sql).expect("parse");
    let planner = QueryPlanner::new(&catalog);
    let (plan, ctx) = planner
        .plan_statement_with_context(&stmt)
        .await
        .expect("plan");
    let plan = LogicalOptimizer::default_rules()
        .optimize(plan)
        .expect("optimize");
    PlanFragmenter::new()
        .with_broadcast_threshold(max_build_bytes)
        .with_stats(Some(ctx.catalog_stats.clone()))
        .with_worker_count(WORKERS)
        .fragment(plan)
}

/// The join runs one task per bucket, and compares the columns its two
/// inputs are bucketed on.
fn assert_one_join_task_per_bucket(root: &PlanFragment) {
    let checked = assert_join_sides_bucket_on_the_join_keys(root);
    assert!(
        checked > 0,
        "no join runs one task per bucket of two hash-partitioned inputs:\n{}",
        describe(root, 0)
    );
}

/// Every join that runs one task per bucket of two hash-partitioned inputs
/// compares, pair by pair, the columns those inputs are bucketed on: the n-th
/// bucketing column of one input reaches the join unchanged and is compared
/// for equality with the n-th bucketing column of the other input. Returns the
/// number of such joins.
fn assert_join_sides_bucket_on_the_join_keys(root: &PlanFragment) -> usize {
    let mut checked = 0;
    for_each_fragment(root, &mut |f| {
        if f.fragment_type != FragmentType::HashPartitioned {
            return;
        }
        for_each_plan(&f.root, &mut |plan| {
            let Some(compared) = compared_columns(plan) else {
                return;
            };
            let [left, right] = plan_children(plan)[..] else {
                return;
            };
            let (Some(left), Some(right)) = (BucketedSide::of(left, f), BucketedSide::of(right, f))
            else {
                return;
            };
            checked += 1;
            let aligned = !left.bucketed_on.is_empty()
                && left.bucketed_on.len() == right.bucketed_on.len()
                && left
                    .bucketed_on
                    .iter()
                    .zip(&right.bucketed_on)
                    .all(|(l, r)| {
                        compared.iter().any(|(left_key, right_key)| {
                            left.passes_on.get(*left_key) == Some(&Some(*l))
                                && right.passes_on.get(*right_key) == Some(&Some(*r))
                        })
                    });
            assert!(
                aligned,
                "a join with one task per bucket reads one input bucketed on {:?} and the other \
                 on {:?}, and does not compare those columns pair by pair, so rows with equal \
                 keys land in different tasks:\n{f}\n{}\nwhole plan:\n{}",
                left.bucketed_on_names,
                right.bucketed_on_names,
                f.root,
                describe(root, 0)
            );
        });
    });
    checked
}

/// The pairs of columns a join compares for equality: positions in the output
/// of its left and of its right input.
fn compared_columns(plan: &LogicalPlan) -> Option<Vec<(usize, usize)>> {
    match plan {
        LogicalPlan::Join {
            left, condition, ..
        } => {
            let mut pairs = Vec::new();
            if let JoinCondition::On(expr) = condition {
                collect_compared_columns(expr, left.schema().len(), &mut pairs);
            }
            Some(pairs)
        }
        LogicalPlan::SemiJoin {
            left_key,
            right_key,
            ..
        }
        | LogicalPlan::AntiJoin {
            left_key,
            right_key,
            ..
        } => Some(match (left_key, right_key) {
            (PlanExpr::Column { index: l, .. }, PlanExpr::Column { index: r, .. }) => {
                vec![(*l, *r)]
            }
            _ => Vec::new(),
        }),
        _ => None,
    }
}

fn collect_compared_columns(expr: &PlanExpr, left_width: usize, pairs: &mut Vec<(usize, usize)>) {
    match expr {
        PlanExpr::BinaryOp {
            left,
            op: BinaryOp::And,
            right,
            ..
        } => {
            collect_compared_columns(left, left_width, pairs);
            collect_compared_columns(right, left_width, pairs);
        }
        PlanExpr::BinaryOp {
            left,
            op: BinaryOp::Eq,
            right,
            ..
        } => {
            if let (PlanExpr::Column { index: a, .. }, PlanExpr::Column { index: b, .. }) =
                (left.as_ref(), right.as_ref())
            {
                let (l, r) = (*a.min(b), *a.max(b));
                if l < left_width && r >= left_width {
                    pairs.push((l, r - left_width));
                }
            }
        }
        _ => {}
    }
}

/// One input of a join that reads a single hash-partitioned source of its
/// fragment without joining it first. It may finish a two-step aggregate on
/// the way: the final step hands on its group keys unchanged.
struct BucketedSide {
    /// The columns of the source's output its rows are bucketed on.
    bucketed_on: Vec<usize>,
    bucketed_on_names: Vec<String>,
    /// For each column this side hands to the join: the column of the
    /// source's output it passes on unchanged, if it does.
    passes_on: Vec<Option<usize>>,
}

impl BucketedSide {
    fn of(side: &LogicalPlan, fragment: &PlanFragment) -> Option<Self> {
        match side {
            LogicalPlan::ExchangeNode { stage_id, schema } => {
                let source = fragment
                    .source_fragments
                    .iter()
                    .find(|source| source.id == *stage_id)?;
                match &source.output_partitioning {
                    PartitioningScheme::Hash { columns, .. } if !columns.is_empty() => {
                        let source_schema = source.root.schema();
                        Some(Self {
                            bucketed_on: columns.clone(),
                            bucketed_on_names: columns
                                .iter()
                                .map(|&i| source_schema[i].name.clone())
                                .collect(),
                            passes_on: (0..schema.len()).map(Some).collect(),
                        })
                    }
                    _ => None,
                }
            }
            LogicalPlan::Filter { input, .. } => Self::of(input, fragment),
            LogicalPlan::Projection { input, exprs, .. } => {
                let below = Self::of(input, fragment)?;
                let passes_on = exprs
                    .iter()
                    .map(|expr| match expr {
                        PlanExpr::Column { index, .. } => below.passes_on[*index],
                        _ => None,
                    })
                    .collect();
                Some(Self { passes_on, ..below })
            }
            LogicalPlan::FinalAggregate {
                input,
                group_by,
                schema,
                ..
            } => {
                let below = Self::of(input, fragment)?;
                let passes_on = (0..schema.len())
                    .map(|column| match group_by.get(column) {
                        Some(PlanExpr::Column { index, .. }) => below.passes_on[*index],
                        _ => None,
                    })
                    .collect();
                Some(Self { passes_on, ..below })
            }
            _ => None,
        }
    }
}

fn for_each_plan<'a>(plan: &'a LogicalPlan, visit: &mut impl FnMut(&'a LogicalPlan)) {
    visit(plan);
    for child in plan_children(plan) {
        for_each_plan(child, visit);
    }
}

fn describe(fragment: &PlanFragment, depth: usize) -> String {
    let pad = "  ".repeat(depth);
    let mut out = format!("{pad}{fragment}\n");
    for line in fragment.root.to_string().lines() {
        out.push_str(&format!("{pad}{line}\n"));
    }
    for child in &fragment.source_fragments {
        out.push_str(&describe(child, depth + 1));
    }
    out
}

fn for_each_fragment<'a>(fragment: &'a PlanFragment, visit: &mut impl FnMut(&'a PlanFragment)) {
    visit(fragment);
    for child in &fragment.source_fragments {
        for_each_fragment(child, visit);
    }
}

#[derive(Debug)]
struct StatsTable {
    schema: Vec<ColumnInfo>,
    stats: TableStatistics,
}

impl TableProvider for StatsTable {
    fn schema(&self) -> Vec<ColumnInfo> {
        self.schema.clone()
    }
    fn statistics(&self) -> Option<TableStatistics> {
        Some(self.stats.clone())
    }
    fn canonical_name(&self) -> Option<(String, String)> {
        None
    }
}

/// `inventory` and `customer` with the columns, row counts and distinct
/// counts of the data set the wrong results were measured on.
fn tpcds_catalog() -> CatalogManager {
    let manager = CatalogManager::new("default", "public");
    let catalog = Arc::new(MemoryCatalog::new());
    let schema = Arc::new(MemorySchema::new());

    register(
        &schema,
        "inventory",
        &[
            ("inv_date_sk", DataType::Int32),
            ("inv_item_sk", DataType::Int32),
            ("inv_warehouse_sk", DataType::Int32),
            ("inv_quantity_on_hand", DataType::Int32),
        ],
        31_321_566,
        &[
            ("inv_date_sk", 261),
            ("inv_item_sk", 40_000),
            ("inv_warehouse_sk", 6),
        ],
    );
    register(
        &schema,
        "customer",
        &[
            ("c_customer_sk", DataType::Int32),
            ("c_customer_id", DataType::Utf8),
            ("c_current_cdemo_sk", DataType::Int32),
            ("c_current_hdemo_sk", DataType::Int32),
            ("c_current_addr_sk", DataType::Int32),
            ("c_first_shipto_date_sk", DataType::Int32),
            ("c_first_sales_date_sk", DataType::Int32),
            ("c_salutation", DataType::Utf8),
            ("c_first_name", DataType::Utf8),
            ("c_last_name", DataType::Utf8),
            ("c_preferred_cust_flag", DataType::Utf8),
            ("c_birth_day", DataType::Int32),
            ("c_birth_month", DataType::Int32),
            ("c_birth_year", DataType::Int32),
            ("c_birth_country", DataType::Utf8),
            ("c_login", DataType::Utf8),
            ("c_email_address", DataType::Utf8),
            ("c_last_review_date_sk", DataType::Int32),
        ],
        433_000,
        &[("c_customer_sk", 433_000)],
    );

    catalog.register_schema("public", schema);
    manager.register_catalog("default", catalog);
    manager
}

fn register(
    schema: &MemorySchema,
    name: &str,
    columns: &[(&str, DataType)],
    row_count: u64,
    ndvs: &[(&str, u64)],
) {
    let cols = columns
        .iter()
        .map(|(name, data_type)| ColumnInfo {
            name: (*name).to_string(),
            data_type: data_type.clone(),
            nullable: false,
        })
        .collect();
    let column_stats = ndvs
        .iter()
        .map(|(column, ndv)| {
            (
                (*column).to_string(),
                ColumnStatistics {
                    ndv: Some(*ndv),
                    null_fraction: Some(0.0),
                    min_value: None,
                    max_value: None,
                },
            )
        })
        .collect::<HashMap<_, _>>();
    schema.register_table(
        name,
        Arc::new(StatsTable {
            schema: cols,
            stats: TableStatistics {
                row_count: Some(row_count),
                size_bytes: None,
                columns: column_stats,
            },
        }),
    );
}
