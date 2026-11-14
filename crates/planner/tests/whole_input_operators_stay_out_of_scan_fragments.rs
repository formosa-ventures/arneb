//! Some operators are only correct when one task sees all of their input:
//! an aggregate that cannot be split into a partial and a final step
//! (`stddev_samp`, `count(DISTINCT ..)`, a bare `GROUP BY` / `SELECT
//! DISTINCT`) and a `LIMIT` / `OFFSET`. A fragment that scans a table, or
//! that joins hash-partitioned inputs, runs several tasks, each over its own
//! slice of the rows. Such an operator must not run in a fragment whose tasks
//! each see a slice of its input: every task would apply it to its own slice.
//! The slice is the task's share of the fragment's own scan or join, or its
//! one bucket of a hash-partitioned input.
//!
//! TPC-DS q39b on two workers returned 54 rows where 21 are correct (#250).
//! Its two `inv` aggregates are joined with a build side small enough to
//! broadcast, and the join was folded into the fragment that scans
//! `inventory` together with the probe side's aggregate. Measured there: every
//! group on that side was built from 2 or 3 of its 4 rows. Measured on the
//! same two workers with a subquery joined to `warehouse`: `LIMIT 1000000`
//! returned 2,000,000 rows; `SELECT DISTINCT` and `count(DISTINCT ..)` each
//! returned every group twice (480,000 rows for 240,000 groups), and so did a
//! `SELECT DISTINCT` over a hash-partitioned join (80,000 rows for 40,000).
//!
//! A subquery as wide as the table it reads took a second route: its join ran
//! one task per bucket of that table, and the subquery's operator ran inside
//! each task. Measured: `(SELECT * FROM inventory LIMIT 1000)` left-joined to
//! `warehouse` or to `item` returned 2,000 rows; a four-column
//! `count(DISTINCT ..)` aggregate per warehouse left-joined to `warehouse`
//! returned 12 rows for 6 groups, every group's 261 days split 134 / 127.
//!
//! An aggregate that does split took a third route when its subquery lists
//! the columns in another order than the aggregate produces them (group keys
//! in `GROUP BY` order, then the aggregates): both of its steps ran inside
//! the fragment that scans the table, with no exchange between them, so
//! every task finished its own share of each group. Measured: rows per
//! warehouse and item (240,000 groups) listed item first and joined to
//! `warehouse` returned 480,000 rows, the largest group counting 172 rows
//! where 261 are correct; so did the count listed first, and `warehouse`
//! written first. Rows per date and item listed item first and joined on
//! both keys to 4,982 `inventory` rows returned 7,380 rows, and joined to
//! one warehouse of `inventory` 7,735,735 where 5,220,261 are correct. With
//! the columns listed in the aggregate's own order, or with a `LEFT JOIN`,
//! each of these returned the correct count.
//!
//! Joins that already run as one task returned correct results with the same
//! inputs (q39a, `LEFT JOIN`), and keep their shape.

use std::collections::HashMap;
use std::sync::Arc;

use arneb_catalog::{
    CatalogManager, ColumnStatistics, MemoryCatalog, MemorySchema, TableProvider, TableStatistics,
};
use arneb_common::types::{ColumnInfo, DataType};
use arneb_planner::{
    plan_children, FragmentType, LogicalOptimizer, LogicalPlan, PartitioningScheme, PlanFragment,
    PlanFragmenter, QueryPlanner,
};

const Q39A_SQL: &str = include_str!("../../../benchmarks/tpcds/queries/q39a.sql");
const Q39B_SQL: &str = include_str!("../../../benchmarks/tpcds/queries/q39b.sql");

const WORKERS: usize = 2;
const BROADCAST_MAX_BUILD_BYTES: usize = 100 * 1024 * 1024;

#[tokio::test]
async fn q39b_aggregates_do_not_share_a_fragment_with_the_inventory_scan() {
    let fragments = fragment(Q39B_SQL).await;
    assert_single_step_aggregates_read_whole_groups(&fragments);
}

#[tokio::test]
async fn q39a_aggregates_do_not_share_a_fragment_with_the_inventory_scan() {
    let fragments = fragment(Q39A_SQL).await;
    assert_single_step_aggregates_read_whole_groups(&fragments);
}

#[tokio::test]
async fn stddev_aggregate_joined_to_a_small_table() {
    let fragments = fragment(
        "SELECT a.inv_warehouse_sk, a.sd, w_warehouse_name \
         FROM (SELECT inv_warehouse_sk, inv_item_sk, stddev_samp(inv_quantity_on_hand) AS sd \
               FROM inventory GROUP BY inv_warehouse_sk, inv_item_sk) a \
         JOIN warehouse ON a.inv_warehouse_sk = w_warehouse_sk",
    )
    .await;
    assert_single_step_aggregates_read_whole_groups(&fragments);
}

#[tokio::test]
async fn stddev_aggregate_joined_to_a_small_table_written_first() {
    let fragments = fragment(
        "SELECT a.inv_warehouse_sk, a.sd, w_warehouse_name \
         FROM warehouse \
         JOIN (SELECT inv_warehouse_sk, inv_item_sk, stddev_samp(inv_quantity_on_hand) AS sd \
               FROM inventory GROUP BY inv_warehouse_sk, inv_item_sk) a \
           ON a.inv_warehouse_sk = w_warehouse_sk",
    )
    .await;
    assert_single_step_aggregates_read_whole_groups(&fragments);
}

#[tokio::test]
async fn count_distinct_aggregate_joined_to_a_small_table() {
    let fragments = fragment(
        "SELECT a.inv_item_sk, a.days, w_warehouse_name \
         FROM (SELECT inv_warehouse_sk, inv_item_sk, count(DISTINCT inv_date_sk) AS days \
               FROM inventory GROUP BY inv_warehouse_sk, inv_item_sk) a \
         JOIN warehouse ON a.inv_warehouse_sk = w_warehouse_sk",
    )
    .await;
    assert_single_step_aggregates_read_whole_groups(&fragments);
}

#[tokio::test]
async fn select_distinct_joined_to_a_small_table() {
    let fragments = fragment(
        "SELECT a.inv_item_sk, w_warehouse_name \
         FROM (SELECT DISTINCT inv_warehouse_sk, inv_item_sk FROM inventory) a \
         JOIN warehouse ON a.inv_warehouse_sk = w_warehouse_sk",
    )
    .await;
    assert_single_step_aggregates_read_whole_groups(&fragments);
}

#[tokio::test]
async fn limit_subquery_joined_to_a_small_table() {
    let fragments = fragment(
        "SELECT a.inv_item_sk, w_warehouse_name \
         FROM (SELECT inv_warehouse_sk, inv_item_sk FROM inventory LIMIT 1000000) a \
         JOIN warehouse ON a.inv_warehouse_sk = w_warehouse_sk",
    )
    .await;
    assert_limits_below_a_join_read_the_whole_input(&fragments);
}

#[tokio::test]
async fn top_n_subquery_joined_to_a_small_table() {
    let fragments = fragment(
        "SELECT a.inv_item_sk, w_warehouse_name \
         FROM (SELECT inv_warehouse_sk, inv_item_sk FROM inventory \
               ORDER BY inv_quantity_on_hand DESC LIMIT 1000000) a \
         JOIN warehouse ON a.inv_warehouse_sk = w_warehouse_sk",
    )
    .await;
    assert_limits_below_a_join_read_the_whole_input(&fragments);
}

#[tokio::test]
async fn offset_subquery_joined_to_a_small_table() {
    let fragments = fragment(
        "SELECT a.inv_item_sk, w_warehouse_name \
         FROM (SELECT inv_warehouse_sk, inv_item_sk FROM inventory \
               ORDER BY inv_item_sk OFFSET 5) a \
         JOIN warehouse ON a.inv_warehouse_sk = w_warehouse_sk",
    )
    .await;
    assert_limits_below_a_join_read_the_whole_input(&fragments);
}

const INVENTORY_SELF_JOIN: &str = "inventory a JOIN inventory b \
     ON a.inv_item_sk = b.inv_item_sk AND a.inv_date_sk = b.inv_date_sk \
    AND a.inv_warehouse_sk = b.inv_warehouse_sk";

#[tokio::test]
async fn select_distinct_over_a_partitioned_join_joined_to_a_small_table() {
    let fragments = fragment(&format!(
        "SELECT x.inv_item_sk, w_warehouse_name \
         FROM (SELECT DISTINCT a.inv_warehouse_sk, a.inv_item_sk FROM {INVENTORY_SELF_JOIN}) x \
         JOIN warehouse ON x.inv_warehouse_sk = w_warehouse_sk"
    ))
    .await;
    assert_single_step_aggregates_read_whole_groups(&fragments);
}

#[tokio::test]
async fn limit_over_a_partitioned_join_joined_to_a_small_table() {
    let fragments = fragment(&format!(
        "SELECT x.inv_item_sk, w_warehouse_name \
         FROM (SELECT a.inv_warehouse_sk, a.inv_item_sk FROM {INVENTORY_SELF_JOIN} \
               LIMIT 1000000) x \
         JOIN warehouse ON x.inv_warehouse_sk = w_warehouse_sk"
    ))
    .await;
    assert_limits_below_a_join_read_the_whole_input(&fragments);
}

/// `SELECT *` keeps the subquery as wide as `inventory`.
const INVENTORY_COLUMNS: &str =
    "a.inv_date_sk, a.inv_item_sk, a.inv_warehouse_sk, a.inv_quantity_on_hand";
const FIRST_ROWS_OF_INVENTORY: &str = "(SELECT * FROM inventory LIMIT 1000)";

#[tokio::test]
async fn limit_as_wide_as_its_table_joined_to_a_small_table() {
    let fragments = fragment(&format!(
        "SELECT {INVENTORY_COLUMNS}, w_warehouse_name \
         FROM {FIRST_ROWS_OF_INVENTORY} a \
         JOIN warehouse ON a.inv_warehouse_sk = w_warehouse_sk"
    ))
    .await;
    assert_limits_below_a_join_read_the_whole_input(&fragments);
}

#[tokio::test]
async fn limit_as_wide_as_its_table_left_joined_to_a_small_table() {
    let fragments = fragment(&format!(
        "SELECT {INVENTORY_COLUMNS}, w_warehouse_name \
         FROM {FIRST_ROWS_OF_INVENTORY} a \
         LEFT JOIN warehouse ON a.inv_warehouse_sk = w_warehouse_sk"
    ))
    .await;
    assert_limits_below_a_join_read_the_whole_input(&fragments);
}

#[tokio::test]
async fn limit_as_wide_as_its_table_left_joined_to_a_larger_table() {
    let fragments = fragment(&format!(
        "SELECT {INVENTORY_COLUMNS}, i_item_id \
         FROM {FIRST_ROWS_OF_INVENTORY} a \
         LEFT JOIN item ON a.inv_item_sk = i_item_sk"
    ))
    .await;
    assert_limits_below_a_join_read_the_whole_input(&fragments);
}

#[tokio::test]
async fn aggregate_as_wide_as_its_table_left_joined_to_a_small_table() {
    let fragments = fragment(
        "SELECT a.inv_warehouse_sk, a.items, a.days, w_warehouse_name \
         FROM (SELECT inv_warehouse_sk, count(DISTINCT inv_item_sk) AS items, \
                      count(DISTINCT inv_date_sk) AS days, max(inv_quantity_on_hand) AS most \
               FROM inventory GROUP BY inv_warehouse_sk) a \
         LEFT JOIN warehouse ON a.inv_warehouse_sk = w_warehouse_sk",
    )
    .await;
    assert_single_step_aggregates_read_whole_groups(&fragments);
}

#[tokio::test]
async fn aggregate_as_wide_as_its_table_left_joined_to_a_larger_table() {
    let fragments = fragment(
        "SELECT a.inv_item_sk, a.days, a.warehouses, a.most, i_item_id \
         FROM (SELECT inv_item_sk, count(DISTINCT inv_date_sk) AS days, \
                      count(DISTINCT inv_warehouse_sk) AS warehouses, \
                      max(inv_quantity_on_hand) AS most \
               FROM inventory GROUP BY inv_item_sk) a \
         LEFT JOIN item ON a.inv_item_sk = i_item_sk",
    )
    .await;
    assert_single_step_aggregates_read_whole_groups(&fragments);
}

#[tokio::test]
async fn stddev_aggregate_joined_to_two_small_tables() {
    let fragments = fragment(
        "SELECT a.inv_warehouse_sk, a.sd, w_warehouse_name, i_item_sk \
         FROM (SELECT inv_warehouse_sk, inv_item_sk, stddev_samp(inv_quantity_on_hand) AS sd \
               FROM inventory GROUP BY inv_warehouse_sk, inv_item_sk) a \
         JOIN warehouse ON a.inv_warehouse_sk = w_warehouse_sk \
         JOIN item ON a.inv_item_sk = i_item_sk",
    )
    .await;
    assert_single_step_aggregates_read_whole_groups(&fragments);
}

/// Rows per warehouse and item, the columns in the aggregate's own order.
const COUNTS_IN_GROUP_ORDER: &str = "(SELECT inv_warehouse_sk, inv_item_sk, count(*) c \
     FROM inventory GROUP BY inv_warehouse_sk, inv_item_sk)";
/// The same groups, listed item first.
const COUNTS_ITEM_FIRST: &str = "(SELECT inv_item_sk, inv_warehouse_sk, count(*) c \
     FROM inventory GROUP BY inv_warehouse_sk, inv_item_sk)";

/// Measured: 480,000 rows where 240,000 are correct.
#[tokio::test]
async fn two_step_aggregate_listed_in_another_order_joined_to_a_small_table() {
    let fragments = fragment(&format!(
        "SELECT count(*), sum(c) FROM {COUNTS_ITEM_FIRST} a \
         JOIN warehouse ON a.inv_warehouse_sk = w_warehouse_sk"
    ))
    .await;
    assert_final_aggregates_merge_every_task_of_their_partial_step(&fragments);
    assert_a_partial_aggregate_reads_the_inventory_scan(&fragments);
}

/// Measured: 480,000 rows where 240,000 are correct.
#[tokio::test]
async fn two_step_aggregate_listing_its_count_first_joined_to_a_small_table() {
    let fragments = fragment(
        "SELECT count(*), sum(c) \
         FROM (SELECT count(*) c, inv_warehouse_sk, inv_item_sk \
               FROM inventory GROUP BY inv_warehouse_sk, inv_item_sk) a \
         JOIN warehouse ON a.inv_warehouse_sk = w_warehouse_sk",
    )
    .await;
    assert_final_aggregates_merge_every_task_of_their_partial_step(&fragments);
    assert_a_partial_aggregate_reads_the_inventory_scan(&fragments);
}

/// Measured: 480,000 rows where 240,000 are correct.
#[tokio::test]
async fn two_step_aggregate_listed_in_another_order_joined_to_a_small_table_written_first() {
    let fragments = fragment(&format!(
        "SELECT count(*), sum(c) FROM warehouse \
         JOIN {COUNTS_ITEM_FIRST} a ON a.inv_warehouse_sk = w_warehouse_sk"
    ))
    .await;
    assert_final_aggregates_merge_every_task_of_their_partial_step(&fragments);
    assert_a_partial_aggregate_reads_the_inventory_scan(&fragments);
}

/// Measured: 7,380 rows where 4,982 are correct.
#[tokio::test]
async fn two_step_aggregate_listed_in_another_order_joined_on_two_keys_to_a_few_rows() {
    let fragments = fragment(
        "SELECT count(*) \
         FROM (SELECT inv_item_sk, inv_date_sk, count(*) c \
               FROM inventory GROUP BY inv_date_sk, inv_item_sk) a \
         JOIN (SELECT * FROM inventory \
               WHERE inv_warehouse_sk = 1 AND inv_quantity_on_hand = 7) b \
           ON a.inv_item_sk = b.inv_item_sk AND a.inv_date_sk = b.inv_date_sk",
    )
    .await;
    assert_final_aggregates_merge_every_task_of_their_partial_step(&fragments);
    assert_a_partial_aggregate_reads_the_inventory_scan(&fragments);
}

/// Measured correct (240,000 rows): the final step runs one task per bucket
/// of the partial step's output.
#[tokio::test]
async fn two_step_aggregate_listed_in_its_own_order_keeps_its_partial_step_next_to_the_scan() {
    let fragments = fragment(&format!(
        "SELECT count(*), sum(c) FROM {COUNTS_IN_GROUP_ORDER} a \
         JOIN warehouse ON a.inv_warehouse_sk = w_warehouse_sk"
    ))
    .await;
    assert_final_aggregates_merge_every_task_of_their_partial_step(&fragments);
    assert_a_partial_aggregate_reads_the_inventory_scan(&fragments);
}

/// Measured correct (240,000 rows).
#[tokio::test]
async fn two_step_aggregate_listed_in_another_order_left_joined_to_a_small_table() {
    let fragments = fragment(&format!(
        "SELECT count(*), sum(c) FROM {COUNTS_ITEM_FIRST} a \
         LEFT JOIN warehouse ON a.inv_warehouse_sk = w_warehouse_sk"
    ))
    .await;
    assert_final_aggregates_merge_every_task_of_their_partial_step(&fragments);
    assert_a_partial_aggregate_reads_the_inventory_scan(&fragments);
}

/// The fold this guards against is worth keeping where it is sound: a join
/// whose probe side is a plain scan still runs inside the scan's fragment,
/// next to the broadcast build's exchange.
#[tokio::test]
async fn a_scan_joined_to_a_small_table_still_joins_inside_the_scan_fragment() {
    let fragments = fragment(
        "SELECT inv_item_sk, inv_quantity_on_hand, w_warehouse_name \
         FROM inventory JOIN warehouse ON inv_warehouse_sk = w_warehouse_sk",
    )
    .await;

    let mut found = false;
    for_each_fragment(&fragments, &mut |f| {
        if scans_table(&f.root, "inventory") && contains_join(&f.root) {
            found = true;
        }
    });
    assert!(
        found,
        "the join should run in the fragment that scans inventory:\n{fragments}"
    );
}

/// A sum splits into a partial and a final step, so its partial step may
/// keep running next to the scan.
#[tokio::test]
async fn a_splittable_aggregate_keeps_its_partial_step_next_to_the_scan() {
    let fragments = fragment(
        "SELECT a.inv_warehouse_sk, a.qty, w_warehouse_name \
         FROM (SELECT inv_warehouse_sk, inv_item_sk, sum(inv_quantity_on_hand) AS qty \
               FROM inventory GROUP BY inv_warehouse_sk, inv_item_sk) a \
         JOIN warehouse ON a.inv_warehouse_sk = w_warehouse_sk",
    )
    .await;
    assert_single_step_aggregates_read_whole_groups(&fragments);

    let mut found = false;
    for_each_fragment(&fragments, &mut |f| {
        if scans_table(&f.root, "inventory") && contains_partial_aggregate(&f.root) {
            found = true;
        }
    });
    assert!(
        found,
        "the partial aggregate should run in the fragment that scans inventory:\n{fragments}"
    );
}

/// q39a's final join runs as one task and returned the correct 563 rows on
/// two workers: its two aggregates run in that same task, with no exchange
/// between them and the join.
#[tokio::test]
async fn q39a_aggregates_stay_in_the_fragment_of_the_join_that_reads_them() {
    let fragments = fragment(Q39A_SQL).await;

    let mut found = false;
    for_each_fragment(&fragments, &mut |f| {
        if single_step_aggregates_below_a_join(&f.root, false) == 2 {
            found = true;
        }
    });
    assert!(
        found,
        "both aggregates should run in the fragment of the join that reads them:\n{}",
        describe(&fragments, 0)
    );
}

/// A `LEFT JOIN` is not folded into the scan fragment and returned the
/// correct 1,000,000 rows on two workers: the limit runs in the join's
/// fragment, with no exchange between them.
#[tokio::test]
async fn a_limit_left_joined_to_a_small_table_stays_in_the_fragment_of_the_join() {
    let fragments = fragment(
        "SELECT a.inv_item_sk, w_warehouse_name \
         FROM (SELECT inv_warehouse_sk, inv_item_sk FROM inventory LIMIT 1000000) a \
         LEFT JOIN warehouse ON a.inv_warehouse_sk = w_warehouse_sk",
    )
    .await;
    assert_limits_below_a_join_read_the_whole_input(&fragments);

    let mut found = false;
    for_each_fragment(&fragments, &mut |f| {
        if limits_below_a_join(&f.root, false) == 1 {
            found = true;
        }
    });
    assert!(
        found,
        "the limit should run in the fragment of the join that reads it:\n{}",
        describe(&fragments, 0)
    );
}

async fn fragment(sql: &str) -> PlanFragment {
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
        .with_broadcast_threshold(Some(BROADCAST_MAX_BUILD_BYTES))
        .with_stats(Some(ctx.catalog_stats.clone()))
        .with_worker_count(WORKERS)
        .fragment(plan)
}

fn assert_single_step_aggregates_read_whole_groups(root: &PlanFragment) {
    let offenders =
        multi_task_fragments_where(root, |f| single_step_aggregate_over_a_slice(&f.root, f));
    assert!(
        offenders.is_empty(),
        "these fragments run several tasks and aggregate, in a single step, the rows of their \
         own scan or join or one bucket of a hash-partitioned input, so each task aggregates \
         only its own slice of every group:\n{}\n\
         whole plan:\n{}",
        offenders.join("\n"),
        describe(root, 0)
    );
}

fn assert_limits_below_a_join_read_the_whole_input(root: &PlanFragment) {
    let offenders =
        multi_task_fragments_where(root, |f| limit_over_a_slice_below_join(&f.root, f, false));
    assert!(
        offenders.is_empty(),
        "these fragments run several tasks and apply a LIMIT / OFFSET to the rows of their own \
         scan or join, or to one bucket of a hash-partitioned input, before joining them, so \
         each task limits only its own slice:\n{}\n\
         whole plan:\n{}",
        offenders.join("\n"),
        describe(root, 0)
    );
}

fn assert_final_aggregates_merge_every_task_of_their_partial_step(root: &PlanFragment) {
    let offenders =
        multi_task_fragments_where(root, |f| final_aggregate_over_a_scan_slice(&f.root));
    assert!(
        offenders.is_empty(),
        "these fragments run several tasks, each over its own share of a table, and finish a \
         two-step aggregate above that scan with no exchange in between, so each task finishes \
         only its own share of every group:\n{}\n\
         whole plan:\n{}",
        offenders.join("\n"),
        describe(root, 0)
    );
}

/// The partial step of an aggregate over `inventory` runs in the fragment
/// that scans the table, directly above the scan.
fn assert_a_partial_aggregate_reads_the_inventory_scan(root: &PlanFragment) {
    let mut found = false;
    for_each_fragment(root, &mut |f| {
        if f.fragment_type == FragmentType::Source
            && partial_aggregate_over_scan(&f.root, "inventory")
        {
            found = true;
        }
    });
    assert!(
        found,
        "the aggregate's partial step should run in the fragment that scans inventory, \
         directly above the scan:\n{}",
        describe(root, 0)
    );
}

/// Fragments that run more than one task: one per worker for a table scan,
/// one per partition for a join of hash-partitioned inputs.
fn multi_task_fragments_where(
    root: &PlanFragment,
    is_offender: impl Fn(&PlanFragment) -> bool,
) -> Vec<String> {
    let mut offenders = Vec::new();
    for_each_fragment(root, &mut |f| {
        let multi_task = matches!(
            f.fragment_type,
            FragmentType::Source | FragmentType::HashPartitioned
        );
        if multi_task && is_offender(f) {
            offenders.push(format!("{f}\n{}", f.root));
        }
    });
    offenders
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

/// True when `plan`, part of `fragment`'s root, holds a single-step aggregate
/// whose input each task sees only a slice of. (No plan hashes such an input
/// on the aggregate's own group keys today; one that did would keep every
/// group in one task and would need this check refined.)
fn single_step_aggregate_over_a_slice(plan: &LogicalPlan, fragment: &PlanFragment) -> bool {
    if let LogicalPlan::Aggregate { input, .. } = plan {
        if reads_a_slice(input, fragment) {
            return true;
        }
    }
    plan_children(plan)
        .into_iter()
        .any(|child| single_step_aggregate_over_a_slice(child, fragment))
}

/// True when a `Limit` (which also carries `OFFSET`) sits below a join of
/// `fragment` and each task sees only a slice of the limit's input.
fn limit_over_a_slice_below_join(
    plan: &LogicalPlan,
    fragment: &PlanFragment,
    below_join: bool,
) -> bool {
    if below_join {
        if let LogicalPlan::Limit { input, .. } = plan {
            if reads_a_slice(input, fragment) {
                return true;
            }
        }
    }
    let below_join = below_join || matches!(plan, LogicalPlan::Join { .. });
    plan_children(plan)
        .into_iter()
        .any(|child| limit_over_a_slice_below_join(child, fragment, below_join))
}

/// True when each task of `fragment` sees only part of the rows `plan`
/// produces: the plan holds a table scan or a join of the fragment itself
/// (the plan never crosses an exchange: `ExchangeNode` is a leaf), or reads an
/// exchange whose producer hash-partitions its output, one bucket per task.
/// A broadcast exchange hands every task the whole input.
fn reads_a_slice(plan: &LogicalPlan, fragment: &PlanFragment) -> bool {
    match plan {
        LogicalPlan::TableScan { .. } | LogicalPlan::Join { .. } => true,
        LogicalPlan::ExchangeNode { stage_id, .. } => {
            fragment.source_fragments.iter().any(|source| {
                source.id == *stage_id
                    && matches!(
                        &source.output_partitioning,
                        PartitioningScheme::Hash { columns, .. } if !columns.is_empty()
                    )
            })
        }
        _ => plan_children(plan)
            .into_iter()
            .any(|child| reads_a_slice(child, fragment)),
    }
}

/// True when `plan`, part of a fragment's root, holds the final step of a
/// two-step aggregate above a table scan of that same fragment (the plan
/// never crosses an exchange: `ExchangeNode` is a leaf).
fn final_aggregate_over_a_scan_slice(plan: &LogicalPlan) -> bool {
    if let LogicalPlan::FinalAggregate { input, .. } = plan {
        if contains_table_scan(input) {
            return true;
        }
    }
    plan_children(plan)
        .into_iter()
        .any(final_aggregate_over_a_scan_slice)
}

fn partial_aggregate_over_scan(plan: &LogicalPlan, table: &str) -> bool {
    if let LogicalPlan::PartialAggregate { input, .. } = plan {
        if scans_table(input, table) && !contains_join(input) {
            return true;
        }
    }
    plan_children(plan)
        .into_iter()
        .any(|child| partial_aggregate_over_scan(child, table))
}

fn contains_table_scan(plan: &LogicalPlan) -> bool {
    matches!(plan, LogicalPlan::TableScan { .. })
        || plan_children(plan).into_iter().any(contains_table_scan)
}

fn single_step_aggregates_below_a_join(plan: &LogicalPlan, below_join: bool) -> usize {
    let here = usize::from(below_join && matches!(plan, LogicalPlan::Aggregate { .. }));
    let below_join = below_join || matches!(plan, LogicalPlan::Join { .. });
    here + plan_children(plan)
        .into_iter()
        .map(|child| single_step_aggregates_below_a_join(child, below_join))
        .sum::<usize>()
}

fn limits_below_a_join(plan: &LogicalPlan, below_join: bool) -> usize {
    let here = usize::from(below_join && matches!(plan, LogicalPlan::Limit { .. }));
    let below_join = below_join || matches!(plan, LogicalPlan::Join { .. });
    here + plan_children(plan)
        .into_iter()
        .map(|child| limits_below_a_join(child, below_join))
        .sum::<usize>()
}

fn scans_table(plan: &LogicalPlan, name: &str) -> bool {
    match plan {
        LogicalPlan::TableScan { table, .. } => table.table == name,
        _ => plan_children(plan)
            .into_iter()
            .any(|child| scans_table(child, name)),
    }
}

fn contains_join(plan: &LogicalPlan) -> bool {
    matches!(plan, LogicalPlan::Join { .. }) || plan_children(plan).into_iter().any(contains_join)
}

fn contains_partial_aggregate(plan: &LogicalPlan) -> bool {
    matches!(plan, LogicalPlan::PartialAggregate { .. })
        || plan_children(plan)
            .into_iter()
            .any(contains_partial_aggregate)
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

/// The four tables q39 reads, with the row counts and distinct counts of the
/// data set the wrong result was measured on.
fn tpcds_catalog() -> CatalogManager {
    let manager = CatalogManager::new("default", "public");
    let catalog = Arc::new(MemoryCatalog::new());
    let schema = Arc::new(MemorySchema::new());

    register(
        &schema,
        "inventory",
        &[
            ("inv_date_sk", DataType::Int64),
            ("inv_item_sk", DataType::Int64),
            ("inv_warehouse_sk", DataType::Int64),
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
        "item",
        &[
            ("i_item_sk", DataType::Int64),
            ("i_item_id", DataType::Utf8),
        ],
        40_000,
        &[("i_item_sk", 40_000)],
    );
    register(
        &schema,
        "warehouse",
        &[
            ("w_warehouse_sk", DataType::Int64),
            ("w_warehouse_name", DataType::Utf8),
        ],
        6,
        &[("w_warehouse_sk", 6), ("w_warehouse_name", 6)],
    );
    register(
        &schema,
        "date_dim",
        &[
            ("d_date_sk", DataType::Int64),
            ("d_year", DataType::Int32),
            ("d_moy", DataType::Int32),
        ],
        73_049,
        &[("d_date_sk", 73_049), ("d_year", 201), ("d_moy", 12)],
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
