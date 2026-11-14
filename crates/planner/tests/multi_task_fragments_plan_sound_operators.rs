//! A fragment that runs several tasks gives each task a slice of its rows:
//! its share of a scan, or one bucket of a hash-partitioned input. Every
//! operator in such a fragment must still produce the rows it would produce
//! over all of its input. A `LIMIT` below the fragment's top, a single-step
//! or final aggregate, a `DISTINCT` or a window whose input is not bucketed on
//! its own keys finishes each slice separately. A join of two sliced inputs
//! finds a match only when both are bucketed on the compared columns, pair by
//! pair. A `RIGHT` / `FULL` join emits the unmatched rows of its preserved side
//! once per task when that side is sent whole to every task. A fragment whose
//! every task emits the same whole input sends those rows once per task.
//!
//! Measured on TPC-DS SF30 with a coordinator and two workers (`warehouse`
//! has 6 rows, `inventory` 31,321,566, of which 5,220,261 in warehouse 1, and
//! 240,000 groups per warehouse and item):
//!
//! - `(SELECT * FROM inventory WHERE inv_warehouse_sk = 1) a RIGHT JOIN
//!   warehouse` returned 5,220,272 rows where 5,220,266 are correct; `FULL
//!   JOIN` the same. The rows per warehouse and item, `RIGHT JOIN warehouse`,
//!   returned 240,006 where 240,000 are correct. Written as `warehouse LEFT
//!   JOIN ..` both returned the correct count.
//! - A per warehouse and item aggregate with `stddev_samp` and
//!   `count(DISTINCT inv_date_sk)`, filtered by `EXISTS` on `warehouse`,
//!   returned its 240,000 groups with day counts summing to 15,520,776 where
//!   31,321,566 is correct; under `NOT EXISTS` it returned 240,000 groups where
//!   none is correct. Listed item first and filtered by `EXISTS` on another
//!   warehouse's `inventory` rows: 15,658,314; under `NOT EXISTS`, 240,000
//!   groups where none is correct.
//! - `(SELECT * FROM inventory LIMIT 1000)` filtered by `EXISTS` on
//!   `warehouse` returned 2,000 rows; left-joined to `inventory` on its three
//!   key columns, 2,000. `warehouse` filtered by `EXISTS` on that subquery
//!   returned two warehouses where the subquery holds one, and the same query
//!   run again failed with "consumer dropped receiver mid-stream on a
//!   must-drain exchange".
//! - TPC-DS q39b returned 54 rows where 21 are correct (#250).
//!
//! The same shapes written as `IN`, as an inner join to `inventory` on the
//! key columns, or with the preserved side written first, returned the
//! correct counts; so did every benchmark query other than q39b.
//!
//! The tests below fragment every combination of a subquery shape and a way
//! of joining it, and check each multi-task fragment operator by operator.
//! The TPC-H and TPC-DS queries other than q39b already plan sound fragments,
//! and keep the plans they have today.

use std::collections::HashMap;
use std::sync::Arc;

use arneb_catalog::{
    CatalogManager, ColumnStatistics, MemoryCatalog, MemorySchema, TableProvider, TableStatistics,
};
use arneb_common::types::{ColumnInfo, DataType};
use arneb_planner::{
    FragmentType, JoinCondition, LogicalOptimizer, LogicalPlan, PartitioningScheme, PlanExpr,
    PlanFragment, PlanFragmenter, QueryPlanner,
};
use arneb_sql_parser::ast::{BinaryOp, JoinType};

const WORKERS: usize = 2;
const BROADCAST_MAX_BUILD_BYTES: usize = 100 * 1024 * 1024;

/// Subqueries over `inventory`. Each exposes `inv_item_sk` and
/// `inv_warehouse_sk`.
const SHAPES: &[(&str, &str)] = &[
    ("T", "(SELECT * FROM inventory WHERE inv_warehouse_sk = 1)"),
    ("Tr", "(SELECT inv_item_sk, inv_date_sk, inv_warehouse_sk, inv_quantity_on_hand FROM inventory WHERE inv_warehouse_sk = 1)"),
    ("Tn", "(SELECT inv_item_sk, inv_warehouse_sk FROM inventory WHERE inv_warehouse_sk = 1)"),
    ("Tc", "(SELECT inv_date_sk, CAST(inv_item_sk + 1 AS INTEGER) AS inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand FROM inventory WHERE inv_warehouse_sk = 1)"),
    ("L", "(SELECT * FROM inventory LIMIT 1000)"),
    ("Lr", "(SELECT inv_item_sk, inv_date_sk, inv_warehouse_sk, inv_quantity_on_hand FROM inventory LIMIT 1000)"),
    ("Ln", "(SELECT inv_item_sk, inv_warehouse_sk FROM inventory LIMIT 1000)"),
    ("Lo", "(SELECT * FROM inventory LIMIT 1000 OFFSET 10)"),
    ("Ls", "(SELECT * FROM inventory ORDER BY inv_quantity_on_hand LIMIT 1000)"),
    ("Lf", "(SELECT * FROM (SELECT * FROM inventory LIMIT 1000) t WHERE inv_quantity_on_hand > 5)"),
    ("D", "(SELECT DISTINCT inv_item_sk, inv_warehouse_sk FROM inventory)"),
    ("Dr", "(SELECT DISTINCT inv_warehouse_sk, inv_item_sk FROM inventory)"),
    ("D4", "(SELECT DISTINCT * FROM inventory)"),
    ("G", "(SELECT inv_warehouse_sk, inv_item_sk FROM inventory GROUP BY inv_warehouse_sk, inv_item_sk)"),
    ("A1", "(SELECT inv_warehouse_sk, inv_item_sk, count(DISTINCT inv_date_sk) d FROM inventory GROUP BY inv_warehouse_sk, inv_item_sk)"),
    ("A1r", "(SELECT inv_item_sk, count(DISTINCT inv_date_sk) d, inv_warehouse_sk FROM inventory GROUP BY inv_warehouse_sk, inv_item_sk)"),
    ("A1w", "(SELECT inv_warehouse_sk, inv_item_sk, stddev_samp(inv_quantity_on_hand) s, count(DISTINCT inv_date_sk) d FROM inventory GROUP BY inv_warehouse_sk, inv_item_sk)"),
    ("A1x", "(SELECT inv_item_sk, inv_warehouse_sk, stddev_samp(inv_quantity_on_hand) s, count(DISTINCT inv_date_sk) d FROM inventory GROUP BY inv_item_sk, inv_warehouse_sk)"),
    ("A2", "(SELECT inv_warehouse_sk, inv_item_sk, count(*) c FROM inventory GROUP BY inv_warehouse_sk, inv_item_sk)"),
    ("A2r", "(SELECT inv_item_sk, inv_warehouse_sk, count(*) c FROM inventory GROUP BY inv_warehouse_sk, inv_item_sk)"),
    ("A2m", "(SELECT inv_item_sk, count(*) c, inv_warehouse_sk FROM inventory GROUP BY inv_warehouse_sk, inv_item_sk)"),
    ("A2w", "(SELECT inv_warehouse_sk, inv_item_sk, count(*) c, sum(inv_quantity_on_hand) s FROM inventory GROUP BY inv_warehouse_sk, inv_item_sk)"),
    ("A2x", "(SELECT inv_item_sk, inv_warehouse_sk, count(*) c, sum(inv_quantity_on_hand) s FROM inventory GROUP BY inv_item_sk, inv_warehouse_sk)"),
    ("A2c", "(SELECT inv_warehouse_sk, inv_item_sk, count(*) + 1 c FROM inventory GROUP BY inv_warehouse_sk, inv_item_sk)"),
    ("A2h", "(SELECT inv_warehouse_sk, inv_item_sk, count(*) c FROM inventory GROUP BY inv_warehouse_sk, inv_item_sk HAVING count(*) > 1)"),
    ("J", "(SELECT i.inv_date_sk, i.inv_item_sk, i.inv_warehouse_sk, d_year FROM inventory i JOIN date_dim ON i.inv_date_sk = d_date_sk)"),
    ("JL", "(SELECT i.inv_date_sk, i.inv_item_sk, i.inv_warehouse_sk, d_year FROM inventory i JOIN date_dim ON i.inv_date_sk = d_date_sk LIMIT 1000)"),
    ("JA", "(SELECT i.inv_warehouse_sk, i.inv_item_sk, count(*) c FROM inventory i JOIN date_dim ON i.inv_date_sk = d_date_sk GROUP BY i.inv_warehouse_sk, i.inv_item_sk)"),
    ("JA1", "(SELECT i.inv_warehouse_sk, i.inv_item_sk, count(DISTINCT d_year) c FROM inventory i JOIN date_dim ON i.inv_date_sk = d_date_sk GROUP BY i.inv_warehouse_sk, i.inv_item_sk)"),
    ("W", "(SELECT inv_item_sk, inv_warehouse_sk, row_number() OVER (PARTITION BY inv_item_sk ORDER BY inv_date_sk) rn FROM inventory)"),
    ("U", "(SELECT inv_item_sk, inv_warehouse_sk FROM inventory WHERE inv_warehouse_sk = 1 UNION ALL SELECT inv_item_sk, inv_warehouse_sk FROM inventory WHERE inv_warehouse_sk = 2)"),
    ("UD", "(SELECT inv_item_sk, inv_warehouse_sk FROM inventory WHERE inv_warehouse_sk = 1 UNION SELECT inv_item_sk, inv_warehouse_sk FROM inventory WHERE inv_warehouse_sk = 2)"),
];

/// Ways of using a subquery `{S}`. `warehouse` (6 rows) is broadcast;
/// `item` and the other warehouse's `inventory` rows are not.
const PARTNERS: &[(&str, &str)] = &[
    ("alone", "SELECT count(*) FROM {S} a"),
    ("JOIN wh", "SELECT count(*) FROM {S} a JOIN warehouse ON a.inv_warehouse_sk = w_warehouse_sk"),
    ("wh JOIN", "SELECT count(*) FROM warehouse JOIN {S} a ON a.inv_warehouse_sk = w_warehouse_sk"),
    ("LEFT wh", "SELECT count(*) FROM {S} a LEFT JOIN warehouse ON a.inv_warehouse_sk = w_warehouse_sk"),
    ("wh LEFT", "SELECT count(*) FROM warehouse LEFT JOIN {S} a ON a.inv_warehouse_sk = w_warehouse_sk"),
    ("RIGHT wh", "SELECT count(*) FROM {S} a RIGHT JOIN warehouse ON a.inv_warehouse_sk = w_warehouse_sk"),
    ("FULL wh", "SELECT count(*) FROM {S} a FULL JOIN warehouse ON a.inv_warehouse_sk = w_warehouse_sk"),
    ("JOIN item", "SELECT count(*) FROM {S} a JOIN item ON a.inv_item_sk = i_item_sk"),
    ("LEFT item", "SELECT count(*) FROM {S} a LEFT JOIN item ON a.inv_item_sk = i_item_sk"),
    ("JOIN inv2", "SELECT count(*) FROM {S} a JOIN (SELECT * FROM inventory WHERE inv_warehouse_sk = 2) b ON a.inv_item_sk = b.inv_item_sk"),
    ("inv2 JOIN", "SELECT count(*) FROM (SELECT * FROM inventory WHERE inv_warehouse_sk = 2) b JOIN {S} a ON a.inv_item_sk = b.inv_item_sk"),
    ("LEFT inv2", "SELECT count(*) FROM {S} a LEFT JOIN (SELECT * FROM inventory WHERE inv_warehouse_sk = 2) b ON a.inv_item_sk = b.inv_item_sk"),
    ("JOIN2 inv2", "SELECT count(*) FROM {S} a JOIN (SELECT * FROM inventory WHERE inv_warehouse_sk = 2) b ON a.inv_item_sk = b.inv_item_sk AND a.inv_warehouse_sk = b.inv_warehouse_sk"),
    ("self JOIN", "SELECT count(*) FROM {S} a JOIN {S} b ON a.inv_item_sk = b.inv_item_sk AND a.inv_warehouse_sk = b.inv_warehouse_sk"),
    ("EXISTS wh", "SELECT count(*) FROM {S} a WHERE EXISTS (SELECT 1 FROM warehouse WHERE w_warehouse_sk = a.inv_warehouse_sk)"),
    ("NOT EX wh", "SELECT count(*) FROM {S} a WHERE NOT EXISTS (SELECT 1 FROM warehouse WHERE w_warehouse_sk = a.inv_warehouse_sk)"),
    ("EXISTS inv2", "SELECT count(*) FROM {S} a WHERE EXISTS (SELECT 1 FROM inventory b WHERE b.inv_warehouse_sk = 2 AND b.inv_item_sk = a.inv_item_sk)"),
    ("NOT EX inv2", "SELECT count(*) FROM {S} a WHERE NOT EXISTS (SELECT 1 FROM inventory b WHERE b.inv_warehouse_sk = 2 AND b.inv_item_sk = a.inv_item_sk)"),
    ("IN wh", "SELECT count(*) FROM {S} a WHERE a.inv_warehouse_sk IN (SELECT w_warehouse_sk FROM warehouse)"),
    ("item IN S", "SELECT count(*) FROM item WHERE i_item_sk IN (SELECT inv_item_sk FROM {S} a)"),
    ("item NOT IN S", "SELECT count(*) FROM item WHERE i_item_sk NOT IN (SELECT inv_item_sk FROM {S} a)"),
    ("inv2 IN S", "SELECT count(*) FROM inventory b WHERE b.inv_warehouse_sk = 2 AND b.inv_item_sk IN (SELECT inv_item_sk FROM {S} a)"),
    ("inv2 EXISTS S", "SELECT count(*) FROM inventory b WHERE b.inv_warehouse_sk = 2 AND EXISTS (SELECT 1 FROM {S} a WHERE a.inv_item_sk = b.inv_item_sk)"),
    ("wh EXISTS S", "SELECT count(*) FROM warehouse WHERE EXISTS (SELECT 1 FROM {S} a WHERE a.inv_warehouse_sk = w_warehouse_sk)"),
    ("GROUP above", "SELECT count(*) FROM (SELECT a.inv_item_sk, count(*) n FROM {S} a GROUP BY a.inv_item_sk) g"),
];

/// `fnv1a(describe(plan))` of each benchmark query before #250 was fixed.
const PLAN_DIGESTS: &[(&str, u64)] = &[
    ("tpch q01", 0x5204f7220f3880b6),
    ("tpch q02", 0x5501fbb9f99fd3e2),
    ("tpch q03", 0x05022da481389758),
    ("tpch q04", 0xac5da7cf5db23be9),
    ("tpch q05", 0x9895a1c6348332cc),
    ("tpch q06", 0xe916288c2429f3e4),
    ("tpch q07", 0x1422ecd679a64957),
    ("tpch q08", 0xb71a9f299e3e06cd),
    ("tpch q09", 0x4a706c20e43e6f93),
    ("tpch q10", 0xa73a908e5f0a8692),
    ("tpch q11", 0x02d6b4896b5d3c69),
    ("tpch q12", 0x3d02a0feebfaa54f),
    ("tpch q13", 0x5ec1e32b0331011b),
    ("tpch q14", 0xf0ffb3b52f417d76),
    ("tpch q15", 0x84e4c162bd72c7f8),
    ("tpch q16", 0xe4849054562533a5),
    ("tpch q17", 0xa7eaecacd529a43f),
    ("tpch q18", 0x75079bbe2aee70f4),
    ("tpch q19", 0xc6249502d79aafb9),
    ("tpch q20", 0xade9e1a2b616c27a),
    ("tpch q21", 0x3e7cf6f715546ef3),
    ("tpch q22", 0x49a543829f917943),
    ("tpcds q01", 0x0e7fdb185f91ad20),
    ("tpcds q02", 0x3f9b3725d92252e2),
    ("tpcds q03", 0x88262fdc004a978e),
    ("tpcds q04", 0xdd87d0440b851b40),
    ("tpcds q05", 0xf4ef6b6f14f4ba95),
    ("tpcds q06", 0x53c518e3af5f2c67),
    ("tpcds q07", 0xb652436c0350a9d2),
    ("tpcds q08", 0xeb31045d15550bdf),
    ("tpcds q09", 0x4cd7cf9e4f29a52d),
    ("tpcds q10", 0xfc93ccbdaf8b256a),
    ("tpcds q11", 0xd3b5eb1da65c1b91),
    ("tpcds q12", 0xcc135989abcd5a3c),
    ("tpcds q13", 0xae7e528fc581f0ce),
    ("tpcds q14a", 0xa8413804b7e3468c),
    ("tpcds q14b", 0x815b2a85da1fe777),
    ("tpcds q15", 0x5debb6e1d9fc8997),
    ("tpcds q16", 0x3bbb7d58d942a5dc),
    ("tpcds q17", 0xfd7eeb371b193416),
    ("tpcds q18", 0x10edbae3b9fc6735),
    ("tpcds q19", 0x2fec227cc8b09710),
    ("tpcds q20", 0x13cd815ffac55bca),
    ("tpcds q21", 0x3ad391fd718fccaf),
    ("tpcds q22", 0xa85a8a3e12219ba6),
    ("tpcds q23a", 0x049d338343f5dd2f),
    ("tpcds q23b", 0xacaad6591c6f5977),
    ("tpcds q24a", 0xb34bc8385d7fddc1),
    ("tpcds q24b", 0x9d36f5d947af75ba),
    ("tpcds q25", 0x277e0d9f3ca4e736),
    ("tpcds q26", 0xaad0f600084906ec),
    ("tpcds q27", 0xcccfab7390a0fd70),
    ("tpcds q28", 0xd504226fd2ea4763),
    ("tpcds q29", 0x53065cc2cf9b1a81),
    ("tpcds q30", 0x839d161e5bdc1733),
    ("tpcds q31", 0x5a53fac9b59841a5),
    ("tpcds q32", 0x0fd0ec5b9d5e079b),
    ("tpcds q33", 0xd6994205de63234e),
    ("tpcds q34", 0x429e893ba6992145),
    ("tpcds q35", 0x74e4f82e1ec93762),
    ("tpcds q36", 0xe6505300f9d07689),
    ("tpcds q37", 0x246c72555380613e),
    ("tpcds q38", 0x20b5451836f98e01),
    ("tpcds q39a", 0x84a2a6058747ae59),
    ("tpcds q40", 0x136ea3bde77d0b7e),
    ("tpcds q41", 0xf0e5b200b0229469),
    ("tpcds q42", 0x0237cca03f2f2bf2),
    ("tpcds q43", 0xd242edb432ffdfb4),
    ("tpcds q44", 0x4886b7db71811969),
    ("tpcds q45", 0xebab00d2eb9b2d8b),
    ("tpcds q46", 0xc98388f6bc31ce83),
    ("tpcds q47", 0xeadabbdf10bc1d41),
    ("tpcds q48", 0x441fd0cbcd52f47a),
    ("tpcds q49", 0x457f2bd565b16137),
    ("tpcds q50", 0xa24b6e9062318d90),
    ("tpcds q51", 0x967bcf1832525ac1),
    ("tpcds q52", 0xf79f870ca1a69360),
    ("tpcds q53", 0xff27588fdd68deb3),
    ("tpcds q54", 0xc71fe790ac1931e5),
    ("tpcds q55", 0xaedae6b47d202c3c),
    ("tpcds q56", 0x6bdafc07907384a3),
    ("tpcds q57", 0xe5d95be3a4b941d4),
    ("tpcds q58", 0x0373b88c0edabb24),
    ("tpcds q59", 0x3779ffe7a4cade86),
    ("tpcds q60", 0x39ebd5415f513096),
    ("tpcds q61", 0x9444019b0e8b87c6),
    ("tpcds q62", 0x2493e533c740979b),
    ("tpcds q63", 0xaff1e06cacb0d447),
    ("tpcds q64", 0x43038aa6cf4a61c0),
    ("tpcds q65", 0xd16267f92a880a2e),
    ("tpcds q66", 0xc352da13a4af8272),
    ("tpcds q67", 0xb4e914ce357fb5df),
    ("tpcds q68", 0xff33c8646da58f38),
    ("tpcds q69", 0x83ccf458c1ecc677),
    ("tpcds q70", 0x9f8c0e6eda48b4f7),
    ("tpcds q71", 0xf083ba0aa2cafc37),
    ("tpcds q72", 0x9dc33c3c63180cdf),
    ("tpcds q73", 0xf04d2ad1984a1376),
    ("tpcds q74", 0x10759a86e56cbb2b),
    ("tpcds q75", 0xe4899a4f29daf0ad),
    ("tpcds q76", 0xac9ca60e2f3669dc),
    ("tpcds q77", 0xa2ef74874e6f232a),
    ("tpcds q78", 0x9a3228396a809c2e),
    ("tpcds q79", 0x7a35f373e8a05341),
    ("tpcds q80", 0x7d3a6f7195372e80),
    ("tpcds q81", 0x0c7b7275545703a2),
    ("tpcds q82", 0x74a529d67acf22a4),
    ("tpcds q83", 0x217b0ad91c876197),
    ("tpcds q84", 0x46102a1a2c9d4edb),
    ("tpcds q85", 0xbb577cbb2dd5996f),
    ("tpcds q86", 0xe4103d127eac64b6),
    ("tpcds q87", 0x7228500c5c60e90d),
    ("tpcds q88", 0x5ce6964dc59d2d5c),
    ("tpcds q89", 0x87c55312daab1cee),
    ("tpcds q90", 0x4409927bc14a00c8),
    ("tpcds q91", 0xa6e0b876c197b4b2),
    ("tpcds q92", 0xf2859591aa14785c),
    ("tpcds q93", 0x4f6b5c1eff5b4a6f),
    ("tpcds q94", 0xe80d6b36b135c87a),
    ("tpcds q95", 0x1b9fb100df4de3cd),
    ("tpcds q96", 0x085496f5c5c3b965),
    ("tpcds q97", 0x161a313420be33f6),
    ("tpcds q98", 0x289359764e6d3211),
    ("tpcds q99", 0x272981664c905c60),
];

#[tokio::test]
async fn subqueries_reading_a_table_plan_sound_fragments() {
    assert_shapes_plan_sound_fragments(&["T", "Tr", "Tn", "Tc"]).await;
}

#[tokio::test]
async fn limit_subqueries_plan_sound_fragments() {
    assert_shapes_plan_sound_fragments(&["L", "Lr", "Ln", "Lo", "Ls", "Lf"]).await;
}

#[tokio::test]
async fn distinct_subqueries_plan_sound_fragments() {
    assert_shapes_plan_sound_fragments(&["D", "Dr", "D4", "G"]).await;
}

#[tokio::test]
async fn single_step_aggregate_subqueries_plan_sound_fragments() {
    assert_shapes_plan_sound_fragments(&["A1", "A1r", "A1w", "A1x"]).await;
}

#[tokio::test]
async fn two_step_aggregate_subqueries_plan_sound_fragments() {
    assert_shapes_plan_sound_fragments(&["A2", "A2r", "A2m", "A2w", "A2x", "A2c", "A2h"]).await;
}

#[tokio::test]
async fn subqueries_over_a_join_plan_sound_fragments() {
    assert_shapes_plan_sound_fragments(&["J", "JL", "JA", "JA1"]).await;
}

#[tokio::test]
async fn window_and_union_subqueries_plan_sound_fragments() {
    assert_shapes_plan_sound_fragments(&["W", "U", "UD"]).await;
}

#[tokio::test]
async fn every_subquery_shape_is_checked() {
    let checked = [
        "T", "Tr", "Tn", "Tc", "L", "Lr", "Ln", "Lo", "Ls", "Lf", "D", "Dr", "D4", "G", "A1",
        "A1r", "A1w", "A1x", "A2", "A2r", "A2m", "A2w", "A2x", "A2c", "A2h", "J", "JL", "JA",
        "JA1", "W", "U", "UD",
    ];
    let names: Vec<&str> = SHAPES.iter().map(|(name, _)| *name).collect();
    assert_eq!(names, checked);
}

#[test]
fn benchmark_queries_plan_sound_fragments() {
    let mut offenders = Vec::new();
    for (key, fragment) in benchmark_plans() {
        let found = unsound(&fragment);
        if !found.is_empty() {
            offenders.push(format!(
                "{key}: {}\n{}",
                found.join("; "),
                describe(&fragment, 0)
            ));
        }
    }
    assert!(
        offenders.is_empty(),
        "{} benchmark queries plan unsound fragments:\n{}",
        offenders.len(),
        offenders.join("\n")
    );
}

/// Every benchmark query except q39b planned sound fragments before #250 was
/// fixed; their plans stay as they were. A digest of each plan's fragment
/// tree, as `describe` prints it.
#[test]
fn benchmark_queries_with_sound_plans_keep_their_plans() {
    let expected: HashMap<&str, u64> = PLAN_DIGESTS.iter().copied().collect();
    let mut changed = Vec::new();
    let mut seen = 0;
    for (key, fragment) in benchmark_plans() {
        let Some(digest) = expected.get(key.as_str()) else {
            continue;
        };
        seen += 1;
        let text = describe(&fragment, 0);
        if fnv1a(&text) != *digest {
            changed.push(format!("{key}\n{text}"));
        }
    }
    assert_eq!(
        seen,
        PLAN_DIGESTS.len(),
        "a benchmark query file is missing"
    );
    assert!(
        changed.is_empty(),
        "{} benchmark plans changed:\n{}",
        changed.len(),
        changed.join("\n")
    );
}

async fn assert_shapes_plan_sound_fragments(names: &[&str]) {
    let catalog = tpcds_catalog();
    let mut offenders = Vec::new();
    for name in names {
        let (_, subquery) = SHAPES
            .iter()
            .find(|(shape, _)| shape == name)
            .unwrap_or_else(|| panic!("no shape {name}"));
        for (partner, template) in PARTNERS {
            let sql = template.replace("{S}", subquery);
            let fragment = fragment_in(&catalog, &sql).await;
            let found = unsound(&fragment);
            if !found.is_empty() {
                offenders.push(format!(
                    "{name} | {partner}: {}\n{sql}\n{}",
                    found.join("; "),
                    describe(&fragment, 0)
                ));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "{} of {} queries plan unsound fragments:\n{}",
        offenders.len(),
        names.len() * PARTNERS.len(),
        offenders.join("\n")
    );
}

async fn fragment_in(catalog: &CatalogManager, sql: &str) -> PlanFragment {
    let stmt = arneb_sql_parser::parse(sql).expect("parse");
    let planner = QueryPlanner::new(catalog);
    let (plan, ctx) = planner
        .plan_statement_with_context(&stmt)
        .await
        .unwrap_or_else(|e| panic!("plan {sql}: {e}"));
    let plan = LogicalOptimizer::default_rules()
        .optimize(plan)
        .expect("optimize");
    PlanFragmenter::new()
        .with_broadcast_threshold(Some(BROADCAST_MAX_BUILD_BYTES))
        .with_stats(Some(ctx.catalog_stats.clone()))
        .with_worker_count(WORKERS)
        .fragment(plan)
}

/// Every TPC-H and TPC-DS query of `benchmarks/`, keyed `"tpch q01"`.
/// Planning the larger TPC-DS queries recurses deeper than a test thread's
/// default stack, so they are planned on a thread of their own.
fn benchmark_plans() -> Vec<(String, PlanFragment)> {
    std::thread::Builder::new()
        .stack_size(256 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .build()
                .expect("runtime")
                .block_on(plan_benchmark_queries())
        })
        .expect("planning thread")
        .join()
        .expect("planning benchmark queries")
}

async fn plan_benchmark_queries() -> Vec<(String, PlanFragment)> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../benchmarks");
    let mut plans = Vec::new();
    for which in ["tpch", "tpcds"] {
        let catalog = benchmark_catalog(which);
        let mut files: Vec<_> = std::fs::read_dir(root.join(which).join("queries"))
            .expect("benchmark queries")
            .map(|entry| entry.expect("entry").path())
            .filter(|path| path.extension().is_some_and(|ext| ext == "sql"))
            .collect();
        files.sort();
        for path in files {
            let name = path
                .file_stem()
                .expect("stem")
                .to_string_lossy()
                .to_string();
            let text = std::fs::read_to_string(&path).expect("query file");
            let sql = text.trim().trim_end_matches(';');
            plans.push((format!("{which} {name}"), fragment_in(&catalog, sql).await));
        }
    }
    plans
}

fn fnv1a(text: &str) -> u64 {
    text.bytes().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3)
    })
}

// ---------------------------------------------------------------------------
// The check: what each task of a multi-task fragment sees.
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
enum Rows {
    /// Every task sees all of these rows.
    Whole,
    /// Each task sees a slice. `Some(columns)`: the slices are hash buckets
    /// of exactly these output columns, in this order.
    Sliced(Option<Vec<usize>>),
}

/// Offences of every fragment that runs more than one task, each prefixed
/// with the fragment id.
fn unsound(root: &PlanFragment) -> Vec<String> {
    let mut all = Vec::new();
    for_each_fragment(root, &mut |fragment| {
        let many_tasks = match (&fragment.fragment_type, &fragment.output_partitioning) {
            (FragmentType::Source, _) => true,
            (
                FragmentType::HashPartitioned,
                PartitioningScheme::Hash {
                    partition_count, ..
                },
            ) => *partition_count > 1,
            (FragmentType::HashPartitioned, PartitioningScheme::Broadcast) => true,
            _ => false,
        };
        if !many_tasks {
            return;
        }
        let mut found = Vec::new();
        if matches!(
            rows(&fragment.root, fragment, true, &mut found),
            Rows::Whole
        ) {
            found.push("every task emits the same whole input".to_string());
        }
        all.extend(found.into_iter().map(|o| format!("[{}] {o}", fragment.id)));
    });
    all
}

fn rows(plan: &LogicalPlan, fragment: &PlanFragment, top: bool, found: &mut Vec<String>) -> Rows {
    use LogicalPlan as L;
    match plan {
        L::TableScan { .. } => Rows::Sliced(None),
        L::ExchangeNode { stage_id, .. } => {
            let source = fragment.source_fragments.iter().find(|s| s.id == *stage_id);
            match source.map(|s| &s.output_partitioning) {
                Some(PartitioningScheme::Broadcast) => Rows::Whole,
                Some(PartitioningScheme::Hash { columns, .. }) if !columns.is_empty() => {
                    Rows::Sliced(Some(columns.clone()))
                }
                Some(_) => Rows::Sliced(None),
                None => {
                    found.push("exchange without a source fragment".to_string());
                    Rows::Sliced(None)
                }
            }
        }
        L::Filter { input, .. } => rows(input, fragment, false, found),
        L::Sort { input, .. } => rows(input, fragment, top, found),
        L::Projection { input, exprs, .. } => match rows(input, fragment, top, found) {
            Rows::Sliced(Some(columns)) => Rows::Sliced(positions_in(&columns, exprs)),
            other => other,
        },
        L::Limit { input, offset, .. } => {
            let input_rows = rows(input, fragment, top, found);
            if matches!(input_rows, Rows::Sliced(_)) && (!top || offset.is_some()) {
                found.push(format!("limit over a slice: {}", first_line(plan)));
            }
            input_rows
        }
        L::PartialAggregate {
            input, group_by, ..
        } => match rows(input, fragment, false, found) {
            Rows::Sliced(Some(columns)) => Rows::Sliced(positions_in(&columns, group_by)),
            other => other,
        },
        L::Aggregate {
            input, group_by, ..
        }
        | L::FinalAggregate {
            input, group_by, ..
        } => {
            let input_rows = rows(input, fragment, false, found);
            let columns = match &input_rows {
                Rows::Whole => return Rows::Whole,
                Rows::Sliced(Some(columns)) if !columns.is_empty() => {
                    positions_in(columns, group_by)
                }
                Rows::Sliced(_) => None,
            };
            if columns.is_none() {
                found.push(format!(
                    "aggregate finishes groups over {input_rows:?}: {}",
                    first_line(plan)
                ));
            }
            Rows::Sliced(columns)
        }
        L::Distinct { input } => match rows(input, fragment, false, found) {
            Rows::Whole => Rows::Whole,
            Rows::Sliced(Some(columns)) if !columns.is_empty() => Rows::Sliced(Some(columns)),
            other => {
                found.push(format!("distinct over {other:?}"));
                Rows::Sliced(None)
            }
        },
        L::Window { input, functions } => {
            let input_rows = rows(input, fragment, false, found);
            let bucketed = match &input_rows {
                Rows::Whole => return Rows::Whole,
                Rows::Sliced(Some(columns)) if !columns.is_empty() => functions.iter().all(|w| {
                    columns
                        .iter()
                        .all(|c| w.partition_by.iter().any(|e| column(e) == Some(*c)))
                }),
                Rows::Sliced(None) => false,
                Rows::Sliced(Some(_)) => false,
            };
            if !bucketed {
                found.push(format!("window over {input_rows:?}"));
            }
            input_rows
        }
        L::Join {
            left,
            right,
            join_type,
            condition,
            ..
        } => {
            let left_rows = rows(left, fragment, false, found);
            let right_rows = rows(right, fragment, false, found);
            let left_width = left.schema().len();
            let mut pairs = Vec::new();
            if let JoinCondition::On(expr) = condition {
                key_pairs(expr, left_width, &mut pairs);
            }
            match (left_rows, right_rows) {
                (Rows::Whole, Rows::Whole) => Rows::Whole,
                (Rows::Sliced(columns), Rows::Whole)
                    if matches!(
                        join_type,
                        JoinType::Inner | JoinType::Left | JoinType::Cross
                    ) =>
                {
                    Rows::Sliced(columns)
                }
                (Rows::Whole, Rows::Sliced(columns))
                    if matches!(
                        join_type,
                        JoinType::Inner | JoinType::Right | JoinType::Cross
                    ) =>
                {
                    Rows::Sliced(columns.map(|c| c.iter().map(|i| i + left_width).collect()))
                }
                (Rows::Sliced(Some(l)), Rows::Sliced(Some(r))) if paired(&l, &r, &pairs) => {
                    Rows::Sliced(Some(l))
                }
                (l, r) => {
                    found.push(format!(
                        "{join_type:?} join of {l:?} and {r:?} comparing {pairs:?}: {}",
                        first_line(plan)
                    ));
                    Rows::Sliced(None)
                }
            }
        }
        L::SemiJoin {
            left,
            right,
            left_key,
            right_key,
            ..
        }
        | L::AntiJoin {
            left,
            right,
            left_key,
            right_key,
            ..
        } => {
            let left_rows = rows(left, fragment, false, found);
            let right_rows = rows(right, fragment, false, found);
            let pairs: Vec<(usize, usize)> = match (column(left_key), column(right_key)) {
                (Some(l), Some(r)) => vec![(l, r)],
                _ => vec![],
            };
            match (left_rows, right_rows) {
                (Rows::Whole, Rows::Whole) => Rows::Whole,
                (Rows::Sliced(columns), Rows::Whole) => Rows::Sliced(columns),
                (Rows::Sliced(Some(l)), Rows::Sliced(Some(r))) if paired(&l, &r, &pairs) => {
                    Rows::Sliced(Some(l))
                }
                (l, r) => {
                    found.push(format!(
                        "semi/anti join of {l:?} and {r:?} comparing {pairs:?}: {}",
                        first_line(plan)
                    ));
                    Rows::Sliced(None)
                }
            }
        }
        L::UnionAll { inputs } => {
            let all: Vec<Rows> = inputs
                .iter()
                .map(|input| rows(input, fragment, false, found))
                .collect();
            if all.iter().all(|r| matches!(r, Rows::Whole)) {
                return Rows::Whole;
            }
            if all.iter().any(|r| matches!(r, Rows::Whole)) {
                found.push("union all of whole and sliced inputs".to_string());
            }
            Rows::Sliced(None)
        }
        other => {
            found.push(format!("unchecked operator: {}", first_line(other)));
            Rows::Sliced(None)
        }
    }
}

/// Where each of `columns` appears, as a bare column, among `exprs`.
fn positions_in(columns: &[usize], exprs: &[PlanExpr]) -> Option<Vec<usize>> {
    columns
        .iter()
        .map(|c| exprs.iter().position(|e| column(e) == Some(*c)))
        .collect()
}

fn column(expr: &PlanExpr) -> Option<usize> {
    match expr {
        PlanExpr::Column { index, .. } => Some(*index),
        _ => None,
    }
}

/// Column pairs `(left, right)` an `ON` condition compares for equality,
/// right columns counted from the right input's first column.
fn key_pairs(expr: &PlanExpr, left_width: usize, pairs: &mut Vec<(usize, usize)>) {
    match expr {
        PlanExpr::BinaryOp {
            left,
            op: BinaryOp::And,
            right,
            ..
        } => {
            key_pairs(left, left_width, pairs);
            key_pairs(right, left_width, pairs);
        }
        PlanExpr::BinaryOp {
            left,
            op: BinaryOp::Eq,
            right,
            ..
        } => {
            if let (Some(a), Some(b)) = (column(left), column(right)) {
                let (l, r) = (a.min(b), a.max(b));
                if l < left_width && r >= left_width {
                    pairs.push((l, r - left_width));
                }
            }
        }
        _ => {}
    }
}

fn paired(left: &[usize], right: &[usize], pairs: &[(usize, usize)]) -> bool {
    !left.is_empty()
        && left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|(l, r)| pairs.contains(&(*l, *r)))
}

fn first_line(plan: &LogicalPlan) -> String {
    plan.to_string()
        .lines()
        .next()
        .unwrap_or("")
        .trim()
        .to_string()
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

// ---------------------------------------------------------------------------
// Catalogs.
// ---------------------------------------------------------------------------

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

/// Every TPC-H or TPC-DS table, with the row counts of the SF30 data set the
/// results above were measured on.
fn benchmark_catalog(which: &str) -> CatalogManager {
    let manager = CatalogManager::new("default", "public");
    let catalog = Arc::new(MemoryCatalog::new());
    let schema = Arc::new(MemorySchema::new());
    if which == "tpcds" {
        register(
            &schema,
            "call_center",
            &[
                ("cc_call_center_sk", DataType::Int32),
                ("cc_call_center_id", DataType::Utf8),
                ("cc_rec_start_date", DataType::Date32),
                ("cc_rec_end_date", DataType::Date32),
                ("cc_closed_date_sk", DataType::Int32),
                ("cc_open_date_sk", DataType::Int32),
                ("cc_name", DataType::Utf8),
                ("cc_class", DataType::Utf8),
                ("cc_employees", DataType::Int32),
                ("cc_sq_ft", DataType::Int32),
                ("cc_hours", DataType::Utf8),
                ("cc_manager", DataType::Utf8),
                ("cc_mkt_id", DataType::Int32),
                ("cc_mkt_class", DataType::Utf8),
                ("cc_mkt_desc", DataType::Utf8),
                ("cc_market_manager", DataType::Utf8),
                ("cc_division", DataType::Int32),
                ("cc_division_name", DataType::Utf8),
                ("cc_company", DataType::Int32),
                ("cc_company_name", DataType::Utf8),
                ("cc_street_number", DataType::Utf8),
                ("cc_street_name", DataType::Utf8),
                ("cc_street_type", DataType::Utf8),
                ("cc_suite_number", DataType::Utf8),
                ("cc_city", DataType::Utf8),
                ("cc_county", DataType::Utf8),
                ("cc_state", DataType::Utf8),
                ("cc_zip", DataType::Utf8),
                ("cc_country", DataType::Utf8),
                (
                    "cc_gmt_offset",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "cc_tax_percentage",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
            ],
            6,
            &[("cc_call_center_sk", 6)],
        );
    }
    if which == "tpcds" {
        register(
            &schema,
            "catalog_page",
            &[
                ("cp_catalog_page_sk", DataType::Int32),
                ("cp_catalog_page_id", DataType::Utf8),
                ("cp_start_date_sk", DataType::Int32),
                ("cp_end_date_sk", DataType::Int32),
                ("cp_department", DataType::Utf8),
                ("cp_catalog_number", DataType::Int32),
                ("cp_catalog_page_number", DataType::Int32),
                ("cp_description", DataType::Utf8),
                ("cp_type", DataType::Utf8),
            ],
            11718,
            &[("cp_catalog_page_sk", 11718)],
        );
    }
    if which == "tpcds" {
        register(
            &schema,
            "catalog_returns",
            &[
                ("cr_returned_date_sk", DataType::Int32),
                ("cr_returned_time_sk", DataType::Int32),
                ("cr_item_sk", DataType::Int32),
                ("cr_refunded_customer_sk", DataType::Int32),
                ("cr_refunded_cdemo_sk", DataType::Int32),
                ("cr_refunded_hdemo_sk", DataType::Int32),
                ("cr_refunded_addr_sk", DataType::Int32),
                ("cr_returning_customer_sk", DataType::Int32),
                ("cr_returning_cdemo_sk", DataType::Int32),
                ("cr_returning_hdemo_sk", DataType::Int32),
                ("cr_returning_addr_sk", DataType::Int32),
                ("cr_call_center_sk", DataType::Int32),
                ("cr_catalog_page_sk", DataType::Int32),
                ("cr_ship_mode_sk", DataType::Int32),
                ("cr_warehouse_sk", DataType::Int32),
                ("cr_reason_sk", DataType::Int32),
                ("cr_order_number", DataType::Int32),
                ("cr_return_quantity", DataType::Int32),
                (
                    "cr_return_amount",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "cr_return_tax",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "cr_return_amt_inc_tax",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "cr_fee",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "cr_return_ship_cost",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "cr_refunded_cash",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "cr_reversed_charge",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "cr_store_credit",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "cr_net_loss",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
            ],
            4322293,
            &[],
        );
    }
    if which == "tpcds" {
        register(
            &schema,
            "catalog_sales",
            &[
                ("cs_sold_date_sk", DataType::Int32),
                ("cs_sold_time_sk", DataType::Int32),
                ("cs_ship_date_sk", DataType::Int32),
                ("cs_bill_customer_sk", DataType::Int32),
                ("cs_bill_cdemo_sk", DataType::Int32),
                ("cs_bill_hdemo_sk", DataType::Int32),
                ("cs_bill_addr_sk", DataType::Int32),
                ("cs_ship_customer_sk", DataType::Int32),
                ("cs_ship_cdemo_sk", DataType::Int32),
                ("cs_ship_hdemo_sk", DataType::Int32),
                ("cs_ship_addr_sk", DataType::Int32),
                ("cs_call_center_sk", DataType::Int32),
                ("cs_catalog_page_sk", DataType::Int32),
                ("cs_ship_mode_sk", DataType::Int32),
                ("cs_warehouse_sk", DataType::Int32),
                ("cs_item_sk", DataType::Int32),
                ("cs_promo_sk", DataType::Int32),
                ("cs_order_number", DataType::Int32),
                ("cs_quantity", DataType::Int32),
                (
                    "cs_wholesale_cost",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "cs_list_price",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "cs_sales_price",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "cs_ext_discount_amt",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "cs_ext_sales_price",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "cs_ext_wholesale_cost",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "cs_ext_list_price",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "cs_ext_tax",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "cs_coupon_amt",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "cs_ext_ship_cost",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "cs_net_paid",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "cs_net_paid_inc_tax",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "cs_net_paid_inc_ship",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "cs_net_paid_inc_ship_tax",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "cs_net_profit",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
            ],
            43204059,
            &[],
        );
    }
    if which == "tpcds" {
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
            433000,
            &[("c_customer_sk", 433000)],
        );
    }
    if which == "tpcds" {
        register(
            &schema,
            "customer_address",
            &[
                ("ca_address_sk", DataType::Int32),
                ("ca_address_id", DataType::Utf8),
                ("ca_street_number", DataType::Utf8),
                ("ca_street_name", DataType::Utf8),
                ("ca_street_type", DataType::Utf8),
                ("ca_suite_number", DataType::Utf8),
                ("ca_city", DataType::Utf8),
                ("ca_county", DataType::Utf8),
                ("ca_state", DataType::Utf8),
                ("ca_zip", DataType::Utf8),
                ("ca_country", DataType::Utf8),
                (
                    "ca_gmt_offset",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                ("ca_location_type", DataType::Utf8),
            ],
            216000,
            &[("ca_address_sk", 216000)],
        );
    }
    if which == "tpcds" {
        register(
            &schema,
            "customer_demographics",
            &[
                ("cd_demo_sk", DataType::Int32),
                ("cd_gender", DataType::Utf8),
                ("cd_marital_status", DataType::Utf8),
                ("cd_education_status", DataType::Utf8),
                ("cd_purchase_estimate", DataType::Int32),
                ("cd_credit_rating", DataType::Utf8),
                ("cd_dep_count", DataType::Int32),
                ("cd_dep_employed_count", DataType::Int32),
                ("cd_dep_college_count", DataType::Int32),
            ],
            1920800,
            &[("cd_demo_sk", 1920800)],
        );
    }
    if which == "tpcds" {
        register(
            &schema,
            "date_dim",
            &[
                ("d_date_sk", DataType::Int32),
                ("d_date_id", DataType::Utf8),
                ("d_date", DataType::Date32),
                ("d_month_seq", DataType::Int32),
                ("d_week_seq", DataType::Int32),
                ("d_quarter_seq", DataType::Int32),
                ("d_year", DataType::Int32),
                ("d_dow", DataType::Int32),
                ("d_moy", DataType::Int32),
                ("d_dom", DataType::Int32),
                ("d_qoy", DataType::Int32),
                ("d_fy_year", DataType::Int32),
                ("d_fy_quarter_seq", DataType::Int32),
                ("d_fy_week_seq", DataType::Int32),
                ("d_day_name", DataType::Utf8),
                ("d_quarter_name", DataType::Utf8),
                ("d_holiday", DataType::Utf8),
                ("d_weekend", DataType::Utf8),
                ("d_following_holiday", DataType::Utf8),
                ("d_first_dom", DataType::Int32),
                ("d_last_dom", DataType::Int32),
                ("d_same_day_ly", DataType::Int32),
                ("d_same_day_lq", DataType::Int32),
                ("d_current_day", DataType::Utf8),
                ("d_current_week", DataType::Utf8),
                ("d_current_month", DataType::Utf8),
                ("d_current_quarter", DataType::Utf8),
                ("d_current_year", DataType::Utf8),
            ],
            73049,
            &[("d_date_sk", 73049)],
        );
    }
    if which == "tpcds" {
        register(
            &schema,
            "household_demographics",
            &[
                ("hd_demo_sk", DataType::Int32),
                ("hd_income_band_sk", DataType::Int32),
                ("hd_buy_potential", DataType::Utf8),
                ("hd_dep_count", DataType::Int32),
                ("hd_vehicle_count", DataType::Int32),
            ],
            7200,
            &[("hd_demo_sk", 7200)],
        );
    }
    if which == "tpcds" {
        register(
            &schema,
            "income_band",
            &[
                ("ib_income_band_sk", DataType::Int32),
                ("ib_lower_bound", DataType::Int32),
                ("ib_upper_bound", DataType::Int32),
            ],
            20,
            &[("ib_income_band_sk", 20)],
        );
    }
    if which == "tpcds" {
        register(
            &schema,
            "inventory",
            &[
                ("inv_date_sk", DataType::Int32),
                ("inv_item_sk", DataType::Int32),
                ("inv_warehouse_sk", DataType::Int32),
                ("inv_quantity_on_hand", DataType::Int32),
            ],
            31321566,
            &[],
        );
    }
    if which == "tpcds" {
        register(
            &schema,
            "item",
            &[
                ("i_item_sk", DataType::Int32),
                ("i_item_id", DataType::Utf8),
                ("i_rec_start_date", DataType::Date32),
                ("i_rec_end_date", DataType::Date32),
                ("i_item_desc", DataType::Utf8),
                (
                    "i_current_price",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "i_wholesale_cost",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                ("i_brand_id", DataType::Int32),
                ("i_brand", DataType::Utf8),
                ("i_class_id", DataType::Int32),
                ("i_class", DataType::Utf8),
                ("i_category_id", DataType::Int32),
                ("i_category", DataType::Utf8),
                ("i_manufact_id", DataType::Int32),
                ("i_manufact", DataType::Utf8),
                ("i_size", DataType::Utf8),
                ("i_formulation", DataType::Utf8),
                ("i_color", DataType::Utf8),
                ("i_units", DataType::Utf8),
                ("i_container", DataType::Utf8),
                ("i_manager_id", DataType::Int32),
                ("i_product_name", DataType::Utf8),
            ],
            40000,
            &[("i_item_sk", 40000)],
        );
    }
    if which == "tpcds" {
        register(
            &schema,
            "promotion",
            &[
                ("p_promo_sk", DataType::Int32),
                ("p_promo_id", DataType::Utf8),
                ("p_start_date_sk", DataType::Int32),
                ("p_end_date_sk", DataType::Int32),
                ("p_item_sk", DataType::Int32),
                (
                    "p_cost",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                ("p_response_targe", DataType::Int32),
                ("p_promo_name", DataType::Utf8),
                ("p_channel_dmail", DataType::Utf8),
                ("p_channel_email", DataType::Utf8),
                ("p_channel_catalog", DataType::Utf8),
                ("p_channel_tv", DataType::Utf8),
                ("p_channel_radio", DataType::Utf8),
                ("p_channel_press", DataType::Utf8),
                ("p_channel_event", DataType::Utf8),
                ("p_channel_demo", DataType::Utf8),
                ("p_channel_details", DataType::Utf8),
                ("p_purpose", DataType::Utf8),
                ("p_discount_active", DataType::Utf8),
            ],
            411,
            &[("p_promo_sk", 411)],
        );
    }
    if which == "tpcds" {
        register(
            &schema,
            "reason",
            &[
                ("r_reason_sk", DataType::Int32),
                ("r_reason_id", DataType::Utf8),
                ("r_reason_desc", DataType::Utf8),
            ],
            75,
            &[("r_reason_sk", 75)],
        );
    }
    if which == "tpcds" {
        register(
            &schema,
            "ship_mode",
            &[
                ("sm_ship_mode_sk", DataType::Int32),
                ("sm_ship_mode_id", DataType::Utf8),
                ("sm_type", DataType::Utf8),
                ("sm_code", DataType::Utf8),
                ("sm_carrier", DataType::Utf8),
                ("sm_contract", DataType::Utf8),
            ],
            20,
            &[("sm_ship_mode_sk", 20)],
        );
    }
    if which == "tpcds" {
        register(
            &schema,
            "store",
            &[
                ("s_store_sk", DataType::Int32),
                ("s_store_id", DataType::Utf8),
                ("s_rec_start_date", DataType::Date32),
                ("s_rec_end_date", DataType::Date32),
                ("s_closed_date_sk", DataType::Int32),
                ("s_store_name", DataType::Utf8),
                ("s_number_employees", DataType::Int32),
                ("s_floor_space", DataType::Int32),
                ("s_hours", DataType::Utf8),
                ("s_manager", DataType::Utf8),
                ("s_market_id", DataType::Int32),
                ("s_geography_class", DataType::Utf8),
                ("s_market_desc", DataType::Utf8),
                ("s_market_manager", DataType::Utf8),
                ("s_division_id", DataType::Int32),
                ("s_division_name", DataType::Utf8),
                ("s_company_id", DataType::Int32),
                ("s_company_name", DataType::Utf8),
                ("s_street_number", DataType::Utf8),
                ("s_street_name", DataType::Utf8),
                ("s_street_type", DataType::Utf8),
                ("s_suite_number", DataType::Utf8),
                ("s_city", DataType::Utf8),
                ("s_county", DataType::Utf8),
                ("s_state", DataType::Utf8),
                ("s_zip", DataType::Utf8),
                ("s_country", DataType::Utf8),
                (
                    "s_gmt_offset",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "s_tax_precentage",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
            ],
            78,
            &[("s_store_sk", 78)],
        );
    }
    if which == "tpcds" {
        register(
            &schema,
            "store_returns",
            &[
                ("sr_returned_date_sk", DataType::Int32),
                ("sr_return_time_sk", DataType::Int32),
                ("sr_item_sk", DataType::Int32),
                ("sr_customer_sk", DataType::Int32),
                ("sr_cdemo_sk", DataType::Int32),
                ("sr_hdemo_sk", DataType::Int32),
                ("sr_addr_sk", DataType::Int32),
                ("sr_store_sk", DataType::Int32),
                ("sr_reason_sk", DataType::Int32),
                ("sr_ticket_number", DataType::Int32),
                ("sr_return_quantity", DataType::Int32),
                (
                    "sr_return_amt",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "sr_return_tax",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "sr_return_amt_inc_tax",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "sr_fee",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "sr_return_ship_cost",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "sr_refunded_cash",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "sr_reversed_charge",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "sr_store_credit",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "sr_net_loss",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
            ],
            8636578,
            &[],
        );
    }
    if which == "tpcds" {
        register(
            &schema,
            "store_sales",
            &[
                ("ss_sold_date_sk", DataType::Int32),
                ("ss_sold_time_sk", DataType::Int32),
                ("ss_item_sk", DataType::Int32),
                ("ss_customer_sk", DataType::Int32),
                ("ss_cdemo_sk", DataType::Int32),
                ("ss_hdemo_sk", DataType::Int32),
                ("ss_addr_sk", DataType::Int32),
                ("ss_store_sk", DataType::Int32),
                ("ss_promo_sk", DataType::Int32),
                ("ss_ticket_number", DataType::Int32),
                ("ss_quantity", DataType::Int32),
                (
                    "ss_wholesale_cost",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "ss_list_price",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "ss_sales_price",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "ss_ext_discount_amt",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "ss_ext_sales_price",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "ss_ext_wholesale_cost",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "ss_ext_list_price",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "ss_ext_tax",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "ss_coupon_amt",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "ss_net_paid",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "ss_net_paid_inc_tax",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "ss_net_profit",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
            ],
            86406277,
            &[],
        );
    }
    if which == "tpcds" {
        register(
            &schema,
            "time_dim",
            &[
                ("t_time_sk", DataType::Int32),
                ("t_time_id", DataType::Utf8),
                ("t_time", DataType::Int32),
                ("t_hour", DataType::Int32),
                ("t_minute", DataType::Int32),
                ("t_second", DataType::Int32),
                ("t_am_pm", DataType::Utf8),
                ("t_shift", DataType::Utf8),
                ("t_sub_shift", DataType::Utf8),
                ("t_meal_time", DataType::Utf8),
            ],
            86400,
            &[("t_time_sk", 86400)],
        );
    }
    if which == "tpcds" {
        register(
            &schema,
            "warehouse",
            &[
                ("w_warehouse_sk", DataType::Int32),
                ("w_warehouse_id", DataType::Utf8),
                ("w_warehouse_name", DataType::Utf8),
                ("w_warehouse_sq_ft", DataType::Int32),
                ("w_street_number", DataType::Utf8),
                ("w_street_name", DataType::Utf8),
                ("w_street_type", DataType::Utf8),
                ("w_suite_number", DataType::Utf8),
                ("w_city", DataType::Utf8),
                ("w_county", DataType::Utf8),
                ("w_state", DataType::Utf8),
                ("w_zip", DataType::Utf8),
                ("w_country", DataType::Utf8),
                (
                    "w_gmt_offset",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
            ],
            6,
            &[("w_warehouse_sk", 6)],
        );
    }
    if which == "tpcds" {
        register(
            &schema,
            "web_page",
            &[
                ("wp_web_page_sk", DataType::Int32),
                ("wp_web_page_id", DataType::Utf8),
                ("wp_rec_start_date", DataType::Date32),
                ("wp_rec_end_date", DataType::Date32),
                ("wp_creation_date_sk", DataType::Int32),
                ("wp_access_date_sk", DataType::Int32),
                ("wp_autogen_flag", DataType::Utf8),
                ("wp_customer_sk", DataType::Int32),
                ("wp_url", DataType::Utf8),
                ("wp_type", DataType::Utf8),
                ("wp_char_count", DataType::Int32),
                ("wp_link_count", DataType::Int32),
                ("wp_image_count", DataType::Int32),
                ("wp_max_ad_count", DataType::Int32),
            ],
            468,
            &[("wp_web_page_sk", 468)],
        );
    }
    if which == "tpcds" {
        register(
            &schema,
            "web_returns",
            &[
                ("wr_returned_date_sk", DataType::Int32),
                ("wr_returned_time_sk", DataType::Int32),
                ("wr_item_sk", DataType::Int32),
                ("wr_refunded_customer_sk", DataType::Int32),
                ("wr_refunded_cdemo_sk", DataType::Int32),
                ("wr_refunded_hdemo_sk", DataType::Int32),
                ("wr_refunded_addr_sk", DataType::Int32),
                ("wr_returning_customer_sk", DataType::Int32),
                ("wr_returning_cdemo_sk", DataType::Int32),
                ("wr_returning_hdemo_sk", DataType::Int32),
                ("wr_returning_addr_sk", DataType::Int32),
                ("wr_web_page_sk", DataType::Int32),
                ("wr_reason_sk", DataType::Int32),
                ("wr_order_number", DataType::Int32),
                ("wr_return_quantity", DataType::Int32),
                (
                    "wr_return_amt",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "wr_return_tax",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "wr_return_amt_inc_tax",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "wr_fee",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "wr_return_ship_cost",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "wr_refunded_cash",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "wr_reversed_charge",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "wr_account_credit",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "wr_net_loss",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
            ],
            2160540,
            &[],
        );
    }
    if which == "tpcds" {
        register(
            &schema,
            "web_sales",
            &[
                ("ws_sold_date_sk", DataType::Int32),
                ("ws_sold_time_sk", DataType::Int32),
                ("ws_ship_date_sk", DataType::Int32),
                ("ws_item_sk", DataType::Int32),
                ("ws_bill_customer_sk", DataType::Int32),
                ("ws_bill_cdemo_sk", DataType::Int32),
                ("ws_bill_hdemo_sk", DataType::Int32),
                ("ws_bill_addr_sk", DataType::Int32),
                ("ws_ship_customer_sk", DataType::Int32),
                ("ws_ship_cdemo_sk", DataType::Int32),
                ("ws_ship_hdemo_sk", DataType::Int32),
                ("ws_ship_addr_sk", DataType::Int32),
                ("ws_web_page_sk", DataType::Int32),
                ("ws_web_site_sk", DataType::Int32),
                ("ws_ship_mode_sk", DataType::Int32),
                ("ws_warehouse_sk", DataType::Int32),
                ("ws_promo_sk", DataType::Int32),
                ("ws_order_number", DataType::Int32),
                ("ws_quantity", DataType::Int32),
                (
                    "ws_wholesale_cost",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "ws_list_price",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "ws_sales_price",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "ws_ext_discount_amt",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "ws_ext_sales_price",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "ws_ext_wholesale_cost",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "ws_ext_list_price",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "ws_ext_tax",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "ws_coupon_amt",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "ws_ext_ship_cost",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "ws_net_paid",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "ws_net_paid_inc_tax",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "ws_net_paid_inc_ship",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "ws_net_paid_inc_ship_tax",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "ws_net_profit",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
            ],
            21598604,
            &[],
        );
    }
    if which == "tpcds" {
        register(
            &schema,
            "web_site",
            &[
                ("web_site_sk", DataType::Int32),
                ("web_site_id", DataType::Utf8),
                ("web_rec_start_date", DataType::Date32),
                ("web_rec_end_date", DataType::Date32),
                ("web_name", DataType::Utf8),
                ("web_open_date_sk", DataType::Int32),
                ("web_close_date_sk", DataType::Int32),
                ("web_class", DataType::Utf8),
                ("web_manager", DataType::Utf8),
                ("web_mkt_id", DataType::Int32),
                ("web_mkt_class", DataType::Utf8),
                ("web_mkt_desc", DataType::Utf8),
                ("web_market_manager", DataType::Utf8),
                ("web_company_id", DataType::Int32),
                ("web_company_name", DataType::Utf8),
                ("web_street_number", DataType::Utf8),
                ("web_street_name", DataType::Utf8),
                ("web_street_type", DataType::Utf8),
                ("web_suite_number", DataType::Utf8),
                ("web_city", DataType::Utf8),
                ("web_county", DataType::Utf8),
                ("web_state", DataType::Utf8),
                ("web_zip", DataType::Utf8),
                ("web_country", DataType::Utf8),
                (
                    "web_gmt_offset",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
                (
                    "web_tax_percentage",
                    DataType::Decimal128 {
                        precision: 7,
                        scale: 2,
                    },
                ),
            ],
            26,
            &[("web_site_sk", 26)],
        );
    }
    if which == "tpch" {
        register(
            &schema,
            "customer",
            &[
                ("c_custkey", DataType::Int64),
                ("c_name", DataType::Utf8),
                ("c_address", DataType::Utf8),
                ("c_nationkey", DataType::Int64),
                ("c_phone", DataType::Utf8),
                ("c_acctbal", DataType::Float64),
                ("c_mktsegment", DataType::Utf8),
                ("c_comment", DataType::Utf8),
            ],
            4500000,
            &[("c_custkey", 4500000)],
        );
    }
    if which == "tpch" {
        register(
            &schema,
            "lineitem",
            &[
                ("l_orderkey", DataType::Int64),
                ("l_partkey", DataType::Int64),
                ("l_suppkey", DataType::Int64),
                ("l_linenumber", DataType::Int32),
                ("l_quantity", DataType::Float64),
                ("l_extendedprice", DataType::Float64),
                ("l_discount", DataType::Float64),
                ("l_tax", DataType::Float64),
                ("l_returnflag", DataType::Utf8),
                ("l_linestatus", DataType::Utf8),
                ("l_shipdate", DataType::Date32),
                ("l_commitdate", DataType::Date32),
                ("l_receiptdate", DataType::Date32),
                ("l_shipinstruct", DataType::Utf8),
                ("l_shipmode", DataType::Utf8),
                ("l_comment", DataType::Utf8),
            ],
            179998372,
            &[],
        );
    }
    if which == "tpch" {
        register(
            &schema,
            "nation",
            &[
                ("n_nationkey", DataType::Int64),
                ("n_name", DataType::Utf8),
                ("n_regionkey", DataType::Int64),
                ("n_comment", DataType::Utf8),
            ],
            25,
            &[("n_nationkey", 25)],
        );
    }
    if which == "tpch" {
        register(
            &schema,
            "orders",
            &[
                ("o_orderkey", DataType::Int64),
                ("o_custkey", DataType::Int64),
                ("o_orderstatus", DataType::Utf8),
                ("o_totalprice", DataType::Float64),
                ("o_orderdate", DataType::Date32),
                ("o_orderpriority", DataType::Utf8),
                ("o_clerk", DataType::Utf8),
                ("o_shippriority", DataType::Int32),
                ("o_comment", DataType::Utf8),
            ],
            45000000,
            &[("o_orderkey", 45000000)],
        );
    }
    if which == "tpch" {
        register(
            &schema,
            "part",
            &[
                ("p_partkey", DataType::Int64),
                ("p_name", DataType::Utf8),
                ("p_mfgr", DataType::Utf8),
                ("p_brand", DataType::Utf8),
                ("p_type", DataType::Utf8),
                ("p_size", DataType::Int32),
                ("p_container", DataType::Utf8),
                ("p_retailprice", DataType::Float64),
                ("p_comment", DataType::Utf8),
            ],
            6000000,
            &[("p_partkey", 6000000)],
        );
    }
    if which == "tpch" {
        register(
            &schema,
            "partsupp",
            &[
                ("ps_partkey", DataType::Int64),
                ("ps_suppkey", DataType::Int64),
                ("ps_availqty", DataType::Int32),
                ("ps_supplycost", DataType::Float64),
                ("ps_comment", DataType::Utf8),
            ],
            24000000,
            &[],
        );
    }
    if which == "tpch" {
        register(
            &schema,
            "region",
            &[
                ("r_regionkey", DataType::Int64),
                ("r_name", DataType::Utf8),
                ("r_comment", DataType::Utf8),
            ],
            5,
            &[("r_regionkey", 5)],
        );
    }
    if which == "tpch" {
        register(
            &schema,
            "supplier",
            &[
                ("s_suppkey", DataType::Int64),
                ("s_name", DataType::Utf8),
                ("s_address", DataType::Utf8),
                ("s_nationkey", DataType::Int64),
                ("s_phone", DataType::Utf8),
                ("s_acctbal", DataType::Float64),
                ("s_comment", DataType::Utf8),
            ],
            300000,
            &[("s_suppkey", 300000)],
        );
    }
    catalog.register_schema("public", schema);
    manager.register_catalog("default", catalog);
    manager
}
