//! Diagnostic (Tier-1, deterministic, no docker): does the partitioned
//! build-side swap actually FIRE on the real q09 / q21 plans at SF30 stats?
//!
//! The swap (fragment.rs) fires for an INNER pure-equi join when
//! `estimated_cardinality(right) >= estimated_cardinality(left) * factor`.
//! `estimated_cardinality` is the SAME (possibly under-estimating) cost
//! model JoinReorder uses, so the open question is whether the build (right)
//! subtree of q09/q21's big joins actually estimates large enough to trigger.
//! This walks the real optimized plan and reports, per INNER join, the
//! left/right estimates + ratio + would-fire verdict — without fragmenting.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use arneb_catalog::{
    CatalogManager, ColumnStatistics, MemoryCatalog, MemorySchema, TableProvider, TableStatistics,
};
use arneb_common::types::{ColumnInfo, DataType, ScalarValue};
use arneb_planner::{
    cost::estimated_cardinality, CatalogStats, JoinCondition, LogicalOptimizer, LogicalPlan,
    QueryPlanner,
};

const Q09_SQL: &str = include_str!("../../../benchmarks/tpch/queries/q09.sql");
const Q08_SQL: &str = include_str!("../../../benchmarks/tpch/queries/q08.sql");
const Q21_SQL: &str = include_str!("../../../benchmarks/tpch/queries/q21.sql");
const Q05_SQL: &str = include_str!("../../../benchmarks/tpch/queries/q05.sql");

const FACTOR: f64 = 100.0; // ARNEB_PARTITIONED_BUILD_SIDE_SWAP_FACTOR default

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
}

#[tokio::test]
async fn q09_build_side_swap_would_fire() {
    let _guard = selective_dim_env_lock().lock().await;
    diagnose("Q09", Q09_SQL).await;
}

#[tokio::test]
async fn q21_build_side_swap_would_fire() {
    let _guard = selective_dim_env_lock().lock().await;
    diagnose("Q21", Q21_SQL).await;
}

#[tokio::test]
async fn q21_selective_dim_first_joins_nation_supplier_before_lineitem() {
    let _guard = selective_dim_env_lock().lock().await;
    let (logical_plan, stats) = optimized_plan(Q21_SQL).await;

    let first = deepest_left_join(&logical_plan).expect("q21 has an inner join chain");
    assert_eq!(tables(first), vec!["nation", "supplier"]);
    let first_out = estimated_cardinality(first, &stats);
    assert!(
        (first_out - 12_000.0).abs() < 1.0,
        "expected nation⋈supplier first join output ~12K, got {first_out}"
    );

    let lineitem_join = find_join_with_tables(&logical_plan, &["lineitem", "nation", "supplier"])
        .expect("q21 should join lineitem after nation⋈supplier");
    let lineitem_out = estimated_cardinality(lineitem_join, &stats);
    assert!(
        (lineitem_out - 3_600_000.0).abs() < 1.0,
        "expected nation⋈supplier⋈lineitem intermediate ~3.6M, got {lineitem_out}"
    );
    assert!(
        lineitem_out < 90_000_000.0,
        "selective dimension first should avoid the old 90M supplier⋈lineitem intermediate"
    );
}

#[tokio::test]
async fn q09_selective_dim_first_keeps_first_intermediate_small() {
    let _guard = selective_dim_env_lock().lock().await;

    disable_selective_dim_first_for_test();
    let (off_plan, off_stats) = optimized_plan_without_env_change(Q09_SQL).await;
    let off = join_profile(&off_plan, &off_stats);

    enable_selective_dim_first_for_test();
    let (logical_plan, stats) = optimized_plan(Q09_SQL).await;
    let on = join_profile(&logical_plan, &stats);

    assert_eq!(
        on.order, off.order,
        "q09 SELDIM-ON should preserve the default join order"
    );

    let first = deepest_left_join(&logical_plan).expect("q09 has an inner join chain");
    assert_eq!(tables(first), vec!["lineitem", "part"]);
    let first_out = estimated_cardinality(first, &stats);
    assert!(
        (first_out - 18_000_000.0).abs() < 1.0,
        "expected q09 first intermediate to stay at ~18M, got {first_out}"
    );
    assert!(
        first_out < 180_000_000.0,
        "q09 regression guard: first intermediate must not move to the 180M fact-sized join"
    );
}

#[tokio::test]
async fn q08_selective_dim_first_places_region_nation_customer_before_facts() {
    let _guard = selective_dim_env_lock().lock().await;

    disable_selective_dim_first_for_test();
    let (off_plan, off_stats) = optimized_plan_without_env_change(Q08_SQL).await;
    let off = join_profile(&off_plan, &off_stats);

    enable_selective_dim_first_for_test();
    let (on_plan, on_stats) = optimized_plan_without_env_change(Q08_SQL).await;
    let on = join_profile(&on_plan, &on_stats);

    println!("\n========== Q08 selective-dim-first diagnosis ==========");
    print_profile("OFF", &off);
    print_profile("ON ", &on);
    for (idx, (off_step, on_step)) in off.steps.iter().zip(on.steps.iter()).enumerate() {
        if on_step.output > off_step.output * 1.1 {
            println!(
                "Q08 worse at step {}: OFF {} -> {:.0}, ON {} -> {:.0}",
                idx + 1,
                off_step.tables.join("+"),
                off_step.output,
                on_step.tables.join("+"),
                on_step.output
            );
        }
    }

    assert!(
        on.order == off.order,
        "q08 safety-net should suppress the regressing selective-dim order; OFF={:?}, ON={:?}",
        off.order,
        on.order
    );
    assert!(
        on.max_intermediate < 1_000_000.0,
        "q08 safety-net should keep the DP/part-first max intermediate below 1M, got {:.0}",
        on.max_intermediate
    );

    let part_join = find_join_with_tables(&on_plan, &["lineitem", "part"])
        .expect("q08 safety-net should keep the part-first DP join");
    let part_out = estimated_cardinality(part_join, &on_stats);
    assert!(
        part_out < 1_000_000.0,
        "expected q08 lineitem⋈part first intermediate below 1M, got {part_out}"
    );
}

#[tokio::test]
async fn q05_selective_dim_first_is_not_suppressed() {
    let _guard = selective_dim_env_lock().lock().await;

    disable_selective_dim_first_for_test();
    let (off_plan, off_stats) = optimized_plan_without_env_change(Q05_SQL).await;
    let off = join_profile(&off_plan, &off_stats);

    enable_selective_dim_first_for_test();
    let (on_plan, on_stats) = optimized_plan_without_env_change(Q05_SQL).await;
    let on = join_profile(&on_plan, &on_stats);

    println!("\n========== Q05 selective-dim-first diagnosis ==========");
    print_profile("OFF", &off);
    print_profile("ON ", &on);

    assert_ne!(
        on.order, off.order,
        "q05 selective-dim-first should not be suppressed; OFF={:?}, ON={:?}",
        off.order, on.order
    );
}

async fn diagnose(label: &str, sql: &str) {
    enable_selective_dim_first_for_test();
    let (logical_plan, stats) = optimized_plan(sql).await;

    println!("\n========== {label} INNER-join build-side analysis (factor={FACTOR}) ==========");
    let mut any_fire = false;
    walk(&logical_plan, &stats, &mut any_fire);
    println!("{label}: partitioned build-side swap WOULD FIRE on >=1 inner join: {any_fire}");
}

async fn optimized_plan(sql: &str) -> (LogicalPlan, Arc<CatalogStats>) {
    enable_selective_dim_first_for_test();
    optimized_plan_without_env_change(sql).await
}

async fn optimized_plan_without_env_change(sql: &str) -> (LogicalPlan, Arc<CatalogStats>) {
    let catalog = tpch_sf30_catalog();
    let stmt = arneb_sql_parser::parse(sql).expect("parse");
    let planner = QueryPlanner::new(&catalog);
    let (logical_plan, ctx) = planner
        .plan_statement_with_context(&stmt)
        .await
        .expect("plan");
    let logical_plan = LogicalOptimizer::default_rules()
        .optimize(logical_plan)
        .expect("optimize");

    let stats = ctx.catalog_stats.clone();
    (logical_plan, stats)
}

fn selective_dim_env_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

fn enable_selective_dim_first_for_test() {
    std::env::set_var("ARNEB_SELECTIVE_DIM_FIRST", "1");
}

fn disable_selective_dim_first_for_test() {
    std::env::remove_var("ARNEB_SELECTIVE_DIM_FIRST");
}

#[derive(Debug)]
struct JoinStep {
    tables: Vec<String>,
    output: f64,
}

#[derive(Debug)]
struct JoinProfile {
    order: Vec<String>,
    steps: Vec<JoinStep>,
    max_intermediate: f64,
}

fn join_profile(plan: &LogicalPlan, stats: &CatalogStats) -> JoinProfile {
    let root = largest_join(plan).expect("expected join chain");
    let order = left_deep_order(root);
    let mut steps = Vec::new();
    collect_join_steps(root, stats, &mut steps);
    let max_intermediate = steps
        .iter()
        .map(|s| s.output)
        .fold(0.0_f64, |a, b| a.max(b));
    JoinProfile {
        order,
        steps,
        max_intermediate,
    }
}

fn largest_join(plan: &LogicalPlan) -> Option<&LogicalPlan> {
    let mut best = None;
    let mut best_count = 0usize;
    find_largest_join(plan, &mut best, &mut best_count);
    best
}

fn find_largest_join<'a>(
    plan: &'a LogicalPlan,
    best: &mut Option<&'a LogicalPlan>,
    best_count: &mut usize,
) {
    if matches!(plan, LogicalPlan::Join { .. }) {
        let count = tables(plan).len();
        if count > *best_count {
            *best = Some(plan);
            *best_count = count;
        }
    }
    for child in children(plan) {
        find_largest_join(child, best, best_count);
    }
}

fn left_deep_order(plan: &LogicalPlan) -> Vec<String> {
    match plan {
        LogicalPlan::Join { left, right, .. } => {
            let mut out = left_deep_order(left);
            out.push(leaf_label(right));
            out
        }
        _ => vec![leaf_label(plan)],
    }
}

fn collect_join_steps(plan: &LogicalPlan, stats: &CatalogStats, out: &mut Vec<JoinStep>) {
    if let LogicalPlan::Join { left, .. } = plan {
        collect_join_steps(left, stats, out);
        out.push(JoinStep {
            tables: tables(plan),
            output: estimated_cardinality(plan, stats),
        });
    }
}

fn print_profile(label: &str, profile: &JoinProfile) {
    println!("{label} order: {}", profile.order.join(" -> "));
    for (idx, step) in profile.steps.iter().enumerate() {
        println!(
            "{label} step {}: [{}] rows={:.0}",
            idx + 1,
            step.tables.join("+"),
            step.output
        );
    }
    println!("{label} max intermediate: {:.0}", profile.max_intermediate);
}

fn walk(plan: &LogicalPlan, stats: &CatalogStats, any_fire: &mut bool) {
    if let LogicalPlan::Join {
        left,
        right,
        join_type,
        condition,
        ..
    } = plan
    {
        let est_left = estimated_cardinality(left, stats);
        let est_right = estimated_cardinality(right, stats);
        let ratio = if est_left > 0.0 {
            est_right / est_left
        } else {
            f64::INFINITY
        };
        let is_inner = matches!(join_type, arneb_sql_parser::ast::JoinType::Inner);
        let pure_equi = matches!(condition, JoinCondition::On(_)); // approx; q09/q21 inner joins are equi
        let fires = is_inner && pure_equi && est_right >= est_left * FACTOR;
        if fires {
            *any_fire = true;
        }
        println!(
            "  Join[{:?}] left=[{}]({:.0})  right=[{}]({:.0})  ratio={:.1}x  FIRES={}",
            join_type,
            tables(left).join("+"),
            est_left,
            tables(right).join("+"),
            est_right,
            ratio,
            fires
        );
    }
    // Recurse into all children.
    for child in children(plan) {
        walk(child, stats, any_fire);
    }
}

fn children(plan: &LogicalPlan) -> Vec<&LogicalPlan> {
    match plan {
        LogicalPlan::Join { left, right, .. }
        | LogicalPlan::SemiJoin { left, right, .. }
        | LogicalPlan::AntiJoin { left, right, .. }
        | LogicalPlan::Intersect { left, right }
        | LogicalPlan::Except { left, right } => vec![left, right],
        LogicalPlan::Projection { input, .. }
        | LogicalPlan::Filter { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Explain { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::PartialAggregate { input, .. }
        | LogicalPlan::FinalAggregate { input, .. }
        | LogicalPlan::Distinct { input }
        | LogicalPlan::Window { input, .. }
        | LogicalPlan::AssignUniqueId { input, .. } => vec![input],
        LogicalPlan::ScalarSubquery { subplan } => vec![subplan],
        LogicalPlan::UnionAll { inputs } => inputs.iter().collect(),
        _ => vec![],
    }
}

fn tables(plan: &LogicalPlan) -> Vec<String> {
    let mut out = Vec::new();
    collect_tables(plan, &mut out);
    out.sort();
    out.dedup();
    out
}

fn leaf_label(plan: &LogicalPlan) -> String {
    tables(plan).join("+")
}

fn deepest_left_join(plan: &LogicalPlan) -> Option<&LogicalPlan> {
    match plan {
        LogicalPlan::Join { left, .. } => deepest_left_join(left).or(Some(plan)),
        _ => children(plan).into_iter().find_map(deepest_left_join),
    }
}

fn find_join_with_tables<'a>(plan: &'a LogicalPlan, expected: &[&str]) -> Option<&'a LogicalPlan> {
    let mut expected = expected
        .iter()
        .map(|name| (*name).to_string())
        .collect::<Vec<_>>();
    expected.sort_unstable();
    find_join_with_table_names(plan, &expected)
}

fn find_join_with_table_names<'a>(
    plan: &'a LogicalPlan,
    expected: &[String],
) -> Option<&'a LogicalPlan> {
    match plan {
        LogicalPlan::Join { .. } if tables(plan) == expected => Some(plan),
        _ => children(plan)
            .into_iter()
            .find_map(|child| find_join_with_table_names(child, expected)),
    }
}

fn collect_tables(plan: &LogicalPlan, out: &mut Vec<String>) {
    if let LogicalPlan::TableScan { table, .. } = plan {
        out.push(table.table.clone());
    }
    for child in children(plan) {
        collect_tables(child, out);
    }
}

fn tpch_sf30_catalog() -> CatalogManager {
    let manager = CatalogManager::new("default", "public");
    let catalog = Arc::new(MemoryCatalog::new());
    let schema = Arc::new(MemorySchema::new());

    reg(
        &schema,
        "part",
        &[
            ("p_partkey", DataType::Int64),
            ("p_name", DataType::Utf8),
            ("p_type", DataType::Utf8),
        ],
        6_000_000,
        &[("p_partkey", 6_000_000), ("p_type", 500)],
    );
    reg(
        &schema,
        "supplier",
        &[
            ("s_suppkey", DataType::Int64),
            ("s_nationkey", DataType::Int64),
            ("s_name", DataType::Utf8),
        ],
        300_000,
        &[("s_suppkey", 300_000), ("s_nationkey", 25)],
    );
    reg(
        &schema,
        "lineitem",
        &[
            ("l_orderkey", DataType::Int64),
            ("l_partkey", DataType::Int64),
            ("l_suppkey", DataType::Int64),
            ("l_quantity", DataType::Float64),
            ("l_extendedprice", DataType::Float64),
            ("l_discount", DataType::Float64),
            ("l_commitdate", DataType::Date32),
            ("l_receiptdate", DataType::Date32),
        ],
        180_000_000,
        &[
            ("l_orderkey", 45_000_000),
            ("l_partkey", 6_000_000),
            ("l_suppkey", 300_000),
        ],
    );
    reg(
        &schema,
        "partsupp",
        &[
            ("ps_partkey", DataType::Int64),
            ("ps_suppkey", DataType::Int64),
            ("ps_supplycost", DataType::Float64),
        ],
        24_000_000,
        &[("ps_partkey", 6_000_000), ("ps_suppkey", 300_000)],
    );
    reg(
        &schema,
        "orders",
        &[
            ("o_orderkey", DataType::Int64),
            ("o_custkey", DataType::Int64),
            ("o_orderstatus", DataType::Utf8),
            ("o_orderdate", DataType::Date32),
        ],
        45_000_000,
        &[
            ("o_orderkey", 45_000_000),
            ("o_custkey", 4_500_000),
            ("o_orderstatus", 3),
            ("o_orderdate", 2_556),
        ],
    );
    reg(
        &schema,
        "customer",
        &[
            ("c_custkey", DataType::Int64),
            ("c_nationkey", DataType::Int64),
        ],
        4_500_000,
        &[("c_custkey", 4_500_000), ("c_nationkey", 25)],
    );
    reg(
        &schema,
        "nation",
        &[
            ("n_nationkey", DataType::Int64),
            ("n_name", DataType::Utf8),
            ("n_regionkey", DataType::Int64),
        ],
        25,
        &[("n_nationkey", 25), ("n_name", 25), ("n_regionkey", 5)],
    );
    reg(
        &schema,
        "region",
        &[("r_regionkey", DataType::Int64), ("r_name", DataType::Utf8)],
        5,
        &[("r_regionkey", 5), ("r_name", 5)],
    );

    catalog.register_schema("public", schema);
    manager.register_catalog("default", catalog);
    manager
}

fn reg(
    schema: &MemorySchema,
    name: &str,
    columns: &[(&str, DataType)],
    row_count: u64,
    ndvs: &[(&str, u64)],
) {
    let cols = columns
        .iter()
        .map(|(n, t)| ColumnInfo {
            name: (*n).to_string(),
            data_type: t.clone(),
            nullable: false,
        })
        .collect();
    let column_stats = ndvs
        .iter()
        .map(|(c, ndv)| {
            (
                (*c).to_string(),
                ColumnStatistics {
                    ndv: Some(*ndv),
                    null_fraction: Some(0.0),
                    min_value: min_value(c),
                    max_value: max_value(c),
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

fn min_value(column: &str) -> Option<ScalarValue> {
    match column {
        "o_orderdate" => Some(ScalarValue::Date32(7_305)),
        _ => None,
    }
}

fn max_value(column: &str) -> Option<ScalarValue> {
    match column {
        "o_orderdate" => Some(ScalarValue::Date32(9_861)),
        _ => None,
    }
}
