//! A semi join that tests a column of one joined table filters that table
//! before the join, not the joined rows after it.
//!
//! TPC-DS q58 / q83 shape: `d_date IN (SELECT d_date FROM date_dim WHERE ...)`
//! over `fact JOIN date_dim JOIN item`. Left above the joins, every fact row
//! is joined to both dimensions and shipped to the semi join before the few
//! matching dates are picked.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use arneb_catalog::{CatalogManager, MemoryCatalog, MemorySchema, TableProvider, TableStatistics};
use arneb_common::types::{ColumnInfo, DataType};
use arneb_planner::{LogicalOptimizer, LogicalPlan, QueryPlanner};

#[derive(Debug)]
struct StatsTable {
    schema: Vec<ColumnInfo>,
    rows: u64,
}

impl TableProvider for StatsTable {
    fn schema(&self) -> Vec<ColumnInfo> {
        self.schema.clone()
    }

    /// Row counts only, as the Hive catalog reports them.
    fn statistics(&self) -> Option<TableStatistics> {
        Some(TableStatistics {
            row_count: Some(self.rows),
            size_bytes: None,
            columns: HashMap::new(),
        })
    }

    fn canonical_name(&self) -> Option<(String, String)> {
        None
    }
}

fn table(rows: u64, columns: &[(&str, DataType)]) -> Arc<dyn TableProvider> {
    Arc::new(StatsTable {
        schema: columns
            .iter()
            .map(|(name, data_type)| ColumnInfo {
                name: name.to_string(),
                data_type: data_type.clone(),
                nullable: true,
            })
            .collect(),
        rows,
    })
}

fn catalog() -> CatalogManager {
    let manager = CatalogManager::new("default", "public");
    let catalog = Arc::new(MemoryCatalog::new());
    let schema = Arc::new(MemorySchema::new());
    schema.register_table(
        "sales",
        table(
            80_000_000,
            &[
                ("s_date_sk", DataType::Int64),
                ("s_item_sk", DataType::Int64),
                ("s_qty", DataType::Int64),
            ],
        ),
    );
    schema.register_table(
        "dates",
        table(
            73_000,
            &[
                ("d_date_sk", DataType::Int64),
                ("d_date", DataType::Date32),
                ("d_week", DataType::Int64),
            ],
        ),
    );
    schema.register_table(
        "items",
        table(
            60_000,
            &[("i_item_sk", DataType::Int64), ("i_name", DataType::Utf8)],
        ),
    );
    schema.register_table(
        "parts",
        table(
            6_000_000,
            &[("p_part_sk", DataType::Int64), ("p_name", DataType::Utf8)],
        ),
    );
    catalog.register_schema("public", schema);
    manager.register_catalog("default", catalog);
    manager
}

async fn optimized(sql: &str) -> LogicalPlan {
    let catalog = catalog();
    let stmt = arneb_sql_parser::parse(sql).expect("parse");
    let (plan, _ctx) = QueryPlanner::new(&catalog)
        .plan_statement_with_context(&stmt)
        .await
        .expect("plan");
    LogicalOptimizer::default_rules()
        .optimize(plan)
        .expect("optimize")
}

#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Semi,
    Anti,
}

fn scanned(plan: &LogicalPlan, out: &mut BTreeSet<String>) {
    match plan {
        LogicalPlan::TableScan { table, .. } => {
            out.insert(table.table.clone());
        }
        LogicalPlan::Projection { input, .. }
        | LogicalPlan::Filter { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Distinct { input }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. } => scanned(input, out),
        LogicalPlan::Join { left, right, .. }
        | LogicalPlan::SemiJoin { left, right, .. }
        | LogicalPlan::AntiJoin { left, right, .. } => {
            scanned(left, out);
            scanned(right, out);
        }
        other => panic!("unexpected node in a test plan:\n{other}"),
    }
}

fn collect_kept_sides(plan: &LogicalPlan, kind: Kind, out: &mut Vec<BTreeSet<String>>) {
    match plan {
        LogicalPlan::TableScan { .. } => {}
        LogicalPlan::Projection { input, .. }
        | LogicalPlan::Filter { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Distinct { input }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. } => collect_kept_sides(input, kind, out),
        LogicalPlan::Join { left, right, .. } => {
            collect_kept_sides(left, kind, out);
            collect_kept_sides(right, kind, out);
        }
        LogicalPlan::SemiJoin { left, right, .. } | LogicalPlan::AntiJoin { left, right, .. } => {
            let this = if matches!(plan, LogicalPlan::SemiJoin { .. }) {
                Kind::Semi
            } else {
                Kind::Anti
            };
            if this == kind {
                let mut tables = BTreeSet::new();
                scanned(left, &mut tables);
                out.push(tables);
            }
            collect_kept_sides(left, kind, out);
            collect_kept_sides(right, kind, out);
        }
        other => panic!("unexpected node in a test plan:\n{other}"),
    }
}

/// For every semi (or anti) join, top-down, the tables scanned under the
/// side whose rows it keeps.
fn kept_side_tables(plan: &LogicalPlan, kind: Kind) -> Vec<BTreeSet<String>> {
    let mut out = Vec::new();
    collect_kept_sides(plan, kind, &mut out);
    out
}

fn set(tables: &[&str]) -> BTreeSet<String> {
    tables.iter().map(|t| t.to_string()).collect()
}

const DATE_IN_WEEK: &str = "d_date IN (SELECT d_date FROM dates WHERE d_week = 5)";

#[tokio::test]
async fn an_in_subquery_on_a_dimension_column_filters_the_dimension_alone() {
    let plan = optimized(&format!(
        "SELECT i_name, sum(s_qty) FROM sales, dates, items \
         WHERE s_date_sk = d_date_sk AND s_item_sk = i_item_sk AND {DATE_IN_WEEK} \
         GROUP BY i_name"
    ))
    .await;
    assert_eq!(
        kept_side_tables(&plan, Kind::Semi),
        vec![set(&["dates"])],
        "{plan}"
    );
}

#[tokio::test]
async fn the_same_holds_for_explicit_inner_joins() {
    let plan = optimized(&format!(
        "SELECT i_name, sum(s_qty) FROM sales \
         JOIN dates ON s_date_sk = d_date_sk \
         JOIN items ON s_item_sk = i_item_sk \
         WHERE {DATE_IN_WEEK} GROUP BY i_name"
    ))
    .await;
    assert_eq!(
        kept_side_tables(&plan, Kind::Semi),
        vec![set(&["dates"])],
        "{plan}"
    );
}

/// `sales LEFT JOIN dates` keeps sales rows that have no date, with NULL date
/// columns. The semi join above removes them (NULL is in no list). Filtering
/// `dates` below the join instead would let them through.
#[tokio::test]
async fn a_semi_join_on_the_null_supplying_side_of_an_outer_join_stays_above_it() {
    let plan = optimized(&format!(
        "SELECT s_qty FROM sales LEFT JOIN dates ON s_date_sk = d_date_sk \
         WHERE {DATE_IN_WEEK}"
    ))
    .await;
    assert_eq!(
        kept_side_tables(&plan, Kind::Semi),
        vec![set(&["dates", "sales"])],
        "{plan}"
    );
}

/// `NOT IN` keeps the rows the list does not contain, so an unmatched sales
/// row of the outer join (NULL date) is decided by the anti join, not by the
/// join below it.
#[tokio::test]
async fn an_anti_join_on_the_null_supplying_side_of_an_outer_join_stays_above_it() {
    let plan = optimized(
        "SELECT s_qty FROM sales LEFT JOIN dates ON s_date_sk = d_date_sk \
         WHERE d_date NOT IN (SELECT d_date FROM dates WHERE d_week = 5)",
    )
    .await;
    assert_eq!(
        kept_side_tables(&plan, Kind::Anti),
        vec![set(&["dates", "sales"]); 3],
        "{plan}"
    );
}

/// The tested value needs columns of both joined tables, so no single table
/// can be filtered ahead of the join.
#[tokio::test]
async fn a_semi_join_on_a_value_computed_from_both_sides_stays_above_the_join() {
    let plan = optimized(
        "SELECT s_qty FROM sales, dates \
         WHERE s_date_sk = d_date_sk \
         AND s_item_sk + d_week IN (SELECT i_item_sk FROM items WHERE i_name = 'x')",
    )
    .await;
    assert_eq!(
        kept_side_tables(&plan, Kind::Semi),
        vec![set(&["dates", "sales"])],
        "{plan}"
    );
}

/// The correlated EXISTS compares a dates column with the subquery's row and
/// also reads a sales column, so its rows cannot be decided from dates alone.
#[tokio::test]
async fn a_semi_join_whose_extra_condition_reads_the_other_side_stays_above_the_join() {
    let plan = optimized(
        "SELECT s_qty FROM sales, dates \
         WHERE s_date_sk = d_date_sk \
         AND EXISTS (SELECT 1 FROM items WHERE i_item_sk = d_week AND i_item_sk <> s_item_sk)",
    )
    .await;
    assert_eq!(
        kept_side_tables(&plan, Kind::Semi),
        vec![set(&["dates", "sales"])],
        "{plan}"
    );
}

/// The dimension is reached through the larger input of the outer join
/// (`sales JOIN dates` is far larger than `parts`), and is still the table
/// that gets filtered.
#[tokio::test]
async fn the_dimension_is_found_under_the_larger_input_of_another_join() {
    let plan = optimized(&format!(
        "SELECT p_name, sum(s_qty) FROM sales, dates, parts \
         WHERE s_date_sk = d_date_sk AND s_item_sk = p_part_sk AND {DATE_IN_WEEK} \
         GROUP BY p_name"
    ))
    .await;
    assert_eq!(
        kept_side_tables(&plan, Kind::Semi),
        vec![set(&["dates"])],
        "{plan}"
    );
}

/// TPC-DS q23 / q95 shape: the tested column belongs to the fact table, and
/// the join with the filtered dimension is what cuts the fact rows down.
/// Testing every fact row before that join would be more work, not less.
#[tokio::test]
async fn a_semi_join_on_the_larger_table_stays_above_its_join_with_a_filtered_dimension() {
    let plan = optimized(
        "SELECT s_qty FROM sales, dates \
         WHERE s_date_sk = d_date_sk AND d_week = 5 \
         AND s_item_sk IN (SELECT i_item_sk FROM items WHERE i_name = 'x')",
    )
    .await;
    assert_eq!(
        kept_side_tables(&plan, Kind::Semi),
        vec![set(&["dates", "sales"])],
        "{plan}"
    );
}

/// The same when other joins sit above: the semi join is not moved onto the
/// fact table under them.
#[tokio::test]
async fn a_semi_join_on_the_fact_table_is_not_moved_below_several_joins() {
    let plan = optimized(
        "SELECT p_name, sum(s_qty) FROM sales, dates, parts \
         WHERE s_date_sk = d_date_sk AND s_item_sk = p_part_sk AND d_week = 5 \
         AND s_item_sk IN (SELECT i_item_sk FROM items WHERE i_name = 'x') \
         GROUP BY p_name",
    )
    .await;
    assert_eq!(
        kept_side_tables(&plan, Kind::Semi),
        vec![set(&["dates", "parts", "sales"])],
        "{plan}"
    );
}
