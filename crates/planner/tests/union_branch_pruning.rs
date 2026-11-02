//! Filters over a UNION ALL view reach its branches, and a branch whose
//! constant column can never satisfy the filter is not scanned at all.
//!
//! TPC-DS q04 builds `year_total` as a UNION ALL of three channels, each
//! tagging its rows with a constant `sale_type`, and reads it six times with
//! `sale_type = 's' | 'c' | 'w'`. Left above the UNION ALL, every reference
//! scans and aggregates all three channels (18 pipelines instead of 6).

use std::sync::Arc;

use arneb_catalog::{CatalogManager, MemoryCatalog, MemorySchema, MemoryTable};
use arneb_common::types::{ColumnInfo, DataType};
use arneb_planner::{LogicalOptimizer, LogicalPlan, QueryPlanner};

fn column(name: &str, data_type: DataType) -> ColumnInfo {
    ColumnInfo {
        name: name.to_string(),
        data_type,
        nullable: true,
    }
}

fn catalog() -> CatalogManager {
    let manager = CatalogManager::new("default", "public");
    let catalog = Arc::new(MemoryCatalog::new());
    let schema = Arc::new(MemorySchema::new());
    for table in ["s_tab", "c_tab", "w_tab"] {
        schema.register_table(
            table,
            Arc::new(MemoryTable::new(vec![
                column("id", DataType::Int64),
                column("v", DataType::Int64),
            ])),
        );
    }
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

const YT: &str = "WITH yt AS ( \
    SELECT id, v, 's' AS t FROM s_tab \
    UNION ALL SELECT id, v, 'c' AS t FROM c_tab \
    UNION ALL SELECT id, v, 'w' AS t FROM w_tab)";

fn scans(plan: &LogicalPlan, table: &str) -> usize {
    format!("{plan}")
        .lines()
        .filter(|l| l.trim_start().starts_with(&format!("TableScan: {table} ")))
        .count()
}

fn scan_counts(plan: &LogicalPlan) -> [usize; 3] {
    [
        scans(plan, "s_tab"),
        scans(plan, "c_tab"),
        scans(plan, "w_tab"),
    ]
}

#[tokio::test]
async fn each_reference_scans_only_the_branch_its_constant_filter_selects() {
    let plan = optimized(&format!(
        "{YT} SELECT a.id FROM yt a, yt b \
         WHERE a.id = b.id AND a.t = 's' AND b.t = 'c'"
    ))
    .await;
    assert_eq!(scan_counts(&plan), [1, 1, 0], "{plan}");
}

#[tokio::test]
async fn a_disjunction_keeps_every_branch_it_can_match() {
    let plan = optimized(&format!("{YT} SELECT id FROM yt WHERE t = 's' OR t = 'c'")).await;
    assert_eq!(scan_counts(&plan), [1, 1, 0], "{plan}");
}

#[tokio::test]
async fn a_filter_on_a_non_constant_column_reaches_every_branch() {
    let plan = optimized(&format!("{YT} SELECT id FROM yt WHERE v > 5")).await;
    let rendered = format!("{plan}");
    assert_eq!(scan_counts(&plan), [1, 1, 1], "{rendered}");
    // Applied inside each branch, not once above the UNION ALL.
    let lines: Vec<&str> = rendered.lines().collect();
    let union_at = lines
        .iter()
        .position(|l| l.trim_start().starts_with("UnionAll"))
        .unwrap_or_else(|| panic!("no UnionAll:\n{rendered}"));
    assert!(
        lines[..union_at].iter().all(|l| !l.contains("v > 5")),
        "filter left above the UNION ALL:\n{rendered}"
    );
    assert_eq!(
        lines[union_at..]
            .iter()
            .filter(|l| l.contains("v > 5"))
            .count(),
        3,
        "{rendered}"
    );
}

#[tokio::test]
async fn constant_and_column_conjuncts_prune_and_push_together() {
    let plan = optimized(&format!("{YT} SELECT id FROM yt WHERE t = 's' AND v > 5")).await;
    let rendered = format!("{plan}");
    assert_eq!(scan_counts(&plan), [1, 0, 0], "{rendered}");
    assert!(rendered.contains("v > 5"), "{rendered}");
}

#[tokio::test]
async fn a_filter_no_branch_can_satisfy_scans_nothing_and_keeps_the_schema() {
    let plan = optimized(&format!("{YT} SELECT id, t FROM yt WHERE t = 'x'")).await;
    assert_eq!(scan_counts(&plan), [0, 0, 0], "{plan}");
    let names: Vec<String> = plan.schema().iter().map(|c| c.name.clone()).collect();
    assert_eq!(names, vec!["id".to_string(), "t".to_string()]);
}

#[tokio::test]
async fn a_null_constant_branch_matches_is_null_and_never_equality() {
    let with_null = "WITH yt AS ( \
        SELECT id, v, 's' AS t FROM s_tab \
        UNION ALL SELECT id, v, CAST(NULL AS VARCHAR) AS t FROM c_tab)";
    let is_null = optimized(&format!("{with_null} SELECT id FROM yt WHERE t IS NULL")).await;
    assert_eq!(scan_counts(&is_null), [0, 1, 0], "{is_null}");
    let equals = optimized(&format!("{with_null} SELECT id FROM yt WHERE t = 's'")).await;
    assert_eq!(scan_counts(&equals), [1, 0, 0], "{equals}");
}
