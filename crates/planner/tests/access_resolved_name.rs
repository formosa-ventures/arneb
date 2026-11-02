//! Access rules are checked on the table a connector actually resolved,
//! not only on the name as written: a connector that matches names
//! case-insensitively (PostgreSQL, Hive) must not let `SECRET` read a
//! denied `secret`. Covers every statement that plans a table read.

use std::sync::Arc;

use arneb_catalog::{AccessRules, CatalogManager, CatalogProvider, SchemaProvider, TableProvider};
use arneb_common::types::{ColumnInfo, DataType};
use arneb_planner::QueryPlanner;
use async_trait::async_trait;

fn resolve_loosely(names: &[&str], wanted: &str) -> Option<String> {
    if names.contains(&wanted) {
        return Some(wanted.to_string());
    }
    let mut found = names.iter().filter(|n| n.eq_ignore_ascii_case(wanted));
    let first = found.next()?;
    found.next().is_none().then(|| first.to_string())
}

#[derive(Debug)]
struct LooseTable {
    schema: String,
    name: String,
}

impl TableProvider for LooseTable {
    fn schema(&self) -> Vec<ColumnInfo> {
        vec![ColumnInfo {
            name: "x".into(),
            data_type: DataType::Int64,
            nullable: false,
        }]
    }
    fn canonical_name(&self) -> Option<(String, String)> {
        Some((self.schema.clone(), self.name.clone()))
    }
}

#[derive(Debug)]
struct LooseSchema(String);

#[async_trait]
impl SchemaProvider for LooseSchema {
    async fn table_names(&self) -> Vec<String> {
        vec!["open".into(), "secret".into()]
    }
    async fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        let name = resolve_loosely(&["open", "secret"], name)?;
        Some(Arc::new(LooseTable {
            schema: self.0.clone(),
            name,
        }))
    }
    fn canonical_name(&self) -> Option<String> {
        Some(self.0.clone())
    }
}

/// `pg.public.{open,secret}`, resolved as PostgreSQL resolves names.
#[derive(Debug)]
struct LooseCatalog;

#[async_trait]
impl CatalogProvider for LooseCatalog {
    async fn schema_names(&self) -> Vec<String> {
        vec!["public".into()]
    }
    async fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        let name = resolve_loosely(&["public"], name)?;
        Some(Arc::new(LooseSchema(name)))
    }
}

fn alice() -> CatalogManager {
    let root = CatalogManager::new("pg", "public");
    root.register_catalog("pg", Arc::new(LooseCatalog));
    let rules = AccessRules::from_json(
        r#"{"tables": [
            {"user": "alice", "schema": "public", "table": "secret", "privileges": []},
            {"privileges": ["SELECT"]}
        ]}"#,
    )
    .unwrap();
    root.with_access("alice", Arc::new(rules))
}

async fn plan(catalog: &CatalogManager, sql: &str) -> Result<(), String> {
    let stmt = arneb_sql_parser::parse(sql).expect("parse");
    QueryPlanner::new(catalog)
        .plan_statement(&stmt)
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// Statement templates; `{t}` is the table read.
const STATEMENTS: &[&str] = &[
    "SELECT * FROM {t}",
    "SELECT x FROM pg.public.open WHERE x IN (SELECT x FROM {t})",
    "SELECT * FROM pg.public.open o JOIN {t} s ON o.x = s.x",
    "WITH c AS (SELECT * FROM {t}) SELECT * FROM c",
    "EXPLAIN SELECT * FROM {t}",
    "INSERT INTO pg.public.open SELECT * FROM {t}",
    "CREATE TABLE pg.public.copy AS SELECT * FROM {t}",
    "CREATE VIEW pg.public.v AS SELECT * FROM {t}",
];

#[tokio::test]
async fn every_read_checks_the_name_the_connector_resolved() {
    let alice = alice();
    for template in STATEMENTS {
        let sql = |t: &str| template.replace("{t}", t);
        let denied = plan(&alice, &sql("pg.public.secret")).await.unwrap_err();
        assert!(denied.contains("Access Denied"), "{template}: {denied}");
        for t in ["pg.public.SECRET", "pg.PUBLIC.secret", "pg.Public.Secret"] {
            let err = plan(&alice, &sql(t))
                .await
                .expect_err(&format!("{template} via {t} must be denied"));
            // Exactly what a table that does not exist gets.
            let missing = plan(&alice, &sql("pg.public.NOPE")).await.unwrap_err();
            assert_eq!(
                err,
                missing.replace("pg.public.NOPE", t),
                "{template} via {t}"
            );
        }
        for t in ["pg.public.open", "pg.PUBLIC.OPEN"] {
            plan(&alice, &sql(t))
                .await
                .unwrap_or_else(|e| panic!("{template} via {t}: {e}"));
        }
    }
}
