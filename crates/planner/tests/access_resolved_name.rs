//! Access rules are checked on the table a connector actually resolved,
//! not only on the name as written: a connector that matches names
//! case-insensitively (PostgreSQL, Hive) must not let `SECRET` read a
//! denied `secret`. Covers every statement that plans a table read.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use arneb_catalog::{
    AccessRules, CatalogManager, CatalogProvider, SchemaProvider, TableLookup, TableProvider,
};
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

/// Counts lookups and full table loads; a connector shaped like Hive's (it
/// learns the stored name cheaply, then loads), so a denied table must
/// never be loaded, including by the planner's case-folding probe.
#[derive(Debug, Default)]
struct Counts {
    probes: AtomicUsize,
    loads: AtomicUsize,
}

#[derive(Debug)]
struct CountingSchema(String, Arc<Counts>);

#[async_trait]
impl SchemaProvider for CountingSchema {
    async fn table_names(&self) -> Vec<String> {
        vec!["open".into(), "secret".into()]
    }
    async fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        self.table_if_allowed(name, &|_, _| true)
            .await
            .ok()?
            .found()
    }
    async fn table_if_allowed(
        &self,
        name: &str,
        allowed: arneb_catalog::TableGate<'_>,
    ) -> Result<TableLookup, arneb_common::error::CatalogError> {
        self.1.probes.fetch_add(1, Ordering::SeqCst);
        let Some(name) = resolve_loosely(&["open", "secret"], name) else {
            return Ok(TableLookup::Missing);
        };
        if !allowed(&self.0, &name) {
            return Ok(TableLookup::Denied);
        }
        self.1.loads.fetch_add(1, Ordering::SeqCst);
        Ok(TableLookup::Found(Arc::new(LooseTable {
            schema: self.0.clone(),
            name,
        })))
    }
    fn canonical_name(&self) -> Option<String> {
        Some(self.0.clone())
    }
}

#[derive(Debug)]
struct CountingCatalog(Arc<Counts>);

#[async_trait]
impl CatalogProvider for CountingCatalog {
    async fn schema_names(&self) -> Vec<String> {
        vec!["public".into()]
    }
    async fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        let name = resolve_loosely(&["public"], name)?;
        Some(Arc::new(CountingSchema(name, Arc::clone(&self.0))))
    }
}

/// `alice` over a [`CountingCatalog`], with the counts it records.
fn counted(rules: &str) -> (CatalogManager, Arc<Counts>) {
    let counts = Arc::new(Counts::default());
    let root = CatalogManager::new("pg", "public");
    root.register_catalog("pg", Arc::new(CountingCatalog(Arc::clone(&counts))));
    let rules = AccessRules::from_json(rules).unwrap();
    (root.with_access("alice", Arc::new(rules)), counts)
}

/// `pg.PUBLIC.SECRET` passes the written-name checks (the rule names
/// `public`), so the planner's case-folding probe runs; it must go
/// through the rules gate and never load the denied `public.secret`.
#[tokio::test]
async fn a_denied_table_is_never_loaded_while_planning() {
    let (alice, counts) = counted(
        r#"{"tables": [
            {"user": "alice", "schema": "public", "table": "secret", "privileges": []},
            {"privileges": ["SELECT"]}
        ]}"#,
    );
    for t in ["pg.PUBLIC.SECRET", "pg.PUBLIC.Secret", "pg.public.SECRET"] {
        assert!(plan(&alice, &format!("SELECT * FROM {t}")).await.is_err());
    }
    assert_eq!(
        counts.loads.load(Ordering::SeqCst),
        0,
        "a denied table was loaded"
    );
    plan(&alice, "SELECT * FROM pg.PUBLIC.OPEN").await.unwrap();
    assert!(counts.loads.load(Ordering::SeqCst) > 0);
}

/// A name denied as written is refused before the connector is asked
/// anything about it, so the refusal takes the same time whether or not
/// the table exists, and costs no table load. The upgrade note's own
/// pattern: allow `open`, deny everything else; `OPEN` is the stored
/// `open` written in a case the rule does not allow.
#[tokio::test]
async fn a_name_denied_as_written_is_never_probed() {
    let (alice, counts) = counted(
        r#"{"tables": [
            {"user": "alice", "schema": "public", "table": "open", "privileges": ["SELECT"]},
            {"privileges": []}
        ]}"#,
    );
    for t in ["pg.public.OPEN", "pg.public.SECRET", "pg.public.NOPE"] {
        let err = plan(&alice, &format!("SELECT * FROM {t}"))
            .await
            .unwrap_err();
        assert!(err.contains("Access Denied"), "{t}: {err}");
    }
    assert_eq!(
        counts.probes.load(Ordering::SeqCst),
        0,
        "a denied name was probed"
    );
    assert_eq!(counts.loads.load(Ordering::SeqCst), 0);
    plan(&alice, "SELECT * FROM pg.public.open").await.unwrap();
    assert!(counts.loads.load(Ordering::SeqCst) > 0);
}
