//! Against a real HMS: a database requested under any spelling HMS
//! normalizes (`TPCH`, `"tpch "`, `@hive#tpch` for the default catalog) is
//! served under the name HMS returns, so access rules are checked on that
//! name; an unknown one, or another catalog's, is missing. Read-only.
//!
//! Requires `docker compose up -d` and the TPC-H seed (HMS on :9083):
//! `cargo test -p arneb-hive --test hms_name_resolution -- --ignored`

use std::sync::Arc;

use arneb_catalog::{CatalogProvider, TableLookup};
use arneb_connectors::storage::StorageRegistry;
use arneb_hive::catalog::{HiveCatalogProvider, HmsClient};

#[tokio::test]
#[ignore]
async fn schemas_are_served_under_the_name_hms_returns() {
    let client = Arc::new(HmsClient::new("127.0.0.1:9083").await.unwrap());
    let catalog = HiveCatalogProvider::new(client, Arc::new(StorageRegistry::new()));
    for written in ["tpch", "TPCH", "tpch ", " tpch", "@hive#tpch"] {
        let schema = catalog
            .schema(written)
            .await
            .unwrap_or_else(|| panic!("{written:?}"));
        assert_eq!(
            schema.canonical_name().as_deref(),
            Some("tpch"),
            "{written:?}"
        );
    }
    assert!(catalog
        .try_schema("no_such_database_xyz")
        .await
        .unwrap()
        .is_none());
    // Another HMS catalog's database is not served from the default one.
    assert!(catalog
        .try_schema("@no_such_catalog#tpch")
        .await
        .unwrap()
        .is_none());
    // A malformed catalog-qualified name names nothing.
    for malformed in ["@x", "@", "@#"] {
        assert!(
            catalog.try_schema(malformed).await.unwrap().is_none(),
            "{malformed:?}"
        );
    }
    // A table HMS knows, and one it doesn't, under the stored schema.
    let tpch = catalog.try_schema("tpch").await.unwrap().unwrap();
    assert!(matches!(
        tpch.table_if_allowed("NATION", &|_, _| true).await.unwrap(),
        TableLookup::Found(_)
    ));
    assert!(matches!(
        tpch.table_if_allowed("no_such_table_xyz", &|_, _| true)
            .await
            .unwrap(),
        TableLookup::Missing
    ));
    // Refused by the gate: not loaded, and denied rather than missing.
    assert!(matches!(
        tpch.table_if_allowed("nation", &|_, _| false)
            .await
            .unwrap(),
        TableLookup::Denied
    ));
    arneb_catalog::conformance::check_name_resolution(&catalog)
        .await
        .unwrap();
}
