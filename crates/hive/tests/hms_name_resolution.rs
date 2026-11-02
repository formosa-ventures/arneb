//! Against a real HMS: a database requested under any spelling HMS
//! normalizes (`TPCH`, `"tpch "`, `@hive#tpch`) is served under the name
//! HMS returns, so access rules are checked on that name; an unknown one
//! is missing. Read-only.
//!
//! Requires `docker compose up -d` and the TPC-H seed (HMS on :9083):
//! `cargo test -p arneb-hive --test hms_name_resolution -- --ignored`

use std::sync::Arc;

use arneb_catalog::CatalogProvider;
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
    assert!(catalog.schema("no_such_database_xyz").await.is_none());
    arneb_catalog::conformance::check_name_resolution(&catalog)
        .await
        .unwrap();
}
