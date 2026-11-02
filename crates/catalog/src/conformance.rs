//! Checks a connector's catalog against the contract the access rules
//! rely on: a schema or table found under a name other than its stored
//! one reports the stored name (`canonical_name`). Connector tests call
//! [`check_name_resolution`]; a connector that resolves names loosely and
//! forgets `canonical_name` fails it.

use crate::CatalogProvider;

/// Other spellings of `name` a loose connector might accept.
fn variants(name: &str) -> Vec<String> {
    let swapped: String = name
        .chars()
        .map(|c| {
            if c.is_uppercase() {
                c.to_lowercase().next().unwrap_or(c)
            } else {
                c.to_uppercase().next().unwrap_or(c)
            }
        })
        .collect();
    let mut out = vec![
        name.to_uppercase(),
        name.to_lowercase(),
        swapped,
        format!("{name} "),
        format!(" {name}"),
    ];
    out.retain(|v| v != name);
    out.dedup();
    out
}

/// Looks every schema and table of `catalog` up under other spellings
/// (case variants, surrounding spaces) and fails if one is found without
/// reporting a stored name, or reports a name the catalog does not list.
/// Exact-match connectors pass trivially (every variant is `None`).
pub async fn check_name_resolution(catalog: &dyn CatalogProvider) -> Result<(), String> {
    let schemas = catalog.schema_names().await;
    for schema in &schemas {
        for variant in variants(schema) {
            if let Some(found) = catalog.schema(&variant).await {
                match found.canonical_name() {
                    Some(stored) if schemas.contains(&stored) => {}
                    other => {
                        return Err(format!(
                            "schema {schema:?} found as {variant:?} but reports \
                             canonical_name {other:?}"
                        ))
                    }
                }
            }
        }
        let Some(provider) = catalog.schema(schema).await else {
            return Err(format!("listed schema {schema:?} does not resolve"));
        };
        let tables = provider.table_names().await;
        for table in &tables {
            for variant in variants(table) {
                if let Some(found) = provider.table(&variant).await {
                    match found.canonical_name() {
                        Some((s, t)) if &s == schema && tables.contains(&t) => {}
                        other => {
                            return Err(format!(
                                "table {schema}.{table} found as {variant:?} but reports \
                                 canonical_name {other:?}"
                            ))
                        }
                    }
                }
            }
        }
    }
    Ok(())
}
