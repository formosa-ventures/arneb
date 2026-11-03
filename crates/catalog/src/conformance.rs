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

/// The listed names a lookup of `variant` may resolve to: `variant`
/// itself when it is listed (an exact match wins), else every listed name
/// equal to it up to case and surrounding spaces.
fn candidates<'a>(listed: &'a [String], variant: &str) -> Vec<&'a str> {
    if let Some(exact) = listed.iter().find(|n| *n == variant) {
        return vec![exact];
    }
    let key = variant.trim().to_lowercase();
    listed
        .iter()
        .filter(|n| n.trim().to_lowercase() == key)
        .map(String::as_str)
        .collect()
}

/// `try_schema`, failing the check when the source can't be asked: an
/// outage must not pass for "nothing to check".
async fn lookup(
    catalog: &dyn CatalogProvider,
    name: &str,
) -> Result<Option<std::sync::Arc<dyn crate::SchemaProvider>>, String> {
    catalog
        .try_schema(name)
        .await
        .map_err(|e| format!("looking up schema {name:?}: {e}"))
}

/// Looks every schema and table of `catalog` up under other spellings
/// (case variants, surrounding spaces) and fails if one is found without
/// reporting a stored name, or reports a stored name the variant could not
/// stand for (`variant` itself when listed, else a listed name equal to it
/// up to case and spaces, in the schema it was found in). Exact-match
/// connectors pass trivially (every unlisted variant is `None`). Fails when
/// the source can't be asked, including when it lists no schemas.
pub async fn check_name_resolution(catalog: &dyn CatalogProvider) -> Result<(), String> {
    let schemas = catalog.schema_names().await;
    // Listings swallow errors, so an empty one may be an outage: there is
    // nothing to check either way, which must not pass.
    if schemas.is_empty() {
        return Err("catalog lists no schemas (empty, or the source is unreachable)".into());
    }
    for schema in &schemas {
        for variant in variants(schema) {
            if let Some(found) = lookup(catalog, &variant).await? {
                let expected = candidates(&schemas, &variant);
                match found.canonical_name() {
                    Some(stored) if expected.contains(&stored.as_str()) => {}
                    other => {
                        return Err(format!(
                            "schema {schema:?} found as {variant:?} but reports \
                             canonical_name {other:?}, expected one of {expected:?}"
                        ))
                    }
                }
            }
        }
        let Some(provider) = lookup(catalog, schema).await? else {
            // Dropped since it was listed (the source answered "no such
            // schema" while changing under the check) is fine; still
            // listed but not found is not. An outage fails the check.
            if catalog.schema_names().await.contains(schema) {
                return Err(format!("listed schema {schema:?} does not resolve"));
            }
            continue;
        };
        if provider
            .canonical_name()
            .is_some_and(|stored| &stored != schema)
        {
            return Err(format!(
                "listed schema {schema:?} reports canonical_name {:?}",
                provider.canonical_name()
            ));
        }
        let tables = provider.table_names().await;
        for table in &tables {
            for variant in variants(table) {
                let found = provider
                    .table_if_allowed(&variant, &|_, _| true)
                    .await
                    .map_err(|e| format!("looking up table {schema}.{variant:?}: {e}"))?;
                if let Some(found) = found {
                    let expected = candidates(&tables, &variant);
                    match found.canonical_name() {
                        Some((s, t)) if &s == schema && expected.contains(&t.as_str()) => {}
                        other => {
                            return Err(format!(
                                "table {schema}.{table} found as {variant:?} but reports \
                                 canonical_name {other:?}, expected {schema:?} and one of \
                                 {expected:?}"
                            ))
                        }
                    }
                }
            }
        }
    }
    Ok(())
}
