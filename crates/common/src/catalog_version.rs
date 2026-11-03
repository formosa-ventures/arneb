//! The version of a node's runtime catalogs that a statement is pinned to.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// The runtime catalog set a statement was planned against: the
/// coordinator's `(epoch, generation)`, a fingerprint of each catalog's
/// spec, and the default catalog and schema an unqualified scan resolves
/// in. A worker runs a scan of catalog `c` only on a copy of `c` whose
/// fingerprint matches, so a change to another catalog never fails it.
///
/// The fingerprints are derived from secret-bearing JSON (each spec's raw
/// password included) with an unkeyed fast hash, so anyone who can see a
/// redacted spec could test password guesses against one offline. They
/// must never be logged or shown to a client: the value only travels
/// between nodes (inside a `TaskDescriptor`, under the cluster security),
/// and `Debug` prints the catalog names without their fingerprints.
#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogVersion {
    /// The epoch of the coordinator the set comes from.
    pub epoch: u64,
    /// The set's catalog generation.
    pub generation: u64,
    /// [`spec_fingerprint`] of each runtime catalog's spec, by name.
    /// Catalogs outside the runtime set (a node's own file tables) have
    /// none.
    pub catalogs: BTreeMap<String, u64>,
    /// The catalog a scan that names none resolves in.
    pub default_catalog: String,
    /// The schema a scan that names none resolves in.
    pub default_schema: String,
}

impl std::fmt::Debug for CatalogVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CatalogVersion")
            .field("epoch", &self.epoch)
            .field("generation", &self.generation)
            .field("catalogs", &self.catalogs.keys().collect::<Vec<_>>())
            .field("default_catalog", &self.default_catalog)
            .field("default_schema", &self.default_schema)
            .finish()
    }
}

impl CatalogVersion {
    /// The fingerprint of catalog `name`, if it is a runtime catalog of the
    /// set.
    pub fn catalog(&self, name: &str) -> Option<u64> {
        self.catalogs.get(name).copied()
    }
}

/// A fingerprint of a catalog spec's JSON text: 64-bit FNV-1a, which is
/// the same on every node, platform and build (unlike `std`'s hasher), so
/// a coordinator and a worker agree on it. The input holds raw secrets and
/// the hash is unkeyed: never log or expose the result (see
/// [`CatalogVersion`]).
pub fn spec_fingerprint(json: &str) -> u64 {
    json.bytes().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_never_prints_a_fingerprint() {
        let v = CatalogVersion {
            epoch: 3,
            generation: 7,
            catalogs: [("pg".to_owned(), 0x1234_5678_9abc_def0)].into(),
            default_catalog: "pg".into(),
            default_schema: "public".into(),
        };
        let shown = format!("{v:?}");
        assert!(
            shown.contains("\"pg\"") && shown.contains("generation: 7"),
            "{shown}"
        );
        assert!(
            !shown.contains(&0x1234_5678_9abc_def0_u64.to_string()) && !shown.contains("9abc"),
            "{shown}"
        );
    }

    #[test]
    fn fingerprint_is_fnv1a() {
        assert_eq!(spec_fingerprint(""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(spec_fingerprint("a"), 0xaf63_dc4c_8601_ec8c);
        assert_ne!(spec_fingerprint("{\"a\":1}"), spec_fingerprint("{\"a\":2}"));
    }
}
