//! The catalog definitions kept in the state store (runtime-config D1, D6):
//! seeded from `[[catalogs]]` on the first start, changed at runtime through
//! the catalog API, and winning over the config file on every later start.
//! A spec is stored as opaque JSON (the server's `CatalogConfig`), so this
//! crate does not depend on the server. Every change bumps the catalog
//! generation, which workers compare to decide whether to pull the set.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::file::{StateError, StateStore};

/// One stored catalog definition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CatalogRecord {
    /// The catalog name (unique in the state).
    pub name: String,
    /// The catalog spec as JSON (the server's serialized `CatalogConfig`).
    /// It holds the raw secrets, so it is never logged.
    pub spec: serde_json::Value,
    /// True when the catalog was seeded from the config file.
    pub from_config: bool,
}

/// What [`StateStore::seed_catalogs`] did or found.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CatalogSeedReport {
    /// The config's catalogs were stored (first start, or a re-seed).
    pub seeded: bool,
    /// Config catalogs absent from the state (not seeded: nothing written).
    pub added: Vec<String>,
    /// Config catalogs whose stored spec differs from the config's.
    pub changed: Vec<String>,
    /// Config-seeded catalogs still in the state but gone from the config.
    pub removed: Vec<String>,
}

impl StateStore {
    /// The catalog generation and the stored catalogs, in name order.
    pub fn catalogs(&self) -> (u64, Vec<CatalogRecord>) {
        let snapshot = self.snapshot();
        (snapshot.catalog_generation, snapshot.catalogs.clone())
    }

    /// Stores `record`, replacing a catalog of the same name. Returns the
    /// new generation.
    pub fn put_catalog(&self, record: CatalogRecord) -> Result<u64, StateError> {
        self.mutate(|d| {
            match d.catalogs.iter_mut().find(|c| c.name == record.name) {
                Some(slot) => *slot = record,
                None => {
                    d.catalogs.push(record);
                    d.catalogs.sort_by(|a, b| a.name.cmp(&b.name));
                }
            }
            Ok(bump(d))
        })
    }

    /// Removes the catalog `name`. Returns the new generation.
    pub fn remove_catalog(&self, name: &str) -> Result<u64, StateError> {
        self.mutate(|d| {
            let before = d.catalogs.len();
            d.catalogs.retain(|c| c.name != name);
            if d.catalogs.len() == before {
                return Err(StateError::CatalogNotFound(name.to_owned()));
            }
            Ok(bump(d))
        })
    }

    /// Replaces every stored catalog with `records` (a later duplicate name
    /// wins). Returns the new generation.
    pub fn replace_catalogs(&self, records: Vec<CatalogRecord>) -> Result<u64, StateError> {
        self.mutate(|d| {
            d.catalogs = sorted_unique(records);
            Ok(bump(d))
        })
    }

    /// Seeds the catalogs from the config's `(name, spec)` list. The first
    /// call on a state whose catalogs were never seeded, or any call with
    /// `reseed`, replaces the stored catalogs with the config's
    /// (`from_config = true`, runtime-created ones removed), bumps the
    /// generation (so a seeded state's generation is at least 1, even with
    /// no catalogs) and reports `seeded`. Otherwise nothing is written and
    /// the report names how the config diverges from the state: specs are
    /// compared by JSON value equality.
    pub fn seed_catalogs(
        &self,
        config: &[(String, serde_json::Value)],
        reseed: bool,
    ) -> Result<CatalogSeedReport, StateError> {
        let snapshot = self.snapshot();
        if snapshot.seeded.catalogs && !reseed {
            let stored: BTreeMap<&str, &CatalogRecord> = snapshot
                .catalogs
                .iter()
                .map(|c| (c.name.as_str(), c))
                .collect();
            let mut report = CatalogSeedReport::default();
            for (name, spec) in config {
                match stored.get(name.as_str()) {
                    None => report.added.push(name.clone()),
                    Some(record) if record.spec != *spec => report.changed.push(name.clone()),
                    Some(_) => {}
                }
            }
            report.removed = snapshot
                .catalogs
                .iter()
                .filter(|c| c.from_config && !config.iter().any(|(n, _)| *n == c.name))
                .map(|c| c.name.clone())
                .collect();
            return Ok(report);
        }
        let records = config
            .iter()
            .map(|(name, spec)| CatalogRecord {
                name: name.clone(),
                spec: spec.clone(),
                from_config: true,
            })
            .collect();
        self.mutate(|d| {
            d.catalogs = sorted_unique(records);
            d.seeded.catalogs = true;
            bump(d);
            Ok(())
        })?;
        Ok(CatalogSeedReport {
            seeded: true,
            ..Default::default()
        })
    }
}

fn bump(d: &mut crate::file::StateData) -> u64 {
    d.catalog_generation += 1;
    d.catalog_generation
}

fn sorted_unique(records: Vec<CatalogRecord>) -> Vec<CatalogRecord> {
    let by_name: BTreeMap<String, CatalogRecord> =
        records.into_iter().map(|r| (r.name.clone(), r)).collect();
    by_name.into_values().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn spec(uri: &str) -> serde_json::Value {
        json!({"name": "x", "type": "hive", "metastore_uri": uri})
    }

    fn names(store: &StateStore) -> Vec<(String, bool)> {
        store
            .catalogs()
            .1
            .into_iter()
            .map(|c| (c.name, c.from_config))
            .collect()
    }

    #[test]
    fn catalog_generation_increments_and_survives_reopen() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("state.json");
        let store = StateStore::open(Some(&p)).unwrap();
        assert_eq!(store.catalogs(), (0, vec![]));
        let rec = |name: &str| CatalogRecord {
            name: name.into(),
            spec: spec("h:9083"),
            from_config: false,
        };
        assert_eq!(store.put_catalog(rec("b")).unwrap(), 1);
        assert_eq!(store.put_catalog(rec("a")).unwrap(), 2);
        assert_eq!(store.put_catalog(rec("a")).unwrap(), 3, "replace bumps too");
        assert!(matches!(
            store.remove_catalog("nope"),
            Err(StateError::CatalogNotFound(n)) if n == "nope"
        ));
        assert_eq!(store.catalogs().0, 3, "a failed remove changes nothing");
        assert_eq!(store.remove_catalog("b").unwrap(), 4);
        drop(store);

        let store = StateStore::open(Some(&p)).unwrap();
        let (generation, records) = store.catalogs();
        assert_eq!(generation, 4);
        assert_eq!(records, vec![rec("a")]);
        assert_eq!(store.replace_catalogs(vec![rec("c"), rec("b")]).unwrap(), 5);
        assert_eq!(
            names(&store),
            vec![("b".into(), false), ("c".into(), false)]
        );
    }

    #[test]
    fn seed_catalogs_first_start_then_reports_divergence_without_writing() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("state.json");
        let store = StateStore::open(Some(&p)).unwrap();
        // A first start with no catalogs still seeds, and the generation is
        // at least 1 (runtime-config Ruling R1).
        let report = store.seed_catalogs(&[], false).unwrap();
        assert!(report.seeded);
        assert!(store.seeded().catalogs);
        assert_eq!(store.catalogs(), (1, vec![]));
        drop(store);

        // A fresh state seeded with the config's catalogs.
        let p2 = tmp.path().join("state2.json");
        let store = StateStore::open(Some(&p2)).unwrap();
        let config = vec![
            ("keep".to_string(), spec("a:1")),
            ("edit".to_string(), spec("b:1")),
            ("gone".to_string(), spec("c:1")),
        ];
        assert!(store.seed_catalogs(&config, false).unwrap().seeded);
        let (generation, _) = store.catalogs();
        assert_eq!(generation, 1);
        assert_eq!(
            names(&store),
            vec![
                ("edit".into(), true),
                ("gone".into(), true),
                ("keep".into(), true)
            ]
        );
        // A runtime-created catalog is not a divergence.
        store
            .put_catalog(CatalogRecord {
                name: "runtime".into(),
                spec: spec("r:1"),
                from_config: false,
            })
            .unwrap();
        drop(store);

        let store = StateStore::open(Some(&p2)).unwrap();
        let before = store.catalogs();
        let later = vec![
            ("keep".to_string(), spec("a:1")),
            ("edit".to_string(), spec("b:2")),
            ("new".to_string(), spec("n:1")),
        ];
        let report = store.seed_catalogs(&later, false).unwrap();
        assert_eq!(
            report,
            CatalogSeedReport {
                seeded: false,
                added: vec!["new".into()],
                changed: vec!["edit".into()],
                removed: vec!["gone".into()],
            }
        );
        assert_eq!(store.catalogs(), before, "nothing is written");
        drop(store);
        let store = StateStore::open(Some(&p2)).unwrap();
        assert_eq!(store.catalogs(), before);
    }

    #[test]
    fn reseed_catalogs_replaces_runtime_ones() {
        let store = StateStore::open(None).unwrap();
        store
            .seed_catalogs(&[("a".to_string(), spec("a:1"))], false)
            .unwrap();
        store
            .put_catalog(CatalogRecord {
                name: "runtime".into(),
                spec: spec("r:1"),
                from_config: false,
            })
            .unwrap();
        let generation = store.catalogs().0;
        let report = store
            .seed_catalogs(&[("b".to_string(), spec("b:1"))], true)
            .unwrap();
        assert!(report.seeded);
        assert_eq!(names(&store), vec![("b".into(), true)]);
        assert_eq!(store.catalogs().0, generation + 1);
    }
}
