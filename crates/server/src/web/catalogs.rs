//! Catalog definitions on `/api/v2/catalog-configs` (runtime-config D6, D7):
//! list, add, change and drop the external catalogs (`hive`, `postgresql`)
//! at runtime. Every route needs manage. The `/api/v2/catalogs` discovery
//! routes are separate and unchanged; they show a new catalog as soon as
//! the change returns.
//!
//! - A create or update first builds the catalog on the coordinator (it
//!   connects to the metastore or database, at most 10 s), outside the
//!   change lock and for a dry run too. A failure is `invalid_argument` and
//!   changes nothing.
//! - Under `WebState::config_changes` (D10) the change re-checks that what
//!   it probed is still current (`conflict` otherwise), persists the spec
//!   (generation + 1), installs the probed catalog on the coordinator and
//!   pushes the new set to every live worker, in parallel, 5 s each. A
//!   failed push is reported, not an error: the worker's heartbeat pull
//!   repairs it (D8).
//! - Secrets never leave redacted: responses and audit entries carry
//!   [`redacted_spec`], and a `PUT` of a redacted value keeps the stored
//!   secret ([`fill_redacted`]).

use std::sync::Arc;
use std::time::Duration;

use arneb_state::{CatalogRecord, StateError, StateStore};
use axum::body::Bytes;
use axum::extract::rejection::{BytesRejection, QueryRejection};
use axum::extract::{Path, Query, State};
use axum::response::Json;
use axum::routing::get;
use axum::{Extension, Router};
use serde::{Deserialize, Serialize};

use super::auth::Identity;
use super::error::{ApiError, ErrorCode};
use super::manage::{
    add_warning, check_limit, config_change_lock, live_catalogs, plural, run_manage, state_store,
    Applied, ChangeLock, DryRun, ManageAction, ManageOutcome, Preview,
};
use super::privilege::Layer;
use super::v2::{bad_body, bad_query, state_error};
use super::WebState;
use crate::catalogs::{
    fill_redacted, redacted_secrets, redacted_spec, same_destination, same_spec, validate_spec,
    validate_spec_fields, BuiltCatalog, LiveCatalogs, REDACTED,
};
use crate::config::CatalogConfig;

/// Catalogs listed when the request names no `limit`.
const DEFAULT_LIMIT: usize = 100;
/// How long a push to one worker may take.
const PUSH_TIMEOUT: Duration = Duration::from_secs(5);

/// One catalog definition, secrets redacted.
#[derive(Debug, Serialize)]
pub(super) struct CatalogConfigView {
    name: String,
    /// Whether the config file manages (seeded) the catalog.
    from_config: bool,
    /// Whether it is the default catalog (unqualified table names resolve
    /// in it).
    default: bool,
    /// The catalog generation the definitions belong to.
    generation: u64,
    /// The spec, as `[[catalogs]]` fields, with every secret redacted.
    spec: serde_json::Value,
}

/// A bounded page of catalog definitions, in name order.
#[derive(Debug, Serialize)]
pub(super) struct CatalogConfigList {
    generation: u64,
    catalogs: Vec<CatalogConfigView>,
    truncated: bool,
}

/// The result of a create, update or drop.
#[derive(Debug, Serialize)]
pub(super) struct CatalogChange {
    name: String,
    /// The catalog generation after the change.
    generation: u64,
    /// Every live worker the new set was pushed to, by id.
    workers: Vec<WorkerSync>,
}

/// One worker's answer to a push.
#[derive(Debug, Serialize)]
pub(super) struct WorkerSync {
    worker_id: String,
    /// The worker applied the new set.
    synced: bool,
    /// Why the push failed (`synced` is false).
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    /// Catalogs of the set the worker applied but could not build (it
    /// retries them on its heartbeat; Ruling R7).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    failed_catalogs: Vec<String>,
}

fn find_record(store: &StateStore, name: &str) -> Option<CatalogRecord> {
    store.catalogs().1.into_iter().find(|r| r.name == name)
}

fn existing_record(store: &StateStore, name: &str) -> Result<CatalogRecord, ApiError> {
    find_record(store, name).ok_or_else(|| no_such_catalog(name))
}

fn no_such_catalog(name: &str) -> ApiError {
    ApiError::new(
        ErrorCode::NotFound,
        format!("no catalog definition named {name:?}"),
    )
    .with_hint(
        "list them with GET /api/v2/catalog-configs; the memory and file catalogs are \
         startup-only",
    )
}

fn catalog_exists(name: &str) -> ApiError {
    ApiError::new(
        ErrorCode::Conflict,
        format!("catalog {name:?} already exists"),
    )
    .with_hint(format!(
        "pick another name, or change it with PUT /api/v2/catalog-configs/{name}"
    ))
}

fn changed_since_probe(name: &str) -> ApiError {
    ApiError::new(
        ErrorCode::Conflict,
        format!("catalog {name:?} was changed by another call while this one connected to it"),
    )
    .with_hint("retry")
}

/// [`StateError`]s of the catalog operations as API errors.
fn catalog_error(e: StateError) -> ApiError {
    match e {
        StateError::CatalogNotFound(name) => no_such_catalog(&name),
        other => state_error(other),
    }
}

/// A stored spec, decoded. `None` (with a WARN naming the catalog only)
/// when it does not decode.
fn decode(record: &CatalogRecord) -> Option<CatalogConfig> {
    match serde_json::from_value(record.spec.clone()) {
        Ok(spec) => Some(spec),
        Err(e) => {
            // Only the error's category: its text may quote a value.
            tracing::warn!(
                catalog = %record.name,
                error = ?e.classify(),
                "stored catalog definition is invalid"
            );
            None
        }
    }
}

fn view(record: &CatalogRecord, generation: u64, default_catalog: &str) -> CatalogConfigView {
    CatalogConfigView {
        name: record.name.clone(),
        from_config: record.from_config,
        default: record.name == default_catalog,
        generation,
        // An undecodable spec is never echoed: it may hold a secret.
        spec: decode(record)
            .map(|spec| redacted_spec(&spec))
            .unwrap_or(serde_json::Value::Null),
    }
}

/// A request spec as a [`CatalogConfig`]. With `path_name`, a missing
/// `name` is taken from the path and a different one is refused. The
/// decoder's message is kept only when it names a field, since it can
/// otherwise quote a value.
fn decode_spec(
    mut spec: serde_json::Value,
    path_name: Option<&str>,
) -> Result<CatalogConfig, ApiError> {
    let Some(fields) = spec.as_object_mut() else {
        return Err(
            ApiError::new(ErrorCode::InvalidArgument, "invalid catalog spec").with_hint(
                "send a JSON object with the fields of a [[catalogs]] entry (name, type, ...)",
            ),
        );
    };
    if let Some(path_name) = path_name {
        match fields.get("name") {
            None => {
                fields.insert("name".into(), path_name.into());
            }
            Some(name) if name.as_str() == Some(path_name) => {}
            Some(_) => {
                return Err(ApiError::new(
                    ErrorCode::InvalidArgument,
                    format!("the spec names another catalog than {path_name:?}"),
                )
                .with_hint("a catalog cannot be renamed: create the new name and drop the old"))
            }
        }
    }
    let hint = "send the fields of a [[catalogs]] entry (name, type, ...)";
    let decoded: CatalogConfig = serde_json::from_value(spec.clone()).map_err(|e| {
        let text = e.to_string();
        let message = if text.starts_with("missing field") || text.starts_with("unknown field") {
            format!("invalid catalog spec: {text}")
        } else {
            "invalid catalog spec: a field has the wrong type".to_owned()
        };
        ApiError::new(ErrorCode::InvalidArgument, message).with_hint(hint)
    })?;
    // Ruling R13: API and MCP input refuses a field the spec does not have
    // (a typo would otherwise be dropped silently), at any depth. The TOML
    // config and the state file stay lenient. Every field of the decoded
    // spec serializes (`None` as null), so a key missing from its JSON form
    // is unknown.
    let known = serde_json::to_value(&decoded).map_err(|_| {
        ApiError::new(ErrorCode::InvalidArgument, "invalid catalog spec").with_hint(hint)
    })?;
    if let Some(field) = unknown_field(&spec, &known, "") {
        return Err(ApiError::new(
            ErrorCode::InvalidArgument,
            format!("invalid catalog spec: unknown field `{field}`"),
        )
        .with_hint(hint));
    }
    Ok(decoded)
}

/// The dotted path of the first key of `input` that `known` lacks, looking
/// into nested objects both have.
fn unknown_field(
    input: &serde_json::Value,
    known: &serde_json::Value,
    prefix: &str,
) -> Option<String> {
    let (Some(input), Some(known)) = (input.as_object(), known.as_object()) else {
        return None;
    };
    for (key, value) in input {
        let path = if prefix.is_empty() {
            key.clone()
        } else {
            format!("{prefix}.{key}")
        };
        match known.get(key) {
            None => return Some(path),
            Some(k) => {
                if let Some(found) = unknown_field(value, k, &path) {
                    return Some(found);
                }
            }
        }
    }
    None
}

/// Refuses a spec that still carries the `"<redacted>"` placeholder as a
/// secret: it would be stored and pushed as the real value.
fn no_placeholders(spec: &CatalogConfig, hint: &str) -> Result<(), ApiError> {
    let left = redacted_secrets(spec);
    if left.is_empty() {
        return Ok(());
    }
    Err(ApiError::new(
        ErrorCode::InvalidArgument,
        format!("{} {REDACTED:?}, not a secret", left.join(", ")),
    )
    .with_hint(hint.to_owned()))
}

/// The hint of a placeholder that cannot be filled.
const SEND_SECRETS: &str = "send the real secrets";
/// The hint of a placeholder for a changed destination (Ruling R10).
const SEND_AGAIN: &str = "send the secret again when you change where it is sent";

/// Checks a new catalog's spec, its name included.
fn validate(spec: &CatalogConfig) -> Result<(), ApiError> {
    validate_spec(spec).map_err(|e| ApiError::new(ErrorCode::InvalidArgument, e))
}

/// Checks an existing catalog's new spec: the name already exists, so it
/// is not held to the name rule (a config-seeded `pg-prod` stays
/// updatable).
fn validate_update(spec: &CatalogConfig) -> Result<(), ApiError> {
    validate_spec_fields(spec).map_err(|e| ApiError::new(ErrorCode::InvalidArgument, e))
}

/// Refuses `spec` when it carries a secret and this coordinator serves its
/// catalogs over Flight without a `[cluster] secret`: any peer that reaches
/// the RPC port could read it with `get_catalogs`. Shared with the re-seed.
pub(super) fn check_exposure(live: &LiveCatalogs, spec: &CatalogConfig) -> Result<(), ApiError> {
    live.exposure_check(spec).map_err(|fields| {
        ApiError::new(
            ErrorCode::InvalidArgument,
            format!(
                "catalog {:?} carries {}, but this coordinator serves catalog definitions \
                 over cluster RPC without a [cluster] secret, so any peer could read them",
                spec.name,
                fields.join(", ")
            ),
        )
        .with_hint(
            "set [cluster] secret (or ARNEB_CLUSTER_SECRET) on every node and restart them, \
             or leave the secret out of the catalog",
        )
    })
}

/// Builds `spec` on the coordinator (connects to it, at most 10 s).
async fn probe(live: &LiveCatalogs, spec: &CatalogConfig) -> Result<BuiltCatalog, ApiError> {
    live.build(spec).await.map_err(|e| {
        ApiError::new(
            ErrorCode::InvalidArgument,
            format!("catalog {:?} could not be built", spec.name),
        )
        .with_hint(format!("the coordinator could not connect: {e}"))
    })
}

/// Every catalog definition, in name order, at most `limit` (default 100,
/// at most 1000), secrets redacted. Needs manage. Shared with the MCP
/// `list_catalog_configs` tool.
pub(super) async fn list_catalog_configs_for(
    state: &WebState,
    who: &Identity,
    limit: Option<usize>,
) -> Result<CatalogConfigList, ApiError> {
    state.authorizer.check(who, Layer::Manage)?;
    let limit = check_limit(limit, DEFAULT_LIMIT)?;
    let default_catalog = live_catalogs(state)?.default_catalog();
    let (generation, records) = state_store(state)?.catalogs();
    let truncated = records.len() > limit;
    Ok(CatalogConfigList {
        generation,
        catalogs: records
            .iter()
            .take(limit)
            .map(|r| view(r, generation, &default_catalog))
            .collect(),
        truncated,
    })
}

/// Catalog definition `name`, secrets redacted. Needs manage. Shared with
/// the MCP `get_catalog_config` tool.
pub(super) async fn get_catalog_config_for(
    state: &WebState,
    who: &Identity,
    name: String,
) -> Result<CatalogConfigView, ApiError> {
    state.authorizer.check(who, Layer::Manage)?;
    let default_catalog = live_catalogs(state)?.default_catalog();
    let (generation, records) = state_store(state)?.catalogs();
    records
        .iter()
        .find(|r| r.name == name)
        .map(|r| view(r, generation, &default_catalog))
        .ok_or_else(|| no_such_catalog(&name))
}

/// Adds the catalog `spec` describes for `who`, who needs manage (also for
/// a dry run): connects to it from the coordinator first, then persists it
/// and pushes it to every live worker. Shared with the MCP
/// `create_catalog` tool.
pub(super) async fn create_catalog_for(
    state: &WebState,
    who: &Identity,
    dry_run: bool,
    spec: serde_json::Value,
) -> Result<ManageOutcome<CatalogChange>, ApiError> {
    let grant = state.authorizer.check(who, Layer::Manage)?;
    let spec = decode_spec(spec, None)?;
    // A "<redacted>" value is kept only by a PUT of the same catalog.
    no_placeholders(
        &spec,
        &format!("{SEND_SECRETS}: a {REDACTED:?} value is kept only by a PUT of the same catalog"),
    )?;
    validate(&spec)?;
    let (store, live) = (state_store(state)?, live_catalogs(state)?);
    check_exposure(&live, &spec)?;
    // Cheap pre-check, so a taken name never pays for a connection; the
    // action checks again under the lock.
    if find_record(&store, &spec.name).is_some() {
        return Err(catalog_exists(&spec.name));
    }
    let built = probe(&live, &spec).await?;
    #[cfg(test)]
    test_hooks::after_probe(state);
    let lock = if dry_run {
        None
    } else {
        Some(config_change_lock(state, who).await?)
    };
    let action = ChangeCatalog {
        name: spec.name.clone(),
        kind: Kind::Create { spec, built },
        store,
        live,
        lock,
    };
    run_manage(state, who, grant, dry_run, action).await
}

/// Replaces catalog `name` with `spec` for `who`, who needs manage (also
/// for a dry run). A secret sent as `"<redacted>"` keeps the stored one, so
/// a `GET` body can be sent back changed. Connects to the new spec from
/// the coordinator first. A config-seeded catalog becomes a runtime one.
/// Shared with the MCP `update_catalog` tool.
pub(super) async fn update_catalog_for(
    state: &WebState,
    who: &Identity,
    dry_run: bool,
    name: String,
    spec: serde_json::Value,
) -> Result<ManageOutcome<CatalogChange>, ApiError> {
    let grant = state.authorizer.check(who, Layer::Manage)?;
    let (store, live) = (state_store(state)?, live_catalogs(state)?);
    let stored = existing_record(&store, &name)?;
    let mut spec = decode_spec(spec, Some(&name))?;
    match decode(&stored) {
        Some(stored_spec) => {
            // Ruling R10: a kept secret is never sent somewhere new.
            if !redacted_secrets(&spec).is_empty() && !same_destination(&spec, &stored_spec) {
                no_placeholders(&spec, SEND_AGAIN)?;
            }
            fill_redacted(&mut spec, &stored_spec);
        }
        // Nothing to fill from.
        None => no_placeholders(&spec, SEND_SECRETS)?,
    }
    no_placeholders(&spec, SEND_SECRETS)?;
    validate_update(&spec)?;
    check_exposure(&live, &spec)?;
    let built = probe(&live, &spec).await?;
    #[cfg(test)]
    test_hooks::after_probe(state);
    let lock = if dry_run {
        None
    } else {
        Some(config_change_lock(state, who).await?)
    };
    let action = ChangeCatalog {
        name,
        kind: Kind::Update {
            spec,
            built,
            probed: stored,
        },
        store,
        live,
        lock,
    };
    run_manage(state, who, grant, dry_run, action).await
}

/// Drops catalog `name` everywhere for `who`, who needs manage (also for a
/// dry run). Dropping the default catalog is allowed, with a warning.
/// Shared with the MCP `drop_catalog` tool.
pub(super) async fn drop_catalog_for(
    state: &WebState,
    who: &Identity,
    dry_run: bool,
    name: String,
) -> Result<ManageOutcome<CatalogChange>, ApiError> {
    let grant = state.authorizer.check(who, Layer::Manage)?;
    let (store, live) = (state_store(state)?, live_catalogs(state)?);
    existing_record(&store, &name)?;
    let lock = if dry_run {
        None
    } else {
        Some(config_change_lock(state, who).await?)
    };
    let action = ChangeCatalog {
        name,
        kind: Kind::Drop,
        store,
        live,
        lock,
    };
    run_manage(state, who, grant, dry_run, action).await
}

/// The `warning` of an update of a config-seeded catalog (Ruling R9).
const DETACH_WARNING: &str = "the config file no longer manages this catalog: its \
     [[catalogs]] entry stops applying, and every start logs a WARN while the entry differs";

enum Kind {
    Create {
        spec: CatalogConfig,
        built: BuiltCatalog,
    },
    Update {
        spec: CatalogConfig,
        built: BuiltCatalog,
        /// The stored record the probe was made against.
        probed: CatalogRecord,
    },
    Drop,
}

struct ChangeCatalog {
    name: String,
    kind: Kind,
    store: Arc<StateStore>,
    live: Arc<LiveCatalogs>,
    lock: Option<ChangeLock>,
}

impl ChangeCatalog {
    /// What a worker count reads like in a summary.
    fn workers(state: &WebState) -> String {
        plural(state.node_registry.alive_workers().len(), "live worker")
    }
}

impl ManageAction for ChangeCatalog {
    type Output = CatalogChange;

    fn action(&self) -> &'static str {
        match self.kind {
            Kind::Create { .. } => "catalog.create",
            Kind::Update { .. } => "catalog.update",
            Kind::Drop => "catalog.drop",
        }
    }

    fn target(&self) -> String {
        self.name.clone()
    }

    /// The spec in its redacted form only.
    fn parameters(&self) -> serde_json::Value {
        match &self.kind {
            Kind::Create { spec, .. } | Kind::Update { spec, .. } => redacted_spec(spec),
            Kind::Drop => serde_json::json!({ "name": self.name }),
        }
    }

    fn persisted(&self) -> bool {
        true
    }

    /// Re-checks what the probe saw: a create's name is still free, an
    /// update's stored spec is unchanged.
    fn preview(&self, state: &WebState) -> Result<Preview, ApiError> {
        let current = find_record(&self.store, &self.name);
        let workers = Self::workers(state);
        let name = &self.name;
        let mut warning = None;
        let changed = true;
        let summary = match &self.kind {
            Kind::Create { spec, .. } => {
                if current.is_some() {
                    return Err(catalog_exists(name));
                }
                format!(
                    "adds {} catalog {name:?} (the coordinator connected to it) and pushes it \
                     to {workers}; queries can use it as soon as this call returns",
                    spec.catalog_type
                )
            }
            Kind::Update { spec, probed, .. } => {
                let current = current.ok_or_else(|| no_such_catalog(name))?;
                if current != *probed {
                    return Err(changed_since_probe(name));
                }
                // The stored runtime spec, already served: nothing to do. A
                // config-seeded one still detaches (Ruling R9), and one not
                // served (its build failed) is installed.
                let served = self
                    .live
                    .specs()
                    .iter()
                    .any(|s| s.name == *name && same_spec(s, spec));
                if !current.from_config
                    && served
                    && serde_json::to_value(spec).ok() == Some(current.spec.clone())
                {
                    return Ok(Preview {
                        summary: format!(
                            "catalog {name:?} already has this spec; nothing to change"
                        ),
                        changed: false,
                        warning: None,
                    });
                }
                if current.from_config {
                    add_warning(&mut warning, DETACH_WARNING.to_owned());
                }
                format!(
                    "replaces {} catalog {name:?} (the coordinator connected to the new spec) \
                     and pushes it to {workers}; running queries keep the catalog they were \
                     planned with",
                    spec.catalog_type
                )
            }
            Kind::Drop => {
                let current = current.ok_or_else(|| no_such_catalog(name))?;
                if *name == self.live.default_catalog() {
                    add_warning(
                        &mut warning,
                        format!(
                            "{name:?} is the default catalog: unqualified table names fail \
                             once it is dropped"
                        ),
                    );
                }
                if current.from_config {
                    add_warning(
                        &mut warning,
                        format!(
                            "catalog {name:?} is in the config file: every start logs a WARN \
                             until it is removed there or the state is re-seed \
                             (POST /api/v2/state/reseed)"
                        ),
                    );
                }
                format!(
                    "removes catalog {name:?} from the coordinator and {workers}; running \
                     queries keep the catalog they were planned with"
                )
            }
        };
        Ok(Preview {
            summary,
            changed,
            warning,
        })
    }

    fn change_lock(&self) -> Option<ChangeLock> {
        self.lock.clone()
    }

    /// Persists, installs on the coordinator, then pushes to the workers.
    /// An update to the spec already stored and served changes nothing: no
    /// generation bump, no push.
    fn apply(self, state: &WebState) -> Result<Applied<CatalogChange>, ApiError> {
        let mut preview = self.preview(state)?;
        let (action, name) = (self.action(), self.name);
        if !preview.changed {
            return Ok(Applied {
                preview,
                result: CatalogChange {
                    name,
                    generation: self.store.catalogs().0,
                    workers: Vec::new(),
                },
            });
        }
        let generation = match self.kind {
            Kind::Create { spec, built } | Kind::Update { spec, built, .. } => {
                let record = CatalogRecord {
                    name: name.clone(),
                    spec: serde_json::to_value(&spec).map_err(|e| {
                        tracing::error!(catalog = %name, error = ?e.classify(), "cannot serialize a catalog spec");
                        ApiError::new(ErrorCode::Internal, "the catalog spec could not be stored")
                    })?,
                    from_config: false,
                };
                let generation = self.store.put_catalog(record).map_err(catalog_error)?;
                self.live.install(spec, built);
                generation
            }
            Kind::Drop => {
                let generation = self.store.remove_catalog(&name).map_err(catalog_error)?;
                self.live.remove(&name);
                generation
            }
        };
        self.live.set_generation(generation);
        let workers = push_to_workers(state, &self.live.snapshot());
        for note in push_warnings(&workers) {
            add_warning(&mut preview.warning, note);
        }
        tracing::info!(
            target: "arneb::config",
            catalog = %name,
            action,
            generation,
            workers = workers.len(),
            unsynced = workers.iter().filter(|w| !w.synced).count(),
            "catalog changed at runtime"
        );
        Ok(Applied {
            preview,
            result: CatalogChange {
                name,
                generation,
                workers,
            },
        })
    }
}

/// The `warning` notes of a push (Ruling R7): the workers that did not
/// confirm it, and the catalogs a worker applied but could not build.
pub(super) fn push_warnings(workers: &[WorkerSync]) -> Vec<String> {
    let mut notes = Vec::new();
    let unsynced: Vec<String> = workers
        .iter()
        .filter(|w| !w.synced)
        .map(|w| {
            format!(
                "{} ({})",
                w.worker_id,
                w.error.as_deref().unwrap_or("no answer")
            )
        })
        .collect();
    if !unsynced.is_empty() {
        notes.push(format!(
            "{} did not confirm the change: {}; each picks it up on its next \
             heartbeat (a worker busy building catalogs can take longer than the \
             {} s push timeout)",
            plural(unsynced.len(), "worker"),
            unsynced.join(", "),
            PUSH_TIMEOUT.as_secs()
        ));
    }
    for w in workers.iter().filter(|w| !w.failed_catalogs.is_empty()) {
        notes.push(format!(
            "worker {} applied the change but could not build catalogs {}; it retries \
             them on its heartbeat",
            w.worker_id,
            w.failed_catalogs.join(", ")
        ));
    }
    notes
}

/// Pushes `set` to every live worker in parallel, at most 5 s each, and
/// waits for all. Blocking: called from the manage action's
/// `spawn_blocking`.
pub(super) fn push_to_workers(state: &WebState, set: &arneb_rpc::CatalogSet) -> Vec<WorkerSync> {
    let mut workers = state.node_registry.alive_workers();
    if workers.is_empty() {
        return Vec::new();
    }
    workers.sort_by(|a, b| a.worker_id.cmp(&b.worker_id));
    let security = arneb_rpc::cluster_security();
    let pushes = workers.into_iter().map(|w| async move {
        let pushed = tokio::time::timeout(
            PUSH_TIMEOUT,
            arneb_rpc::push_catalogs(&w.address, set, security),
        )
        .await;
        let (synced, error, failed_catalogs) = match pushed {
            Ok(Ok(failed)) => (true, None, failed),
            Ok(Err(e)) => (false, Some(e), Vec::new()),
            Err(_) => (
                false,
                Some(format!("no answer within {} s", PUSH_TIMEOUT.as_secs())),
                Vec::new(),
            ),
        };
        if !synced {
            tracing::warn!(
                target: "arneb::cluster",
                worker_id = %w.worker_id,
                generation = set.generation,
                error = error.as_deref().unwrap_or_default(),
                "catalog push failed; the worker's heartbeat pull repairs it"
            );
        }
        WorkerSync {
            worker_id: w.worker_id,
            synced,
            error,
            failed_catalogs,
        }
    });
    tokio::runtime::Handle::current().block_on(futures::future::join_all(pushes))
}

/// `?limit=` of the listing.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ListParams {
    limit: Option<usize>,
}

/// Parses a body that carries a catalog spec (and maybe its secrets). The
/// parser's message is dropped, since it can quote the input.
fn parse_spec_body(body: Result<Bytes, BytesRejection>) -> Result<serde_json::Value, ApiError> {
    let body = body.map_err(bad_body)?;
    serde_json::from_slice(&body).map_err(|_| {
        ApiError::new(ErrorCode::InvalidArgument, "invalid request body").with_hint(
            "send the catalog spec as a JSON object, with Content-Type: application/json",
        )
    })
}

async fn list_catalog_configs(
    State(state): State<WebState>,
    Extension(who): Extension<Identity>,
    params: Result<Query<ListParams>, QueryRejection>,
) -> Result<Json<CatalogConfigList>, ApiError> {
    let Query(params) = params.map_err(bad_query)?;
    Ok(Json(
        list_catalog_configs_for(&state, &who, params.limit).await?,
    ))
}

async fn get_catalog_config(
    State(state): State<WebState>,
    Extension(who): Extension<Identity>,
    Path(name): Path<String>,
) -> Result<Json<CatalogConfigView>, ApiError> {
    Ok(Json(get_catalog_config_for(&state, &who, name).await?))
}

async fn create_catalog(
    State(state): State<WebState>,
    Extension(who): Extension<Identity>,
    params: Result<Query<DryRun>, QueryRejection>,
    body: Result<Bytes, BytesRejection>,
) -> Result<Json<ManageOutcome<CatalogChange>>, ApiError> {
    let Query(DryRun { dry_run }) = params.map_err(bad_query)?;
    state.authorizer.check(&who, Layer::Manage)?;
    let spec = parse_spec_body(body)?;
    Ok(Json(create_catalog_for(&state, &who, dry_run, spec).await?))
}

async fn update_catalog(
    State(state): State<WebState>,
    Extension(who): Extension<Identity>,
    Path(name): Path<String>,
    params: Result<Query<DryRun>, QueryRejection>,
    body: Result<Bytes, BytesRejection>,
) -> Result<Json<ManageOutcome<CatalogChange>>, ApiError> {
    let Query(DryRun { dry_run }) = params.map_err(bad_query)?;
    state.authorizer.check(&who, Layer::Manage)?;
    let spec = parse_spec_body(body)?;
    Ok(Json(
        update_catalog_for(&state, &who, dry_run, name, spec).await?,
    ))
}

async fn drop_catalog(
    State(state): State<WebState>,
    Extension(who): Extension<Identity>,
    Path(name): Path<String>,
    params: Result<Query<DryRun>, QueryRejection>,
) -> Result<Json<ManageOutcome<CatalogChange>>, ApiError> {
    let Query(DryRun { dry_run }) = params.map_err(bad_query)?;
    Ok(Json(drop_catalog_for(&state, &who, dry_run, name).await?))
}

/// The catalog definition routes, merged into `/api/v2`.
pub(super) fn routes() -> Router<WebState> {
    Router::new()
        .route(
            "/catalog-configs",
            get(list_catalog_configs).post(create_catalog),
        )
        .route(
            "/catalog-configs/{name}",
            get(get_catalog_config)
                .put(update_catalog)
                .delete(drop_catalog),
        )
}

/// A test-only step between a change's probe and its lock.
#[cfg(test)]
pub(super) mod test_hooks {
    use std::sync::Mutex;

    use crate::web::WebState;

    /// `(lock key, hook)`: the next create or update on the state whose
    /// `config_changes` has this key runs `hook` after its probe. Keyed per
    /// state, so tests running in parallel do not share a hook.
    type Hook = (usize, Box<dyn FnOnce() + Send>);
    static AFTER_PROBE: Mutex<Vec<Hook>> = Mutex::new(Vec::new());

    fn key(state: &WebState) -> usize {
        crate::web::manage::test_hooks::key(&state.config_changes)
    }

    /// Run `hook` after the next probe on `state`.
    pub(in crate::web) fn run_after_probe(state: &WebState, hook: impl FnOnce() + Send + 'static) {
        AFTER_PROBE
            .lock()
            .unwrap()
            .push((key(state), Box::new(hook)));
    }

    pub(super) fn after_probe(state: &WebState) {
        let key = key(state);
        let hook = {
            let mut hooks = AFTER_PROBE.lock().unwrap();
            let at = hooks.iter().position(|(k, _)| *k == key);
            at.map(|at| hooks.swap_remove(at).1)
        };
        if let Some(hook) = hook {
            hook();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use arneb_catalog::CatalogManager;
    use arneb_connectors::memory::{MemoryCatalog, MemoryConnectorFactory};
    use arneb_connectors::ConnectorRegistry;
    use axum::http::{Method, StatusCode};
    use serde_json::{json, Value};

    use super::*;
    use crate::web::v2::tests::{basic, call, web_state, with_authz};

    const PG_SECRET: &str = "pg-secret-Zq81";
    const URL_SECRET: &str = "url-secret-Kx42";
    const BASE: &str = "/api/v2/catalog-configs";

    type Probed = Arc<Mutex<Vec<CatalogConfig>>>;

    fn root() -> String {
        basic("root", "rootpw")
    }

    fn bob() -> String {
        basic("bob", "hunter2")
    }

    fn fake_built() -> BuiltCatalog {
        let catalog = Arc::new(MemoryCatalog::new());
        BuiltCatalog {
            provider: catalog.clone(),
            connector: Arc::new(MemoryConnectorFactory::new(catalog, "default")),
        }
    }

    /// Password auth with `root` in admin_users (bob does not manage); the
    /// default catalog is `lake`; every build succeeds and records the
    /// spec it was given.
    fn node() -> (WebState, Probed) {
        let probed: Probed = Default::default();
        let p = probed.clone();
        let live = LiveCatalogs::with_test_builder(
            Arc::new(CatalogManager::new("lake", "default")),
            Arc::new(ConnectorRegistry::new()),
            Box::new(move |spec| {
                p.lock().unwrap().push(spec.clone());
                Ok(fake_built())
            }),
        );
        let mut state = with_authz(web_state(true, None), None, &["root"]);
        state.catalogs = Some(live);
        (state, probed)
    }

    /// [`node`] whose builds really connect.
    fn connecting_node() -> WebState {
        let live = LiveCatalogs::new(
            Arc::new(CatalogManager::new("lake", "default")),
            Arc::new(ConnectorRegistry::new()),
            Default::default(),
        );
        let mut state = with_authz(web_state(true, None), None, &["root"]);
        state.catalogs = Some(live);
        state
    }

    fn closed_port() -> u16 {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    }

    /// A postgresql spec whose URL and `password` carry secrets.
    fn pg_spec(name: &str, port: u16) -> Value {
        json!({
            "name": name,
            "type": "postgresql",
            "connection_url": format!("postgresql://arneb:{URL_SECRET}@127.0.0.1:{port}/arneb"),
            "user": "arneb",
            "password": PG_SECRET,
        })
    }

    fn store_of(state: &WebState) -> &StateStore {
        state.state.as_deref().unwrap()
    }

    fn stored(state: &WebState, name: &str) -> Option<CatalogRecord> {
        find_record(store_of(state), name)
    }

    fn audit_json(state: &WebState) -> Vec<Value> {
        state
            .audit
            .query(&arneb_state::AuditQuery {
                limit: 100,
                before_seq: None,
                user: None,
                action: None,
            })
            .entries
            .into_iter()
            .map(|e| serde_json::to_value(&e).unwrap())
            .collect()
    }

    fn dry(path: &str, dry_run: bool) -> String {
        if dry_run {
            format!("{path}?dry_run=true")
        } else {
            path.to_owned()
        }
    }

    async fn create(state: &WebState, spec: Value, dry_run: bool) -> (StatusCode, Value) {
        call(
            state,
            Method::POST,
            &dry(BASE, dry_run),
            Some(&root()),
            Some(spec),
        )
        .await
    }

    async fn update(
        state: &WebState,
        name: &str,
        spec: Value,
        dry_run: bool,
    ) -> (StatusCode, Value) {
        call(
            state,
            Method::PUT,
            &dry(&format!("{BASE}/{name}"), dry_run),
            Some(&root()),
            Some(spec),
        )
        .await
    }

    async fn drop_catalog(state: &WebState, name: &str, dry_run: bool) -> (StatusCode, Value) {
        call(
            state,
            Method::DELETE,
            &dry(&format!("{BASE}/{name}"), dry_run),
            Some(&root()),
            None,
        )
        .await
    }

    async fn get(state: &WebState, path: &str) -> (StatusCode, Value) {
        call(state, Method::GET, path, Some(&root()), None).await
    }

    #[tokio::test]
    async fn catalog_routes_require_manage() {
        let (state, probed) = node();
        let spec = pg_spec("pg", closed_port());
        let one = format!("{BASE}/pg");
        let routes: Vec<(Method, String, Option<Value>)> = vec![
            (Method::GET, BASE.into(), None),
            (Method::GET, one.clone(), None),
            (Method::POST, BASE.into(), Some(spec.clone())),
            (Method::PUT, one.clone(), Some(spec.clone())),
            (Method::DELETE, one.clone(), None),
        ];
        for (method, path, body) in routes {
            let dry = (method != Method::GET).then(|| format!("{path}?dry_run=true"));
            for path in std::iter::once(path.clone()).chain(dry) {
                let (status, resp) =
                    call(&state, method.clone(), &path, Some(&bob()), body.clone()).await;
                assert_eq!(status, StatusCode::FORBIDDEN, "{method} {path}: {resp}");
                assert_eq!(resp["error"]["code"], "forbidden", "{method} {path}");
            }
        }
        assert!(probed.lock().unwrap().is_empty(), "nothing was probed");
        assert!(store_of(&state).catalogs().1.is_empty());
        assert!(audit_json(&state).is_empty());

        // Under [auth] none: refused, whoever asks.
        let mut none = with_authz(web_state(false, None), None, &["root"]);
        none.catalogs = state.catalogs.clone();
        let (status, resp) = create(&none, spec, false).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{resp}");
        assert_eq!(resp["error"]["code"], "manage_requires_auth");
    }

    /// Ruling R13: a typo in an API spec, top-level or inside `storage`,
    /// is refused naming the field (never its value), and nothing is
    /// probed or stored; a PUT is refused the same way.
    #[tokio::test]
    async fn unknown_spec_fields_are_refused() {
        let (state, probed) = node();
        let mut top = pg_spec("pg", closed_port());
        top["max_conections"] = json!(4);
        let (status, resp) = create(&state, top, false).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{resp}");
        assert_eq!(resp["error"]["code"], "invalid_argument");
        let message = resp["error"]["message"].as_str().unwrap();
        assert!(message.contains("max_conections"), "{message}");

        let nested = json!({
            "name": "lake2",
            "type": "hive",
            "metastore_uri": "127.0.0.1:9083",
            "storage": {"s3": {"endpoint": "http://s3:9000", "secret_acess_key": "typo-secret-77"}},
        });
        let (status, resp) = create(&state, nested.clone(), true).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{resp}");
        let message = resp["error"]["message"].as_str().unwrap();
        assert!(message.contains("storage.s3.secret_acess_key"), "{message}");
        assert!(!resp.to_string().contains("typo-secret-77"), "{resp}");
        assert!(probed.lock().unwrap().is_empty(), "nothing was probed");
        assert!(store_of(&state).catalogs().1.is_empty());

        // A PUT of an existing catalog is refused the same way.
        let (status, resp) = create(&state, pg_spec("pg", closed_port()), false).await;
        assert_eq!(status, StatusCode::OK, "{resp}");
        let mut typo = pg_spec("pg", closed_port());
        typo["pasword"] = json!("x");
        let (status, resp) = update(&state, "pg", typo, false).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{resp}");
        assert!(resp["error"]["message"]
            .as_str()
            .unwrap()
            .contains("pasword"));
    }

    #[tokio::test]
    async fn create_with_unreachable_backend_is_invalid_argument_and_changes_nothing() {
        let state = connecting_node();
        let generation = store_of(&state).catalogs().0;
        for dry_run in [true, false] {
            let (status, body) = create(&state, pg_spec("pg", closed_port()), dry_run).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "dry_run={dry_run}: {body}");
            assert_eq!(body["error"]["code"], "invalid_argument");
            let hint = body["error"]["hint"].as_str().unwrap();
            assert!(
                hint.starts_with("the coordinator could not connect: "),
                "{hint}"
            );
            let text = body.to_string();
            assert!(
                !text.contains(PG_SECRET) && !text.contains(URL_SECRET),
                "{text}"
            );
        }
        assert_eq!(store_of(&state).catalogs(), (generation, vec![]));
        assert!(state.catalogs.as_ref().unwrap().specs().is_empty());
        assert!(audit_json(&state).is_empty());
    }

    #[tokio::test]
    async fn create_existing_name_is_conflict() {
        let (state, probed) = node();
        let (status, body) = create(&state, pg_spec("pg", closed_port()), false).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        for dry_run in [true, false] {
            let (status, body) = create(&state, pg_spec("pg", closed_port()), dry_run).await;
            assert_eq!(status, StatusCode::CONFLICT, "dry_run={dry_run}: {body}");
            assert_eq!(body["error"]["code"], "conflict");
        }
        assert_eq!(
            probed.lock().unwrap().len(),
            1,
            "a taken name is not probed"
        );
    }

    #[tokio::test]
    async fn drop_unknown_is_not_found() {
        let (state, _) = node();
        for name in ["nope", "memory", "file"] {
            for dry_run in [true, false] {
                let (status, body) = drop_catalog(&state, name, dry_run).await;
                assert_eq!(
                    status,
                    StatusCode::NOT_FOUND,
                    "{name} dry_run={dry_run}: {body}"
                );
                assert_eq!(body["error"]["code"], "not_found");
            }
            let (status, body) = update(&state, name, pg_spec(name, 1), false).await;
            assert_eq!(status, StatusCode::NOT_FOUND, "PUT {name}: {body}");
            let (status, _) = get(&state, &format!("{BASE}/{name}")).await;
            assert_eq!(status, StatusCode::NOT_FOUND, "GET {name}");
        }
        assert!(audit_json(&state).is_empty());
    }

    #[tokio::test]
    async fn drop_default_catalog_warns() {
        let (state, _) = node();
        let (status, body) = create(&state, pg_spec("lake", closed_port()), false).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let (_, listed) = get(&state, BASE).await;
        assert_eq!(listed["catalogs"][0]["default"], true, "{listed}");

        let (status, body) = drop_catalog(&state, "lake", true).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let warning = body["warning"].as_str().unwrap();
        assert!(
            warning.contains("default catalog") && warning.contains("unqualified"),
            "{warning}"
        );
        assert!(
            stored(&state, "lake").is_some(),
            "a dry run changes nothing"
        );

        let generation = store_of(&state).catalogs().0;
        let (status, body) = drop_catalog(&state, "lake", false).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["action"], "catalog.drop");
        assert_eq!(body["result"]["generation"], generation + 1);
        assert!(stored(&state, "lake").is_none());
        let live = state.catalogs.as_ref().unwrap();
        assert!(live.specs().is_empty());
        assert_eq!(live.generation(), generation + 1);
        let audit = audit_json(&state);
        assert_eq!(audit[0]["action"], "catalog.drop");
        assert_eq!(audit[0]["parameters"]["name"], "lake");
    }

    #[tokio::test]
    async fn catalog_secrets_never_leave_redacted() {
        let (state, _) = node();
        let mut spec = pg_spec("pg", closed_port());
        spec["storage"] =
            json!({"s3": {"region": "us-east-1", "secret_access_key": "s3-secret-Vv03"}});
        let mut seen = Vec::new();
        let (status, body) = create(&state, spec.clone(), false).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        seen.push(body);
        seen.push(create(&state, pg_spec("pg2", 1), true).await.1);
        seen.push(get(&state, BASE).await.1);
        let (_, one) = get(&state, &format!("{BASE}/pg")).await;
        assert_eq!(one["spec"]["password"], "<redacted>", "{one}");
        let url = one["spec"]["connection_url"].as_str().unwrap();
        assert!(url.contains("arneb:<redacted>@"), "{url}");
        seen.push(one.clone());
        seen.push(update(&state, "pg", one["spec"].clone(), true).await.1);
        seen.push(update(&state, "pg", one["spec"].clone(), false).await.1);
        seen.push(drop_catalog(&state, "pg", true).await.1);
        seen.push(get(&state, "/api/v2/audit").await.1);
        seen.push(Value::Array(audit_json(&state)));
        for body in &seen {
            let text = body.to_string();
            for secret in [PG_SECRET, URL_SECRET, "s3-secret-Vv03"] {
                assert!(!text.contains(secret), "{secret} leaked in {text}");
            }
        }
        // The test is meaningful: the store does hold them.
        let raw = stored(&state, "pg").unwrap().spec.to_string();
        assert!(
            raw.contains(PG_SECRET) && raw.contains(URL_SECRET) && raw.contains("s3-secret-Vv03")
        );
        let audit = audit_json(&state);
        assert!(
            audit
                .iter()
                .any(|e| e["action"] == "catalog.create"
                    && e["parameters"]["password"] == "<redacted>"),
            "{audit:?}"
        );
    }

    #[tokio::test]
    async fn put_with_redacted_password_keeps_the_stored_one() {
        let (state, probed) = node();
        let (status, body) = create(&state, pg_spec("pg", closed_port()), false).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let (_, one) = get(&state, &format!("{BASE}/pg")).await;
        let mut spec = one["spec"].clone();
        spec["max_connections"] = json!(4);
        let (status, body) = update(&state, "pg", spec, false).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["action"], "catalog.update");

        let record = stored(&state, "pg").unwrap();
        assert_eq!(record.spec["password"], PG_SECRET);
        let url = record.spec["connection_url"].as_str().unwrap();
        assert!(
            url.contains(&format!("arneb:{URL_SECRET}@")),
            "stored url lost its password"
        );
        assert_eq!(record.spec["max_connections"], 4);
        // The probe connected with the real secrets, not the placeholder.
        let last = probed.lock().unwrap().last().cloned().unwrap();
        assert_eq!(last.password.as_ref().unwrap().0, PG_SECRET);
        assert_eq!(last.max_connections, Some(4));
        let live = state.catalogs.as_ref().unwrap();
        assert_eq!(live.specs()[0].max_connections, Some(4));
    }

    /// A PUT of the spec already stored and served (here a GET body sent
    /// back) changes nothing: `changed: false`, audit effect `no_change`,
    /// no generation bump and no push.
    #[tokio::test]
    async fn identical_put_is_a_no_op() {
        let (state, _) = node();
        let (status, created) = create(&state, pg_spec("pg", closed_port()), false).await;
        assert_eq!(status, StatusCode::OK, "{created}");
        let generation = store_of(&state).catalogs().0;
        let (_, one) = get(&state, &format!("{BASE}/pg")).await;
        let (status, body) = update(&state, "pg", one["spec"].clone(), false).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["changed"], false, "{body}");
        assert_eq!(body["result"]["generation"], generation);
        assert_eq!(body["result"]["workers"], json!([]));
        assert_eq!(store_of(&state).catalogs().0, generation);
        let live = state.catalogs.as_ref().unwrap();
        assert_eq!(live.generation(), created["result"]["generation"]);
        let audit = audit_json(&state);
        let last = audit
            .iter()
            .find(|e| e["action"] == "catalog.update")
            .expect("audited");
        assert_eq!(last["parameters"]["effect"], "no_change", "{last}");
        // The dry run says so too.
        let (_, dry_body) = update(&state, "pg", one["spec"].clone(), true).await;
        assert_eq!(dry_body["changed"], false, "{dry_body}");
    }

    /// A change queued on `config_changes` behind a rules change that drops
    /// the caller's manage grant is refused with `forbidden` once it gets
    /// the lock, and changes nothing.
    #[tokio::test]
    async fn queued_change_rechecks_manage_under_the_lock() {
        let (mut state, _) = node();
        let bob_manages = r#"{"system_information":[{"user":"bob","allow":["read","write"]}]}"#;
        state = with_authz(state, Some(bob_manages), &["root"]);
        let (status, body) = create(&state, pg_spec("pg", closed_port()), false).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let before = stored(&state, "pg").unwrap();

        // A rules change holds the lock; bob's PUT queues behind it.
        let held = Arc::clone(&state.config_changes).lock_owned().await;
        let mut spec = pg_spec("pg", closed_port());
        spec["max_connections"] = json!(3);
        let queued = {
            let state = state.clone();
            tokio::spawn(async move {
                call(
                    &state,
                    Method::PUT,
                    &format!("{BASE}/pg"),
                    Some(&bob()),
                    Some(spec),
                )
                .await
            })
        };
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(!queued.is_finished(), "the PUT waits for the lock");
        // The rules change applies (dropping bob's write) and releases.
        state.rules.replace(Some(Arc::new(
            arneb_catalog::AccessRules::from_json(
                r#"{"system_information":[{"user":"bob","allow":["read"]}]}"#,
            )
            .unwrap(),
        )));
        drop(held);
        let (status, body) = queued.await.unwrap();
        assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
        assert_eq!(body["error"]["code"], "forbidden");
        assert_eq!(stored(&state, "pg").unwrap(), before);
    }

    #[tokio::test]
    async fn update_of_a_config_catalog_detaches_it() {
        let (state, _) = node();
        let spec = pg_spec("pg", closed_port());
        store_of(&state)
            .seed_catalogs(&[("pg".into(), spec.clone())], false)
            .unwrap();
        assert!(stored(&state, "pg").unwrap().from_config);
        let (_, listed) = get(&state, BASE).await;
        assert_eq!(listed["catalogs"][0]["from_config"], true);
        let (status, body) = update(&state, "pg", spec.clone(), true).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert!(body["warning"]
            .as_str()
            .unwrap()
            .contains("the config file no longer manages this catalog"));
        let (status, body) = update(&state, "pg", spec, false).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert!(!stored(&state, "pg").unwrap().from_config);
    }

    /// Config-seeded catalogs were never held to the `[a-z0-9_]` name rule:
    /// a `pg-prod` from the file can still be updated (to rotate its
    /// password) and dropped; only a create checks the name.
    #[tokio::test]
    async fn existing_names_outside_the_name_rule_can_be_updated_and_dropped() {
        let (state, _) = node();
        let spec = pg_spec("pg-prod", closed_port());
        store_of(&state)
            .seed_catalogs(&[("pg-prod".into(), spec.clone())], false)
            .unwrap();
        let (status, body) = create(&state, pg_spec("pg-new", 1), false).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(body["error"]["code"], "invalid_argument");
        let mut rotated = spec.clone();
        rotated["password"] = json!("rotated-secret");
        let (status, body) = update(&state, "pg-prod", rotated, false).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let record = stored(&state, "pg-prod").unwrap();
        assert_eq!(record.spec["password"], "rotated-secret");
        let (status, body) = drop_catalog(&state, "pg-prod", false).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert!(stored(&state, "pg-prod").is_none());
    }

    /// A coordinator without a `[cluster] secret` refuses a catalog that
    /// would serve a secret over Flight, on create and on update.
    #[tokio::test]
    async fn secrets_are_refused_without_a_cluster_secret() {
        let (state, probed) = node();
        let live = state.catalogs.clone().unwrap();
        live.set_unauthenticated_flight(true);
        let (status, body) = create(&state, pg_spec("pg", closed_port()), false).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(body["error"]["code"], "invalid_argument");
        assert!(body["error"]["hint"]
            .as_str()
            .unwrap()
            .contains("[cluster] secret"));
        assert!(!body.to_string().contains(PG_SECRET));
        assert!(stored(&state, "pg").is_none());
        assert!(
            probed.lock().unwrap().is_empty(),
            "refused before connecting"
        );

        let plain = json!({
            "name": "pg", "type": "postgresql",
            "connection_url": format!("postgresql://arneb@127.0.0.1:{}/arneb", closed_port()),
        });
        let (status, body) = create(&state, plain.clone(), false).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let mut with_password = plain;
        with_password["password"] = json!(PG_SECRET);
        let (status, body) = update(&state, "pg", with_password, false).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert!(stored(&state, "pg").unwrap().spec["password"].is_null());
    }

    #[tokio::test]
    async fn list_is_bounded() {
        let (state, _) = node();
        for name in ["a1", "b2"] {
            assert_eq!(
                create(&state, pg_spec(name, 1), false).await.0,
                StatusCode::OK
            );
        }
        let (status, body) = get(&state, &format!("{BASE}?limit=1")).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["truncated"], true);
        assert_eq!(body["catalogs"].as_array().unwrap().len(), 1);
        assert_eq!(body["catalogs"][0]["name"], "a1");
        assert_eq!(body["generation"], store_of(&state).catalogs().0);
        for limit in [0, 1001] {
            let (status, body) = get(&state, &format!("{BASE}?limit={limit}")).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{limit}: {body}");
            assert_eq!(body["error"]["code"], "invalid_argument");
        }
        let (_, body) = get(&state, &format!("{BASE}?limit=1000")).await;
        assert_eq!(body["truncated"], false);
    }

    #[tokio::test]
    async fn create_validates_the_spec() {
        let (state, probed) = node();
        for spec in [
            json!({"name": "memory", "type": "hive", "metastore_uri": "h:9083"}),
            json!({"name": "Bad-Name", "type": "hive", "metastore_uri": "h:9083"}),
            json!({"name": "m", "type": "mysql"}),
            json!({"name": "h", "type": "hive"}),
            json!({"type": "hive"}),
            json!({"name": "p", "type": "postgresql", "connection_url": "postgresql://h/db",
                   "max_connections": PG_SECRET}),
            json!([1, 2]),
        ] {
            let (status, body) = create(&state, spec.clone(), false).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{spec}: {body}");
            assert_eq!(body["error"]["code"], "invalid_argument");
            assert!(!body.to_string().contains(PG_SECRET), "{body}");
        }
        let (status, body) = update(&state, "x", json!({"name": "y"}), false).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
        assert!(probed.lock().unwrap().is_empty());
    }

    /// A stub worker Flight server whose `sync_catalogs` records each set
    /// and answers `failed`.
    async fn stub_worker(failed: Vec<String>) -> (String, Arc<Mutex<Vec<arneb_rpc::CatalogSet>>>) {
        let seen: Arc<Mutex<Vec<arneb_rpc::CatalogSet>>> = Default::default();
        let s = seen.clone();
        let mut flight = arneb_rpc::FlightState::new();
        flight.set_catalog_sync_callback(Arc::new(move |set: arneb_rpc::CatalogSet| {
            s.lock().unwrap().push(set);
            let failed = failed.clone();
            Box::pin(async move { Ok(failed) })
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(arneb_rpc::__serve_flight_for_test(listener, flight));
        (format!("http://{addr}"), seen)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn create_pushes_to_registered_workers() {
        let (state, _) = node();
        let (w1, seen) = stub_worker(Vec::new()).await;
        let (w3, _) = stub_worker(vec!["pg".into()]).await;
        let registry = &state.node_registry;
        registry.heartbeat("w1".into(), w1, 8);
        registry.heartbeat(
            "w2".into(),
            format!("http://127.0.0.1:{}", closed_port()),
            8,
        );
        registry.heartbeat("w3".into(), w3, 8);

        let (status, body) = create(&state, pg_spec("pg", closed_port()), false).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let result = &body["result"];
        let workers = result["workers"].as_array().unwrap();
        assert_eq!(workers.len(), 3, "{body}");
        assert_eq!(workers[0]["worker_id"], "w1");
        assert_eq!(workers[0]["synced"], true);
        assert!(workers[0].get("error").is_none(), "{body}");
        assert_eq!(workers[1]["worker_id"], "w2");
        assert_eq!(workers[1]["synced"], false);
        assert!(workers[1]["error"].is_string(), "{body}");
        // Ruling R7: applied with a failed build is synced, and named.
        assert_eq!(workers[2]["worker_id"], "w3");
        assert_eq!(workers[2]["synced"], true);
        assert_eq!(workers[2]["failed_catalogs"], json!(["pg"]));
        let warning = body["warning"].as_str().unwrap();
        assert!(warning.contains("w2"), "{warning}");
        assert!(!warning.contains("w1 "), "{warning}");
        assert!(
            warning.contains("worker w3") && warning.contains("pg"),
            "{warning}"
        );

        let sets = seen.lock().unwrap().clone();
        assert_eq!(sets.len(), 1);
        let live = state.catalogs.as_ref().unwrap();
        assert_eq!(sets[0].generation, result["generation"].as_u64().unwrap());
        assert_eq!(sets[0].epoch, live.epoch());
        assert_eq!(sets[0].catalogs.len(), 1);
        assert_eq!(sets[0].catalogs[0]["name"], "pg");
        // The pushed spec carries the raw secret (the worker must connect).
        assert_eq!(sets[0].catalogs[0]["password"], PG_SECRET);
    }

    #[tokio::test]
    async fn create_then_discovery_lists_it() {
        let Ok(url) = std::env::var("ARNEB_TEST_POSTGRES_URL") else {
            eprintln!("ARNEB_TEST_POSTGRES_URL not set; skipping");
            return;
        };
        let manager = Arc::new(CatalogManager::new("lake", "default"));
        let connectors = Arc::new(ConnectorRegistry::new());
        let live = LiveCatalogs::new(manager.clone(), connectors.clone(), Default::default());
        let mut state = with_authz(web_state(true, None), None, &["root"]);
        state.agent = Some(arneb_protocol::agent::AgentQueryService::new(
            manager,
            connectors,
            state.query_tracker.clone(),
            Default::default(),
        ));
        state.catalogs = Some(live);
        let spec = json!({"name": "pg_live", "type": "postgresql", "connection_url": url});
        let (status, body) = create(&state, spec, false).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let (status, body) = get(&state, "/api/v2/catalogs").await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert!(body.to_string().contains("pg_live"), "{body}");
    }

    #[tokio::test]
    async fn stale_probe_is_conflict() {
        let (state, _) = node();
        let (status, body) = create(&state, pg_spec("pg", closed_port()), false).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        // Another change lands between the probe and the apply.
        let store = state.state.clone().unwrap();
        test_hooks::run_after_probe(&state, move || {
            let mut record = find_record(&store, "pg").unwrap();
            record.spec["max_connections"] = json!(7);
            store.put_catalog(record).unwrap();
        });
        let mut spec = pg_spec("pg", closed_port());
        spec["max_connections"] = json!(4);
        let (status, body) = update(&state, "pg", spec, false).await;
        assert_eq!(status, StatusCode::CONFLICT, "{body}");
        assert_eq!(body["error"]["code"], "conflict");
        assert_eq!(body["error"]["hint"], "retry");
        assert_eq!(stored(&state, "pg").unwrap().spec["max_connections"], 7);
    }

    /// Important 1(a): a POST cannot store the placeholder as a secret
    /// (an agent cloning catalog A's GET body into a create for B).
    #[tokio::test]
    async fn create_with_a_redacted_placeholder_is_refused() {
        let (state, probed) = node();
        let url = |u: &str| json!({"name": "pg", "type": "postgresql", "connection_url": u});
        let hive = |storage: Value| json!({"name": "lake", "type": "hive", "metastore_uri": "h:9083", "storage": storage});
        for spec in [
            {
                let mut s = pg_spec("pg", 1);
                s["password"] = json!(REDACTED);
                s
            },
            url("postgresql://arneb:<redacted>@127.0.0.1:1/arneb"),
            url("postgresql://127.0.0.1:1/arneb?user=arneb&password=<redacted>"),
            hive(json!({"s3": {"region": "r", "secret_access_key": REDACTED}})),
            hive(json!({"gcs": {"service_account_path": REDACTED}})),
            hive(json!({"azure": {"storage_account": "a", "access_key": REDACTED}})),
        ] {
            for dry_run in [true, false] {
                let (status, body) = create(&state, spec.clone(), dry_run).await;
                assert_eq!(status, StatusCode::BAD_REQUEST, "{spec}: {body}");
                assert_eq!(body["error"]["code"], "invalid_argument");
                assert!(
                    body["error"]["hint"]
                        .as_str()
                        .unwrap()
                        .starts_with("send the real secrets"),
                    "{body}"
                );
            }
        }
        assert!(probed.lock().unwrap().is_empty(), "nothing was probed");
        assert!(store_of(&state).catalogs().1.is_empty());
        assert!(audit_json(&state).is_empty());
    }

    /// Important 1(b): a stored spec that does not decode has nothing to
    /// fill a placeholder from.
    #[tokio::test]
    async fn update_of_an_undecodable_stored_spec_needs_real_secrets() {
        let (state, _) = node();
        store_of(&state)
            .put_catalog(CatalogRecord {
                name: "pg".into(),
                spec: json!({"name": "pg", "type": 5}),
                from_config: false,
            })
            .unwrap();
        let mut spec = pg_spec("pg", 1);
        spec["password"] = json!(REDACTED);
        let (status, body) = update(&state, "pg", spec, false).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(body["error"]["hint"], "send the real secrets");
        assert_eq!(stored(&state, "pg").unwrap().spec["type"], 5);
        let (status, body) = update(&state, "pg", pg_spec("pg", 1), false).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(stored(&state, "pg").unwrap().spec["password"], PG_SECRET);
    }

    /// Ruling R10: a PUT that changes where a kept secret is sent must
    /// send it again.
    #[tokio::test]
    async fn put_that_changes_the_destination_needs_the_secret_again() {
        let (state, _) = node();
        let (status, body) = create(&state, pg_spec("pg", 5432), false).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let (_, one) = get(&state, &format!("{BASE}/pg")).await;
        let redacted = one["spec"].clone();

        // Another host, password left redacted: refused.
        let mut moved = redacted.clone();
        moved["connection_url"] = json!("postgresql://arneb:<redacted>@evil.example:5432/arneb");
        let (status, body) = update(&state, "pg", moved.clone(), false).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(body["error"]["code"], "invalid_argument");
        assert_eq!(
            body["error"]["hint"],
            "send the secret again when you change where it is sent"
        );
        // Only the top-level password redacted, URL without one: still refused.
        let mut moved_pw = redacted.clone();
        moved_pw["connection_url"] = json!("postgresql://arneb@evil.example:5432/arneb");
        let (status, _) = update(&state, "pg", moved_pw, true).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(stored(&state, "pg").unwrap().spec["password"], PG_SECRET);

        // The same move with the real secrets: ok.
        moved["connection_url"] = json!(format!(
            "postgresql://arneb:{URL_SECRET}@evil.example:5432/arneb"
        ));
        moved["password"] = json!("new-pw-Lm55");
        let (status, body) = update(&state, "pg", moved, false).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(
            stored(&state, "pg").unwrap().spec["password"],
            "new-pw-Lm55"
        );

        // Only max_connections changes, secrets redacted: ok, kept.
        let (_, one) = get(&state, &format!("{BASE}/pg")).await;
        let mut spec = one["spec"].clone();
        spec["max_connections"] = json!(3);
        let (status, body) = update(&state, "pg", spec, false).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let record = stored(&state, "pg").unwrap();
        assert_eq!(record.spec["password"], "new-pw-Lm55");
        assert_eq!(record.spec["max_connections"], 3);

        // Hive: a new metastore or S3 endpoint with a redacted S3 secret.
        let lake = json!({"name": "lake", "type": "hive", "metastore_uri": "h:9083",
            "storage": {"s3": {"region": "r", "endpoint": "http://s3.local",
                               "secret_access_key": "s3-secret-Vv03"}}});
        assert_eq!(create(&state, lake, false).await.0, StatusCode::OK);
        let (_, one) = get(&state, &format!("{BASE}/lake")).await;
        for (field, value) in [
            ("/metastore_uri", json!("evil:9083")),
            ("/storage/s3/endpoint", json!("http://evil.example")),
        ] {
            let mut spec = one["spec"].clone();
            *spec.pointer_mut(field).unwrap() = value;
            let (status, body) = update(&state, "lake", spec, true).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{field}: {body}");
        }
        let (status, body) = update(&state, "lake", one["spec"].clone(), true).await;
        assert_eq!(status, StatusCode::OK, "unchanged destination: {body}");
    }
}
