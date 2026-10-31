//! The re-seed action, `POST /api/v2/state/reseed` (runtime-config D2): make
//! the state equal a first start of the config file again.
//!
//! - The config is reloaded with `AppConfig::load` from the path the process
//!   started with (Ruling R3), env overrides applied. A config that fails to
//!   load is `invalid_argument`; the message keeps only the loader's first
//!   line (a TOML error quotes the offending line below it, which may hold a
//!   secret).
//! - The config's catalogs that differ from the live ones are built on the
//!   coordinator first, outside the change lock and for a dry run too (D10).
//!   A catalog that fails to build is still stored (it came from the config)
//!   and reported in the `warning`, like startup's skip-with-WARN.
//! - Under `WebState::config_changes` it computes the plan, runs the lockout
//!   guard (D3) against the config's rules, then persists and swaps each
//!   object in turn: users (`seed_users(.., true)`, tombstones cleared, the
//!   cached logins and Web sessions of every updated or removed user end),
//!   rules, settings (cleared; the config's admission limits and slow-query
//!   threshold apply) and catalogs (installed at the store's generation and
//!   pushed to every live worker, 5 s each).
//! - Tokens, cordons and the audit log are left alone, except the tokens
//!   user seeding revokes. Responses and audit entries carry names only.

use std::collections::BTreeMap;
use std::sync::Arc;

use arneb_catalog::{AccessRules, RulesHandle};
use arneb_protocol::ScramVerifier;
use arneb_state::{AdmissionLimits, StateStore, StoredRules};
use axum::extract::rejection::QueryRejection;
use axum::extract::{Query, State};
use axum::response::Json;
use axum::routing::post;
use axum::{Extension, Router};
use serde::Serialize;

use super::auth::Identity;
use super::catalogs::{push_to_workers, push_warnings, WorkerSync};
use super::error::{ApiError, ErrorCode};
use super::manage::{
    run_manage, Applied, ChangeLock, DryRun, ManageAction, ManageOutcome, Preview,
};
use super::privilege::Layer;
use super::v2::bad_query;
use super::WebState;
use crate::catalogs::{BuiltCatalog, LiveCatalogs};
use crate::config::{slow_query_threshold_from_ms, AppConfig, AuthType, CatalogConfig};

/// The catalogs a re-seed adds, changes and removes.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub(super) struct CatalogDiff {
    added: Vec<String>,
    /// In the state and the config, with another spec (or detached from
    /// the config by a runtime change).
    changed: Vec<String>,
    /// In the state but not in the config (runtime-created ones included).
    removed: Vec<String>,
}

/// The users a re-seed adds, updates and removes, by name.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub(super) struct UserDiff {
    added: Vec<String>,
    /// Their verifier becomes the config's; their tokens are revoked.
    updated: Vec<String>,
    removed: Vec<String>,
    /// Config users deleted at runtime that the re-seed brings back.
    tombstones_cleared: Vec<String>,
}

/// What a re-seed does (or did). Names only, never specs or passwords.
#[derive(Debug, Serialize)]
pub(super) struct ReseedReport {
    catalogs: CatalogDiff,
    /// Whether the stored access rules change.
    rules_changed: bool,
    /// The runtime knobs that go back to the config value.
    settings_reset: Vec<&'static str>,
    users: UserDiff,
    warnings: Vec<String>,
    /// The catalog generation after the re-seed (absent on a no-op).
    #[serde(skip_serializing_if = "Option::is_none")]
    generation: Option<u64>,
    /// Every live worker the new catalog set was pushed to.
    workers: Vec<WorkerSync>,
}

/// The reloaded config, reduced to what a re-seed applies.
struct Seed {
    users: Vec<(String, ScramVerifier)>,
    /// The rules file's text and parsed rules; `None`: no rules file.
    rules: Option<(String, Arc<AccessRules>)>,
    /// The `[[catalogs]]` specs and their JSON, in config order.
    catalogs: Vec<(CatalogConfig, serde_json::Value)>,
    admission: AdmissionLimits,
    slow_query_threshold_ms: u64,
}

/// A config loader message, cut to its first line: a TOML parse error
/// quotes the offending source line below it, and that line may hold a
/// password or key.
fn load_failed(message: &str) -> ApiError {
    let first = message.lines().next().unwrap_or_default().trim();
    ApiError::new(
        ErrorCode::InvalidArgument,
        format!("the config file could not be loaded: {first}"),
    )
    .with_hint("fix the config file the server was started with (--config), then retry")
}

/// Loads the config from `path` and reduces it to a [`Seed`].
fn load_seed(path: Option<&std::path::Path>) -> Result<Seed, ApiError> {
    let config = AppConfig::load(path).map_err(|e| load_failed(&format!("{e:#}")))?;
    if config.auth.auth_type != AuthType::Password {
        return Err(ApiError::new(
            ErrorCode::InvalidArgument,
            "the config file's [auth] type is not \"password\"",
        )
        .with_hint(
            "[auth] type is startup-only: a re-seed would remove every config user; \
             restore type = \"password\" in the config file",
        ));
    }
    let users = config
        .auth
        .user_verifiers()
        .map_err(|e| load_failed(&format!("{e:#}")))?;
    let rules = config
        .access_control
        .load_rules_text()
        .map_err(|e| load_failed(&format!("{e:#}")))?
        .map(|(text, rules)| (text, Arc::new(rules)));
    let catalogs = config
        .catalogs
        .into_iter()
        .map(|spec| {
            let json = serde_json::to_value(&spec).map_err(|e| {
                tracing::error!(catalog = %spec.name, error = ?e.classify(), "cannot serialize a catalog spec");
                ApiError::new(ErrorCode::Internal, "a [[catalogs]] spec could not be serialized")
            })?;
            Ok((spec, json))
        })
        .collect::<Result<Vec<_>, ApiError>>()?;
    Ok(Seed {
        users,
        rules,
        catalogs,
        admission: AdmissionLimits {
            max_running_queries: config.admission.max_running_queries,
            max_queued_queries: config.admission.max_queued_queries,
        },
        slow_query_threshold_ms: config.observability.slow_query_threshold_ms,
    })
}

fn store(state: &WebState) -> Result<Arc<StateStore>, ApiError> {
    state.state.clone().ok_or_else(|| {
        tracing::error!("password auth is configured without a state store");
        ApiError::new(ErrorCode::Internal, "the state store is unavailable")
    })
}

fn live(state: &WebState) -> Result<Arc<LiveCatalogs>, ApiError> {
    state.catalogs.clone().ok_or_else(|| {
        tracing::error!("the re-seed route is served without live catalogs");
        ApiError::new(ErrorCode::Internal, "the live catalogs are unavailable")
    })
}

/// The installed specs as JSON, by name.
fn installed(live: &LiveCatalogs) -> BTreeMap<String, serde_json::Value> {
    live.specs()
        .iter()
        .filter_map(|s| Some((s.name.clone(), serde_json::to_value(s).ok()?)))
        .collect()
}

/// The lockout guard (D3): the config's rules must leave `who` manage.
fn guard_lockout(
    state: &WebState,
    who: &Identity,
    rules: Option<&AccessRules>,
) -> Result<(), ApiError> {
    if state.authorizer.would_manage(who, rules) {
        return Ok(());
    }
    Err(ApiError::new(
        ErrorCode::Conflict,
        "this re-seed would remove your own manage privilege",
    )
    .with_hint(
        "keep a system_information write rule for yourself, or ask a user in \
         [access_control] admin_users",
    ))
}

/// Re-seeds the state from the config file for `who`, who needs manage
/// (also for a dry run). Shared with the MCP `reseed_state` tool.
pub(super) async fn reseed_for(
    state: &WebState,
    who: &Identity,
    dry_run: bool,
) -> Result<ManageOutcome<ReseedReport>, ApiError> {
    let grant = state.authorizer.check(who, Layer::Manage)?;
    let (store, live) = (store(state)?, live(state)?);
    let path = state.config_path.clone();
    let seed = tokio::task::spawn_blocking(move || load_seed(path.as_deref()))
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "config reload task failed");
            ApiError::new(ErrorCode::Internal, "the config file could not be loaded")
        })??;
    guard_lockout(state, who, seed.rules.as_ref().map(|(_, r)| r.as_ref()))?;
    // D10: build what differs from the live set before taking the lock.
    let now = installed(&live);
    let builds = seed
        .catalogs
        .iter()
        .filter(|(spec, json)| now.get(&spec.name) != Some(json))
        .map(|(spec, _)| {
            let live = Arc::clone(&live);
            async move {
                let built = live.build(spec).await.map_err(|e| e.to_string());
                (spec.name.clone(), built)
            }
        });
    let built: BTreeMap<String, Result<BuiltCatalog, String>> = futures::future::join_all(builds)
        .await
        .into_iter()
        .collect();
    let (lock, admission_lock) = if dry_run {
        (None, None)
    } else {
        let lock = Arc::new(Arc::clone(&state.config_changes).lock_owned().await);
        // Admission changes have their own lock (manage-api D9); taken
        // after config_changes, which no admission change takes.
        let admission_lock = Arc::clone(&state.admission_changes).lock_owned().await;
        (Some(lock), Some(admission_lock))
    };
    let action = Reseed {
        who: who.user.clone(),
        seed,
        built,
        store,
        live,
        handle: state.rules.clone(),
        admission: state.admission.clone(),
        lock,
        _admission_lock: admission_lock,
    };
    run_manage(state, who, grant, dry_run, action).await
}

struct Reseed {
    who: String,
    seed: Seed,
    /// The builds made before the lock, by catalog name.
    built: BTreeMap<String, Result<BuiltCatalog, String>>,
    store: Arc<StateStore>,
    live: Arc<LiveCatalogs>,
    handle: RulesHandle,
    /// The node's admission controller (every node that serves SQL).
    admission: Option<arneb_scheduler::AdmissionController>,
    lock: Option<ChangeLock>,
    _admission_lock: Option<tokio::sync::OwnedMutexGuard<()>>,
}

/// What applying the re-seed now does.
struct Plan {
    catalogs: CatalogDiff,
    rules_changed: bool,
    settings_reset: Vec<&'static str>,
    users: UserDiff,
    warnings: Vec<String>,
}

impl Plan {
    fn changed(&self) -> bool {
        self.catalogs != CatalogDiff::default()
            || self.rules_changed
            || !self.settings_reset.is_empty()
            || self.users != UserDiff::default()
    }

    fn summary(&self) -> String {
        if !self.changed() {
            return "the state already equals the config file; nothing to change".to_owned();
        }
        let list = |what: &str, names: &[String], parts: &mut Vec<String>| {
            if !names.is_empty() {
                parts.push(format!("{what} {}", names.join(", ")));
            }
        };
        let mut parts = Vec::new();
        list("adds catalogs", &self.catalogs.added, &mut parts);
        list("replaces catalogs", &self.catalogs.changed, &mut parts);
        list("removes catalogs", &self.catalogs.removed, &mut parts);
        if self.rules_changed {
            parts.push("replaces the access rules with the config's rules file".to_owned());
        }
        if !self.settings_reset.is_empty() {
            parts.push(format!(
                "resets {} to the config",
                self.settings_reset.join(", ")
            ));
        }
        list("adds users", &self.users.added, &mut parts);
        list(
            "resets the password of users",
            &self.users.updated,
            &mut parts,
        );
        list("removes users", &self.users.removed, &mut parts);
        list(
            "clears the tombstones of users",
            &self.users.tombstones_cleared,
            &mut parts,
        );
        format!(
            "re-seeds the state from the config file: {}; tokens, worker cordons and the \
             audit log are kept",
            parts.join("; ")
        )
    }

    fn parameters(&self) -> serde_json::Value {
        serde_json::json!({
            "catalogs": self.catalogs,
            "rules_changed": self.rules_changed,
            "settings_reset": self.settings_reset,
            "users": self.users,
        })
    }
}

fn persist_failed(what: &str, done: &[&str], e: arneb_state::StateError) -> ApiError {
    tracing::error!(error = %e, what, ?done, "cannot persist the re-seed");
    let done = if done.is_empty() {
        "nothing changed".to_owned()
    } else {
        format!("{} were already re-seeded", done.join(", "))
    };
    ApiError::new(
        ErrorCode::Internal,
        format!("the {what} could not be saved to the state store; {done}"),
    )
    .with_hint("retry the re-seed")
}

impl Reseed {
    fn config_rules(&self) -> Option<StoredRules> {
        self.seed.rules.as_ref().map(|(json, _)| StoredRules {
            json: json.clone(),
            from_config: true,
        })
    }

    fn config_specs(&self) -> Vec<(String, serde_json::Value)> {
        self.seed
            .catalogs
            .iter()
            .map(|(spec, json)| (spec.name.clone(), json.clone()))
            .collect()
    }

    /// The plan against the current state. Re-checks what the builds saw
    /// (D10): a config catalog that was not built must still be the
    /// installed one.
    fn plan(&self) -> Result<Plan, ApiError> {
        let mut warnings = Vec::new();
        let (_, records) = self.store.catalogs();
        let stored: BTreeMap<&str, &arneb_state::CatalogRecord> =
            records.iter().map(|r| (r.name.as_str(), r)).collect();
        let mut catalogs = CatalogDiff::default();
        let now = installed(&self.live);
        for (spec, json) in &self.seed.catalogs {
            let name = &spec.name;
            match stored.get(name.as_str()) {
                None => catalogs.added.push(name.clone()),
                Some(r) if r.spec != *json || !r.from_config => catalogs.changed.push(name.clone()),
                Some(_) => {}
            }
            match self.built.get(name) {
                None if now.get(name) != Some(json) => {
                    return Err(ApiError::new(
                        ErrorCode::Conflict,
                        format!("catalog {name:?} changed while the re-seed was building"),
                    )
                    .with_hint("retry"));
                }
                Some(Err(e)) => warnings.push(format!(
                    "catalog {name:?} could not be built on the coordinator ({e}): it is \
                     stored from the config but not served until it can be built (a restart, \
                     or a PUT of the catalog)"
                )),
                _ => {}
            }
        }
        catalogs.removed = records
            .iter()
            .filter(|r| !self.seed.catalogs.iter().any(|(s, _)| s.name == r.name))
            .map(|r| r.name.clone())
            .collect();

        let rules_changed = self.store.access_rules() != self.config_rules();

        let mut settings_reset = Vec::new();
        let config = self.seed.admission;
        let stored_admission = self.store.admission_limits();
        let live_admission = self.admission.as_ref().map(|c| {
            let s = c.snapshot();
            (s.max_running, s.max_queued)
        });
        if stored_admission.is_some()
            || live_admission.is_some_and(|a| a.0 != config.max_running_queries)
        {
            settings_reset.push("max_running_queries");
        }
        if stored_admission.is_some()
            || live_admission.is_some_and(|a| a.1 != config.max_queued_queries)
        {
            settings_reset.push("max_queued_queries");
        }
        if self.store.slow_query_threshold_ms().is_some()
            || super::settings::current_threshold_ms() != self.seed.slow_query_threshold_ms
        {
            settings_reset.push("slow_query_threshold_ms");
        }

        let seeded = self.store.preview_seed_users(&self.seed.users, true);
        let users = UserDiff {
            added: seeded.added,
            updated: seeded.updated,
            removed: seeded.removed,
            tombstones_cleared: self.store.tombstones(),
        };
        if users.removed.contains(&self.who) {
            return Err(ApiError::new(
                ErrorCode::Conflict,
                format!(
                    "this re-seed would delete your own user {:?}: it is not in the config file",
                    self.who
                ),
            )
            .with_hint(
                "add the user to [[auth.users]] in the config file, or re-seed as another \
                 manager",
            ));
        }
        if users.updated.contains(&self.who) {
            warnings.push(
                "your password becomes the config file's: your API tokens are revoked and \
                 your Web sessions end"
                    .to_owned(),
            );
        }
        Ok(Plan {
            catalogs,
            rules_changed,
            settings_reset,
            users,
            warnings,
        })
    }

    fn preview_of(plan: &Plan) -> Preview {
        Preview {
            summary: plan.summary(),
            changed: plan.changed(),
            warning: (!plan.warnings.is_empty()).then(|| plan.warnings.join("; ")),
        }
    }
}

impl ManageAction for Reseed {
    type Output = ReseedReport;

    fn action(&self) -> &'static str {
        "state.reseed"
    }

    fn target(&self) -> String {
        "state".to_owned()
    }

    /// Names only.
    fn parameters(&self) -> serde_json::Value {
        self.plan()
            .map(|p| p.parameters())
            .unwrap_or_else(|_| serde_json::json!({}))
    }

    fn persisted(&self) -> bool {
        true
    }

    fn preview(&self, _state: &WebState) -> Result<Preview, ApiError> {
        Ok(Self::preview_of(&self.plan()?))
    }

    fn change_lock(&self) -> Option<ChangeLock> {
        self.lock.clone()
    }

    /// Persists and swaps each object in turn (users, rules, settings,
    /// catalogs), then pushes the catalogs to the workers.
    fn apply(self, state: &WebState) -> Result<Applied<ReseedReport>, ApiError> {
        let plan = self.plan()?;
        let mut preview = Self::preview_of(&plan);
        let Plan {
            catalogs,
            rules_changed,
            settings_reset,
            users,
            mut warnings,
        } = plan;
        if !preview.changed {
            return Ok(Applied {
                preview,
                result: ReseedReport {
                    catalogs,
                    rules_changed,
                    settings_reset,
                    users,
                    warnings,
                    generation: None,
                    workers: Vec::new(),
                },
            });
        }
        let mut done = Vec::new();

        // Users: the store is the live credential source.
        let seeded = self
            .store
            .seed_users(&self.seed.users, true)
            .map_err(|e| persist_failed("users", &done, e))?;
        for user in seeded.updated.iter().chain(&seeded.removed) {
            if let Some(basic) = &state.basic {
                basic.forget_user(user);
            }
            state.sessions.end_user(user);
        }
        done.push("users");

        // Rules.
        let rules_text = self.seed.rules.as_ref().map(|(text, _)| text.as_str());
        self.store
            .seed_rules(rules_text, true)
            .map_err(|e| persist_failed("access rules", &done, e))?;
        self.handle
            .replace(self.seed.rules.as_ref().map(|(_, r)| Arc::clone(r)));
        done.push("access rules");

        // Settings: cleared, so the config values apply.
        self.store
            .clear_settings()
            .map_err(|e| persist_failed("settings", &done, e))?;
        if let Some(controller) = &self.admission {
            super::admission::apply_config_limits(controller, self.seed.admission);
        }
        arneb_common::metrics::set_slow_query_threshold(slow_query_threshold_from_ms(
            self.seed.slow_query_threshold_ms,
        ));
        done.push("settings");

        // Catalogs: stored as the config's, then installed at the store's
        // generation and pushed.
        self.store
            .seed_catalogs(&self.config_specs(), true)
            .map_err(|e| persist_failed("catalogs", &done, e))?;
        let generation = self.store.catalogs().0;
        let wanted: Vec<&str> = self
            .seed
            .catalogs
            .iter()
            .map(|(s, _)| s.name.as_str())
            .collect();
        for (name, _) in installed(&self.live) {
            if !wanted.contains(&name.as_str()) {
                self.live.remove(&name);
            }
        }
        let mut built = self.built;
        for (spec, _) in self.seed.catalogs {
            match built.remove(&spec.name) {
                Some(Ok(b)) => self.live.install(spec, b),
                // Not served, like a startup build failure.
                Some(Err(_)) => {
                    self.live.remove(&spec.name);
                }
                // Already installed with this spec (checked by the plan).
                None => {}
            }
        }
        self.live.set_generation(generation);
        let workers = push_to_workers(state, &self.live.snapshot());
        warnings.extend(push_warnings(&workers));
        if !warnings.is_empty() {
            preview.warning = Some(warnings.join("; "));
        }
        tracing::info!(
            target: "arneb::config",
            generation,
            catalogs_added = ?catalogs.added,
            catalogs_changed = ?catalogs.changed,
            catalogs_removed = ?catalogs.removed,
            rules_changed,
            settings_reset = ?settings_reset,
            users_added = ?users.added,
            users_updated = ?users.updated,
            users_removed = ?users.removed,
            tombstones_cleared = ?users.tombstones_cleared,
            revoked_tokens = seeded.revoked_tokens,
            workers = workers.len(),
            "state re-seeded from the config file"
        );
        Ok(Applied {
            preview,
            result: ReseedReport {
                catalogs,
                rules_changed,
                settings_reset,
                users,
                warnings,
                generation: Some(generation),
                workers,
            },
        })
    }
}

async fn post_reseed(
    State(state): State<WebState>,
    Extension(who): Extension<Identity>,
    params: Result<Query<DryRun>, QueryRejection>,
) -> Result<Json<ManageOutcome<ReseedReport>>, ApiError> {
    let Query(DryRun { dry_run }) = params.map_err(bad_query)?;
    Ok(Json(reseed_for(&state, &who, dry_run).await?))
}

/// The re-seed route, merged into `/api/v2`.
pub(super) fn routes() -> Router<WebState> {
    Router::new().route("/state/reseed", post(post_reseed))
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use arneb_catalog::CatalogManager;
    use arneb_connectors::memory::{MemoryCatalog, MemoryConnectorFactory};
    use arneb_connectors::ConnectorRegistry;
    use arneb_state::CatalogRecord;
    use axum::http::{Method, StatusCode};
    use serde_json::{json, Value};

    use super::*;
    use crate::catalogs::CatalogBuildError;
    use crate::web::settings::THRESHOLD_TEST_LOCK;
    use crate::web::v2::tests::{basic, call, web_state, with_authz};

    const PATH: &str = "/api/v2/state/reseed";
    /// The users `web_state` seeds, with their passwords.
    const USERS: [(&str, &str); 5] = [
        ("alice", "secret"),
        ("bob", "hunter2"),
        ("root", "rootpw"),
        ("ops", "opspw"),
        ("carol", "carolpw"),
    ];
    /// The config's rules: root and ops manage, everyone reads.
    const CONFIG_RULES: &str = r#"{"system_information":[{"user":"root|ops","allow":["read","write"]},{"allow":["read"]}]}"#;

    fn verifier(pw: &str) -> String {
        ScramVerifier::from_password(pw, b"0123456789abcdef", 4096).to_string()
    }

    fn fake_built() -> BuiltCatalog {
        let catalog = Arc::new(MemoryCatalog::new());
        BuiltCatalog {
            provider: catalog.clone(),
            connector: Arc::new(MemoryConnectorFactory::new(catalog, "default")),
        }
    }

    /// Writes a config file into `dir`: password auth with [`USERS`], the
    /// rules file `rules` (when `Some`), hive catalogs `lake` and `broken`.
    fn write_config(dir: &Path, rules: Option<&str>) -> PathBuf {
        let mut text = String::from("[auth]\ntype = \"password\"\n");
        for (name, pw) in USERS {
            text.push_str(&format!(
                "[[auth.users]]\nname = \"{name}\"\npassword_hash = \"{}\"\n",
                verifier(pw)
            ));
        }
        if let Some(rules) = rules {
            let rules_path = dir.join("rules.json");
            std::fs::write(&rules_path, rules).unwrap();
            text.push_str(&format!(
                "[access_control]\nrules_file = \"{}\"\n",
                rules_path.display()
            ));
        }
        for name in ["lake", "broken"] {
            text.push_str(&format!(
                "[[catalogs]]\nname = \"{name}\"\ntype = \"hive\"\nmetastore_uri = \"127.0.0.1:9083\"\n"
            ));
        }
        let path = dir.join("arneb.toml");
        std::fs::write(&path, text).unwrap();
        path
    }

    /// A node started from `config` (first-start seeded like `main.rs`),
    /// `admins` in admin_users, an admission controller, live catalogs
    /// whose build of `broken` fails.
    async fn node(config: &Path, admins: &[&str]) -> WebState {
        let mut state = with_authz(web_state(true, None), Some(CONFIG_RULES), admins);
        let live = LiveCatalogs::with_test_builder(
            Arc::new(CatalogManager::new("lake", "default")),
            Arc::new(ConnectorRegistry::new()),
            Box::new(|spec| {
                if spec.name == "broken" {
                    Err(CatalogBuildError::Unreachable("refused".into()))
                } else {
                    Ok(fake_built())
                }
            }),
        );
        let seed = load_seed(Some(config)).unwrap();
        let store = state.state.clone().unwrap();
        store
            .seed_rules(seed.rules.as_ref().map(|(t, _)| t.as_str()), false)
            .unwrap();
        let specs: Vec<(String, Value)> = seed
            .catalogs
            .iter()
            .map(|(s, j)| (s.name.clone(), j.clone()))
            .collect();
        store.seed_catalogs(&specs, false).unwrap();
        let (generation, _) = store.catalogs();
        live.apply_set(
            generation,
            seed.catalogs.iter().map(|(s, _)| s.clone()).collect(),
        )
        .await;
        state.catalogs = Some(live);
        state.admission = Some(arneb_scheduler::AdmissionController::new(8, 256));
        state.config_path = Some(config.to_owned());
        state
    }

    /// Runtime changes: a runtime catalog, runtime rules, stored admission
    /// limits, a tombstoned config user and a stored slow-query threshold.
    fn change_at_runtime(state: &WebState) {
        let store = state.state.as_ref().unwrap();
        let spec: CatalogConfig = serde_json::from_value(json!({
            "name": "rt", "type": "hive", "metastore_uri": "10.0.0.1:9083"
        }))
        .unwrap();
        store
            .put_catalog(CatalogRecord {
                name: "rt".into(),
                spec: serde_json::to_value(&spec).unwrap(),
                from_config: false,
            })
            .unwrap();
        let live = state.catalogs.as_ref().unwrap();
        live.install(spec, fake_built());
        live.set_generation(store.catalogs().0);
        let runtime_rules =
            r#"{"system_information":[{"user":"root|ops","allow":["read","write"]}]}"#;
        store
            .set_access_rules(Some(StoredRules {
                json: runtime_rules.into(),
                from_config: false,
            }))
            .unwrap();
        state.rules.replace(Some(Arc::new(
            AccessRules::from_json(runtime_rules).unwrap(),
        )));
        let limits = AdmissionLimits {
            max_running_queries: 2,
            max_queued_queries: 7,
        };
        store.set_admission_limits(limits).unwrap();
        state.admission.as_ref().unwrap().set_limits(2, 7);
        store.set_slow_query_threshold_ms(123).unwrap();
        arneb_common::metrics::set_slow_query_threshold(slow_query_threshold_from_ms(123));
        store.delete_user("carol").unwrap();
    }

    /// Everything a re-seed compares, without generations.
    #[derive(Debug, PartialEq)]
    struct Observed {
        users: Vec<(String, bool)>,
        tombstones: Vec<String>,
        rules: Option<StoredRules>,
        catalogs: Vec<CatalogRecord>,
        admission: Option<AdmissionLimits>,
        slow_query: Option<u64>,
    }

    fn observe(store: &StateStore) -> Observed {
        Observed {
            users: store
                .list_users()
                .into_iter()
                .map(|u| (u.name, u.from_config))
                .collect(),
            tombstones: store.tombstones(),
            rules: store.access_rules(),
            catalogs: store.catalogs().1,
            admission: store.admission_limits(),
            slow_query: store.slow_query_threshold_ms(),
        }
    }

    fn audit_actions(state: &WebState) -> Vec<(String, Value)> {
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
            .map(|e| (e.record.action, e.record.parameters))
            .collect()
    }

    async fn reseed(state: &WebState, user: (&str, &str), dry_run: bool) -> (StatusCode, Value) {
        let path = if dry_run {
            format!("{PATH}?dry_run=true")
        } else {
            PATH.to_owned()
        };
        call(
            state,
            Method::POST,
            &path,
            Some(&basic(user.0, user.1)),
            None,
        )
        .await
    }

    #[tokio::test]
    async fn reseed_dry_run_lists_every_change() {
        let _g = THRESHOLD_TEST_LOCK.lock().await;
        let tmp = tempfile::tempdir().unwrap();
        let config = write_config(tmp.path(), Some(CONFIG_RULES));
        let state = node(&config, &["root"]).await;
        change_at_runtime(&state);
        let store = state.state.clone().unwrap();
        let before = observe(&store);
        let generation = store.catalogs().0;

        let (status, body) = reseed(&state, ("root", "rootpw"), true).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["dry_run"], true);
        assert_eq!(body["action"], "state.reseed");
        assert_eq!(body["target"], "state");
        assert_eq!(body["changed"], true);
        assert!(body.get("result").is_none(), "{body}");
        let summary = body["summary"].as_str().unwrap();
        for part in [
            "removes catalogs rt",
            "replaces the access rules",
            "max_running_queries",
            "max_queued_queries",
            "slow_query_threshold_ms",
            "adds users carol",
            "clears the tombstones of users carol",
        ] {
            assert!(summary.contains(part), "{part:?} missing: {summary}");
        }
        let warning = body["warning"].as_str().unwrap();
        assert!(
            warning.contains("catalog \"broken\" could not be built"),
            "{warning}"
        );

        // Nothing changed.
        assert_eq!(observe(&store), before);
        assert_eq!(store.catalogs().0, generation);
        let live = state.catalogs.as_ref().unwrap();
        assert!(live.specs().iter().any(|s| s.name == "rt"));
        let snap = state.admission.as_ref().unwrap().snapshot();
        assert_eq!((snap.max_running, snap.max_queued), (2, 7));
        assert_eq!(super::super::settings::current_threshold_ms(), 123);
        assert!(audit_actions(&state).is_empty());
        arneb_common::metrics::set_slow_query_threshold(slow_query_threshold_from_ms(10_000));
    }

    #[tokio::test]
    async fn reseed_restores_config() {
        let _g = THRESHOLD_TEST_LOCK.lock().await;
        let tmp = tempfile::tempdir().unwrap();
        let config = write_config(tmp.path(), Some(CONFIG_RULES));
        let state = node(&config, &["root"]).await;
        change_at_runtime(&state);
        // bob's password was changed at runtime: the re-seed restores the
        // config's and ends his Web sessions; alice's survive.
        let store = state.state.clone().unwrap();
        store
            .set_user_password(
                "bob",
                ScramVerifier::from_password("changed", b"0123456789abcdef", 4096),
            )
            .unwrap();
        let kept = state.sessions.create("alice").unwrap();
        let ended = state.sessions.create("bob").unwrap();

        let (status, body) = reseed(&state, ("root", "rootpw"), false).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["changed"], true);
        let result = &body["result"];
        assert_eq!(
            result["catalogs"],
            json!({"added": [], "changed": [], "removed": ["rt"]})
        );
        assert_eq!(result["rules_changed"], true);
        assert_eq!(
            result["settings_reset"],
            json!([
                "max_running_queries",
                "max_queued_queries",
                "slow_query_threshold_ms"
            ])
        );
        assert_eq!(
            result["users"],
            json!({"added": ["carol"], "updated": ["bob"], "removed": [],
                   "tombstones_cleared": ["carol"]})
        );
        assert!(
            result["warnings"][0]
                .as_str()
                .unwrap()
                .contains("\"broken\""),
            "{result}"
        );
        assert!(!body.to_string().contains("9083"), "no specs: {body}");

        // The state equals a first-start seed of the same config.
        let fresh = StateStore::open(None).unwrap();
        let seed = load_seed(Some(&config)).unwrap();
        fresh.seed_users(&seed.users, false).unwrap();
        fresh
            .seed_rules(seed.rules.as_ref().map(|(t, _)| t.as_str()), false)
            .unwrap();
        let specs: Vec<(String, Value)> = seed
            .catalogs
            .iter()
            .map(|(s, j)| (s.name.clone(), j.clone()))
            .collect();
        fresh.seed_catalogs(&specs, false).unwrap();
        assert_eq!(observe(&store), observe(&fresh));

        // The live state follows: catalogs at the store's generation (the
        // broken one not served), rules, admission, threshold.
        let live = state.catalogs.as_ref().unwrap();
        let names: Vec<String> = live.specs().into_iter().map(|s| s.name).collect();
        assert_eq!(names, vec!["lake"]);
        assert_eq!(live.generation(), store.catalogs().0);
        assert_eq!(result["generation"], store.catalogs().0);
        assert_eq!(
            state.rules.current().unwrap().section_sizes(),
            AccessRules::from_json(CONFIG_RULES)
                .unwrap()
                .section_sizes()
        );
        let snap = state.admission.as_ref().unwrap().snapshot();
        assert_eq!((snap.max_running, snap.max_queued), (8, 256));
        assert_eq!(super::super::settings::current_threshold_ms(), 10_000);
        assert_eq!(state.sessions.resolve(&kept).as_deref(), Some("alice"));
        assert_eq!(state.sessions.resolve(&ended), None);

        let audit = audit_actions(&state);
        assert_eq!(audit.len(), 1);
        assert_eq!(audit[0].0, "state.reseed");
        assert_eq!(audit[0].1["effect"], "applied");
        assert_eq!(audit[0].1["catalogs"]["removed"], json!(["rt"]));

        // A second re-seed is a no-op.
        let (status, again) = reseed(&state, ("root", "rootpw"), false).await;
        assert_eq!(status, StatusCode::OK, "{again}");
        assert_eq!(again["changed"], false, "{again}");
    }

    #[tokio::test]
    async fn reseed_with_broken_config_is_invalid_argument() {
        let tmp = tempfile::tempdir().unwrap();
        let config = write_config(tmp.path(), None);
        let state = node(&config, &["root"]).await;
        let store = state.state.clone().unwrap();
        let before = observe(&store);

        // A parse error on a line that holds a secret.
        std::fs::write(
            &config,
            "[auth]\ntype = \"password\"\nsecret_access_key = \"S3CRET-zz91\" oops\n",
        )
        .unwrap();
        for dry_run in [true, false] {
            let (status, body) = reseed(&state, ("root", "rootpw"), dry_run).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
            assert_eq!(body["error"]["code"], "invalid_argument");
            let message = body["error"]["message"].as_str().unwrap();
            assert!(message.contains("line 3"), "{message}");
            assert!(!body.to_string().contains("S3CRET"), "{body}");
        }

        // A missing file.
        let mut missing = state.clone();
        missing.config_path = Some(tmp.path().join("missing.toml"));
        let (status, body) = reseed(&missing, ("root", "rootpw"), false).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(body["error"]["code"], "invalid_argument");

        // [auth] type none would remove every user.
        std::fs::write(&config, "port = 5432\n").unwrap();
        let (status, body) = reseed(&state, ("root", "rootpw"), false).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");

        assert_eq!(observe(&store), before);
        assert!(audit_actions(&state).is_empty());
    }

    #[tokio::test]
    async fn reseed_that_drops_own_manage_is_refused() {
        let tmp = tempfile::tempdir().unwrap();
        // The config's rules leave only root managing.
        let config_rules = r#"{"system_information":[{"user":"root","allow":["read","write"]}]}"#;
        let config = write_config(tmp.path(), Some(config_rules));
        // Nobody in admin_users; ops manages through the runtime rules.
        let state = node(&config, &[]).await;
        let store = state.state.clone().unwrap();
        let ops_rules = r#"{"system_information":[{"user":"root|ops","allow":["read","write"]}]}"#;
        store
            .set_access_rules(Some(StoredRules {
                json: ops_rules.into(),
                from_config: false,
            }))
            .unwrap();
        state
            .rules
            .replace(Some(Arc::new(AccessRules::from_json(ops_rules).unwrap())));
        let before = observe(&store);
        for dry_run in [true, false] {
            let (status, body) = reseed(&state, ("ops", "opspw"), dry_run).await;
            assert_eq!(status, StatusCode::CONFLICT, "{body}");
            assert_eq!(body["error"]["code"], "conflict");
            assert_eq!(
                body["error"]["hint"],
                "keep a system_information write rule for yourself, or ask a user in \
                 [access_control] admin_users"
            );
        }
        assert_eq!(observe(&store), before);
        assert!(audit_actions(&state).is_empty());

        // A non-manager is forbidden, dry run or not.
        let (status, body) = reseed(&state, ("alice", "secret"), true).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    }
}
