//! Access rules on `/api/v2` (runtime-config D3): read the stored rules,
//! and replace or clear them at runtime as a manage action.
//!
//! A change is persisted in the state store before it is applied to the
//! live [`arneb_catalog::RulesHandle`], so it applies to every statement
//! planned after the response, on every protocol, including connections
//! already open. The audit entry names the rules by SHA-256 only.

use std::sync::Arc;

use arneb_catalog::{AccessRules, RulesHandle};
use arneb_state::{StateStore, StoredRules};
use axum::body::Bytes;
use axum::extract::rejection::{BytesRejection, QueryRejection};
use axum::extract::{Query, State};
use axum::response::Json;
use axum::routing::get;
use axum::{Extension, Router};
use serde::Serialize;

use super::auth::Identity;
use super::error::{ApiError, ErrorCode};
use super::manage::{
    run_manage, Applied, ChangeLock, DryRun, ManageAction, ManageOutcome, Preview,
};
use super::privilege::Layer;
use super::v2::{bad_body, bad_query};
use super::WebState;

/// The stored access rules.
#[derive(Debug, Serialize)]
pub(super) struct RulesView {
    /// The rules JSON, or `null` when there are none (allow everything).
    rules: Option<serde_json::Value>,
    /// Whether the stored rules were seeded from the config's rules file;
    /// `false` when they were set at runtime or there are none.
    from_config: bool,
}

/// The `warning` of a `DELETE`.
const ALLOW_ALL_WARNING: &str = "with no access rules every user may read every catalog, \
     schema and table and run any statement (allow all), and only [access_control] \
     admin_users keep the manage privilege";

fn store(state: &WebState) -> Result<Arc<StateStore>, ApiError> {
    state
        .state
        .clone()
        .ok_or_else(|| ApiError::new(ErrorCode::Internal, "access rules need the state store"))
}

fn view(stored: Option<StoredRules>) -> RulesView {
    match stored {
        Some(r) => RulesView {
            // Validated before it was stored, so it is JSON.
            rules: serde_json::from_str(&r.json).ok(),
            from_config: r.from_config,
        },
        None => RulesView {
            rules: None,
            from_config: false,
        },
    }
}

fn hex_sha256(text: &str) -> String {
    arneb_state::sha256(text.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// The stored access rules. Needs manage. Shared with the MCP
/// `get_access_rules` tool.
pub(super) async fn get_rules_for(state: &WebState, who: &Identity) -> Result<RulesView, ApiError> {
    state.authorizer.check(who, Layer::Manage)?;
    Ok(view(store(state)?.access_rules()))
}

/// The lockout guard: refuses a change after which `who` would no longer
/// hold manage. Part of validation, so a dry run is refused the same way.
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
        "this change would remove your own manage privilege",
    )
    .with_hint(
        "keep a system_information write rule for yourself, or ask a user in \
         [access_control] admin_users",
    ))
}

/// Replaces the access rules with `json` (a full rules file) for `who`,
/// who needs manage (also for a dry run). Shared with the MCP
/// `set_access_rules` tool.
pub(super) async fn set_rules_for(
    state: &WebState,
    who: &Identity,
    dry_run: bool,
    json: String,
) -> Result<ManageOutcome<RulesView>, ApiError> {
    let grant = state.authorizer.check(who, Layer::Manage)?;
    let rules = AccessRules::from_json(&json).map_err(|e| {
        ApiError::new(ErrorCode::InvalidArgument, e.to_string()).with_hint(
            "send a full rules file in Trino's file-based access control format \
             (catalogs, schemas, tables, queries, system_information)",
        )
    })?;
    guard_lockout(state, who, Some(&rules))?;
    change(state, who, grant, dry_run, Some((json, Arc::new(rules)))).await
}

/// Removes every access rule for `who`, who needs manage (also for a dry
/// run): the query layer then allows everything. Shared with the MCP
/// `clear_access_rules` tool.
pub(super) async fn clear_rules_for(
    state: &WebState,
    who: &Identity,
    dry_run: bool,
) -> Result<ManageOutcome<RulesView>, ApiError> {
    let grant = state.authorizer.check(who, Layer::Manage)?;
    guard_lockout(state, who, None)?;
    change(state, who, grant, dry_run, None).await
}

async fn change(
    state: &WebState,
    who: &Identity,
    grant: super::privilege::Grant,
    dry_run: bool,
    to: Option<(String, Arc<AccessRules>)>,
) -> Result<ManageOutcome<RulesView>, ApiError> {
    let store = store(state)?;
    // runtime-config D10: held from reading `from` until the audit entry is
    // appended. The action keeps a share, so a request dropped mid-call
    // still serializes its apply.
    let serialized = Arc::new(Arc::clone(&state.config_changes).lock_owned().await);
    let action = ChangeRules {
        from: store.access_rules().map(|r| hex_sha256(&r.json)),
        to,
        store,
        handle: state.rules.clone(),
        _serialized: Arc::clone(&serialized),
    };
    let outcome = run_manage(state, who, grant, dry_run, action).await;
    drop(serialized);
    outcome
}

struct ChangeRules {
    /// SHA-256 of the stored rules when the action was built, under
    /// [`WebState::config_changes`].
    from: Option<String>,
    /// The new rules (text and parsed), or `None` to clear them.
    to: Option<(String, Arc<AccessRules>)>,
    store: Arc<StateStore>,
    handle: RulesHandle,
    _serialized: ChangeLock,
}

/// `catalogs 2, schemas absent, …` for a summary.
fn sections(rules: &AccessRules) -> String {
    const NAMES: [&str; 5] = [
        "catalogs",
        "schemas",
        "tables",
        "queries",
        "system_information",
    ];
    NAMES
        .iter()
        .zip(rules.section_sizes())
        .map(|(name, n)| match n {
            Some(n) => format!("{name} {n}"),
            None => format!("{name} absent"),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

impl ChangeRules {
    fn plan(&self) -> Preview {
        let stored = self.store.access_rules();
        match &self.to {
            Some((json, rules)) => {
                if stored.as_ref().is_some_and(|s| &s.json == json) {
                    return Preview {
                        summary: "the access rules are already this text; nothing to change".into(),
                        changed: false,
                        warning: None,
                    };
                }
                let verb = if stored.is_some() { "replaces" } else { "sets" };
                Preview {
                    summary: format!(
                        "{verb} the access rules ({}); they apply to the next statement \
                         on every connection",
                        sections(rules)
                    ),
                    changed: true,
                    warning: None,
                }
            }
            None => Preview {
                summary: if stored.is_some() {
                    "removes all access rules; they stop applying from the next statement \
                     on every connection"
                        .into()
                } else {
                    "there are no access rules; nothing to change".into()
                },
                changed: stored.is_some(),
                warning: Some(ALLOW_ALL_WARNING.into()),
            },
        }
    }
}

fn persist_failed(e: arneb_state::StateError) -> ApiError {
    tracing::error!(error = %e, "cannot persist the access rules");
    ApiError::new(
        ErrorCode::Internal,
        "the access rules could not be saved to the state store; nothing changed",
    )
}

impl ManageAction for ChangeRules {
    type Output = RulesView;

    fn action(&self) -> &'static str {
        if self.to.is_some() {
            "access_rules.set"
        } else {
            "access_rules.clear"
        }
    }

    fn target(&self) -> String {
        "access_rules".to_owned()
    }

    /// The rules by SHA-256 only, never their text.
    fn parameters(&self) -> serde_json::Value {
        let mut map = serde_json::Map::new();
        map.insert("from_sha256".into(), self.from.clone().into());
        if let Some((json, _)) = &self.to {
            map.insert("sha256".into(), hex_sha256(json).into());
        }
        serde_json::Value::Object(map)
    }

    fn persisted(&self) -> bool {
        true
    }

    fn preview(&self, _state: &WebState) -> Result<Preview, ApiError> {
        Ok(self.plan())
    }

    fn change_lock(&self) -> Option<ChangeLock> {
        Some(Arc::clone(&self._serialized))
    }

    /// Persists first, then swaps the live rules, so a failed write changes
    /// nothing.
    fn apply(self, _state: &WebState) -> Result<Applied<RulesView>, ApiError> {
        let preview = self.plan();
        if preview.changed {
            let stored = self.to.as_ref().map(|(json, _)| StoredRules {
                json: json.clone(),
                from_config: false,
            });
            self.store
                .set_access_rules(stored)
                .map_err(persist_failed)?;
            self.handle
                .replace(self.to.as_ref().map(|(_, rules)| Arc::clone(rules)));
            tracing::info!(
                target: "arneb::config",
                sha256 = self.to.as_ref().map(|(json, _)| hex_sha256(json)),
                "access rules changed at runtime"
            );
        }
        Ok(Applied {
            preview,
            result: view(self.store.access_rules()),
        })
    }
}

async fn get_rules(
    State(state): State<WebState>,
    Extension(who): Extension<Identity>,
) -> Result<Json<RulesView>, ApiError> {
    Ok(Json(get_rules_for(&state, &who).await?))
}

async fn put_rules(
    State(state): State<WebState>,
    Extension(who): Extension<Identity>,
    params: Result<Query<DryRun>, QueryRejection>,
    body: Result<Bytes, BytesRejection>,
) -> Result<Json<ManageOutcome<RulesView>>, ApiError> {
    let Query(DryRun { dry_run }) = params.map_err(bad_query)?;
    let body = body.map_err(bad_body)?;
    let json = String::from_utf8(body.to_vec()).map_err(|_| {
        ApiError::new(ErrorCode::InvalidArgument, "the body is not UTF-8")
            .with_hint("send the rules file as JSON")
    })?;
    Ok(Json(set_rules_for(&state, &who, dry_run, json).await?))
}

async fn delete_rules(
    State(state): State<WebState>,
    Extension(who): Extension<Identity>,
    params: Result<Query<DryRun>, QueryRejection>,
) -> Result<Json<ManageOutcome<RulesView>>, ApiError> {
    let Query(DryRun { dry_run }) = params.map_err(bad_query)?;
    Ok(Json(clear_rules_for(&state, &who, dry_run).await?))
}

/// The access rules routes, merged into `/api/v2`.
pub(super) fn routes() -> Router<WebState> {
    Router::new().route(
        "/access-rules",
        get(get_rules).put(put_rules).delete(delete_rules),
    )
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::{header, Method, StatusCode};
    use serde_json::{json, Value};
    use tower::ServiceExt as _;

    use super::*;
    use crate::web::build_router;
    use crate::web::v2::tests::{basic, call, web_state, with_authz};

    const PATH: &str = "/api/v2/access-rules";
    /// carol manages only through this `system_information` rule.
    const CAROL_MANAGES: &str =
        r#"{"system_information":[{"user":"carol","allow":["read","write"]}]}"#;
    const OPS_MANAGES: &str = r#"{"system_information":[{"user":"ops","allow":["write"]}]}"#;

    /// Password auth, `root` in admin_users, carol managing by rule; the
    /// rules are also stored, as a startup seed would leave them.
    fn node() -> WebState {
        let state = with_authz(web_state(true, None), Some(CAROL_MANAGES), &["root"]);
        let store = state.state.as_ref().unwrap();
        store.seed_rules(Some(CAROL_MANAGES), false).unwrap();
        state
    }

    fn root() -> String {
        basic("root", "rootpw")
    }

    fn carol() -> String {
        basic("carol", "carolpw")
    }

    /// A request with a raw (not necessarily JSON) body.
    async fn raw(
        state: &WebState,
        method: Method,
        path: &str,
        auth: &str,
        body: &str,
    ) -> (StatusCode, Value) {
        let req = axum::http::Request::builder()
            .method(method)
            .uri(path)
            .header(header::AUTHORIZATION, auth)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(body.to_owned()))
            .unwrap();
        let resp = build_router(state.clone()).oneshot(req).await.unwrap();
        let status = resp.status();
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    fn audit_entries(state: &WebState) -> Vec<arneb_state::AuditRecord> {
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
            .map(|e| e.record)
            .collect()
    }

    fn stored_json(state: &WebState) -> Option<String> {
        state.state.as_ref().unwrap().access_rules().map(|r| r.json)
    }

    #[tokio::test]
    async fn get_rules_returns_the_stored_json() {
        let state = node();
        let (status, body) = call(&state, Method::GET, PATH, Some(&root()), None).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(
            body,
            json!({
                "rules": serde_json::from_str::<Value>(CAROL_MANAGES).unwrap(),
                "from_config": true,
            })
        );
        let (status, body) = call(
            &state,
            Method::GET,
            PATH,
            Some(&basic("bob", "hunter2")),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    }

    #[tokio::test]
    async fn put_rules_requires_manage() {
        let state = node();
        let bob = basic("bob", "hunter2");
        for path in [PATH.to_owned(), format!("{PATH}?dry_run=true")] {
            let (status, body) = raw(&state, Method::PUT, &path, &bob, OPS_MANAGES).await;
            assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
            assert_eq!(body["error"]["code"], "forbidden");
            let (status, body) = raw(&state, Method::DELETE, &path, &bob, "").await;
            assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
            assert_eq!(body["error"]["code"], "forbidden");
        }
        assert_eq!(stored_json(&state).as_deref(), Some(CAROL_MANAGES));
        assert!(audit_entries(&state).is_empty());
    }

    #[tokio::test]
    async fn put_rules_under_none_needs_auth() {
        let state = with_authz(web_state(false, None), None, &["root"]);
        let (status, body) = raw(&state, Method::PUT, PATH, &root(), OPS_MANAGES).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
        assert_eq!(body["error"]["code"], "manage_requires_auth");
        let (status, body) = raw(&state, Method::DELETE, PATH, &root(), "").await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
        assert_eq!(body["error"]["code"], "manage_requires_auth");
        assert!(state.rules.current().is_none());
    }

    #[tokio::test]
    async fn put_rules_bad_json_is_invalid_argument() {
        let state = node();
        for bad in [
            "{not json",
            "",
            r#"{"catalogs":[{"group":"x","allow":"all"}]}"#,
            r#"{"impersonation":[]}"#,
        ] {
            let (status, body) = raw(&state, Method::PUT, PATH, &root(), bad).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{bad}: {body}");
            assert_eq!(body["error"]["code"], "invalid_argument", "{bad}");
            let message = body["error"]["message"].as_str().unwrap();
            let expected = AccessRules::from_json(bad).unwrap_err().to_string();
            assert_eq!(message, expected, "the parser's message");
        }
        assert_eq!(stored_json(&state).as_deref(), Some(CAROL_MANAGES));
        assert!(audit_entries(&state).is_empty());
    }

    #[tokio::test]
    async fn put_rules_dry_run_changes_nothing() {
        let state = node();
        let before = state.rules.current().unwrap();
        let path = format!("{PATH}?dry_run=true");
        let (status, body) = raw(&state, Method::PUT, &path, &root(), OPS_MANAGES).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["dry_run"], true);
        assert_eq!(body["action"], "access_rules.set");
        assert_eq!(body["target"], "access_rules");
        assert_eq!(body["changed"], true);
        assert!(body.get("result").is_none(), "{body}");
        let summary = body["summary"].as_str().unwrap();
        assert!(
            summary.starts_with(
                "replaces the access rules (catalogs absent, schemas absent, tables absent, \
                 queries absent, system_information 1)"
            ),
            "{summary}"
        );
        assert_eq!(stored_json(&state).as_deref(), Some(CAROL_MANAGES));
        assert!(Arc::ptr_eq(&before, &state.rules.current().unwrap()));
        assert!(audit_entries(&state).is_empty());
    }

    #[tokio::test]
    async fn put_rules_applies_and_audits() {
        let state = node();
        let ops = basic("ops", "opspw");
        let (status, _) = call(&state, Method::GET, PATH, Some(&ops), None).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "ops does not manage yet");

        let (status, body) = raw(&state, Method::PUT, PATH, &root(), OPS_MANAGES).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["dry_run"], false);
        assert_eq!(body["changed"], true);
        assert_eq!(
            body["result"],
            json!({
                "rules": serde_json::from_str::<Value>(OPS_MANAGES).unwrap(),
                "from_config": false,
            })
        );
        // Persisted, then live for every holder of the handle, including
        // the authorizer: ops now manages and carol no longer does.
        assert_eq!(stored_json(&state).as_deref(), Some(OPS_MANAGES));
        let live = state.rules.current().unwrap();
        assert!(live.system_information_write("ops"));
        let (status, _) = call(&state, Method::GET, PATH, Some(&ops), None).await;
        assert_eq!(status, StatusCode::OK);
        let (status, _) = call(&state, Method::GET, PATH, Some(&carol()), None).await;
        assert_eq!(status, StatusCode::FORBIDDEN);

        let entries = audit_entries(&state);
        assert_eq!(entries.len(), 1);
        let e = &entries[0];
        assert_eq!(e.action, "access_rules.set");
        assert_eq!(e.target, "access_rules");
        assert_eq!(e.user, "root");
        assert_eq!(
            e.parameters,
            json!({
                "from_sha256": hex_sha256(CAROL_MANAGES),
                "sha256": hex_sha256(OPS_MANAGES),
                "effect": "applied",
            })
        );
        let text = e.parameters.to_string();
        assert!(!text.contains("system_information"), "{text}");
        assert_eq!(hex_sha256(OPS_MANAGES).len(), 64);

        // The same text again is a no-op.
        let (status, again) = raw(&state, Method::PUT, PATH, &root(), OPS_MANAGES).await;
        assert_eq!(status, StatusCode::OK, "{again}");
        assert_eq!(again["changed"], false, "{again}");
        assert_eq!(audit_entries(&state)[0].parameters["effect"], "no_change");
    }

    #[tokio::test]
    async fn delete_rules_warns_allow_all() {
        let state = node();
        let path = format!("{PATH}?dry_run=true");
        let (status, dry) = raw(&state, Method::DELETE, &path, &root(), "").await;
        assert_eq!(status, StatusCode::OK, "{dry}");
        assert_eq!(dry["action"], "access_rules.clear");
        assert_eq!(dry["changed"], true);
        let warning = dry["warning"].as_str().unwrap();
        assert!(warning.starts_with(ALLOW_ALL_WARNING), "{warning}");
        assert!(warning.contains("allow all"), "{warning}");
        assert!(state.rules.current().is_some());

        let (status, body) = raw(&state, Method::DELETE, PATH, &root(), "").await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["changed"], true);
        assert!(body["warning"].as_str().unwrap().contains("allow all"));
        assert_eq!(body["result"], json!({"rules": null, "from_config": false}));
        assert!(state.rules.current().is_none());
        assert_eq!(stored_json(&state), None);
        let entries = audit_entries(&state);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].action, "access_rules.clear");
        assert_eq!(
            entries[0].parameters,
            json!({"from_sha256": hex_sha256(CAROL_MANAGES), "effect": "applied"})
        );

        let (status, again) = raw(&state, Method::DELETE, PATH, &root(), "").await;
        assert_eq!(status, StatusCode::OK, "{again}");
        assert_eq!(again["changed"], false, "{again}");
    }

    #[tokio::test]
    async fn rules_change_that_drops_own_manage_is_refused() {
        let state = node();
        let hint = "keep a system_information write rule for yourself, or ask a user in \
                    [access_control] admin_users";
        for path in [PATH.to_owned(), format!("{PATH}?dry_run=true")] {
            let (status, body) = raw(&state, Method::PUT, &path, &carol(), OPS_MANAGES).await;
            assert_eq!(status, StatusCode::CONFLICT, "{path}: {body}");
            assert_eq!(body["error"]["code"], "conflict");
            assert_eq!(body["error"]["hint"], hint);
            let (status, body) = raw(&state, Method::DELETE, &path, &carol(), "").await;
            assert_eq!(status, StatusCode::CONFLICT, "{path}: {body}");
            assert_eq!(body["error"]["code"], "conflict");
        }
        // Read-only for herself is a lockout too.
        let read_only = r#"{"system_information":[{"user":"carol","allow":["read"]}]}"#;
        let (status, _) = raw(&state, Method::PUT, PATH, &carol(), read_only).await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(stored_json(&state).as_deref(), Some(CAROL_MANAGES));
        assert!(audit_entries(&state).is_empty());

        // Keeping her own write rule is fine.
        let both = r#"{"system_information":[{"user":"carol|ops","allow":["write"]}]}"#;
        let (status, body) = raw(&state, Method::PUT, PATH, &carol(), both).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(stored_json(&state).as_deref(), Some(both));
    }

    #[tokio::test]
    async fn admin_user_is_never_locked_out() {
        let state = node();
        let (status, body) = raw(&state, Method::PUT, PATH, &root(), OPS_MANAGES).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let (status, body) = raw(&state, Method::DELETE, PATH, &root(), "").await;
        assert_eq!(status, StatusCode::OK, "{body}");
        // With no rules at all root still manages.
        let (status, body) = call(&state, Method::GET, PATH, Some(&root()), None).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body, json!({"rules": null, "from_config": false}));
    }

    /// A request dropped while its change runs still holds the change lock
    /// until the audit entry is appended.
    #[tokio::test]
    async fn dropped_request_holds_the_config_lock_until_audited() {
        let state = node();
        let (inside, release) =
            crate::web::manage::test_hooks::stop_after_apply(&state.config_changes);
        let call = tokio::spawn({
            let state = state.clone();
            async move { raw(&state, Method::PUT, PATH, &root(), OPS_MANAGES).await }
        });
        tokio::task::spawn_blocking(move || inside.recv().unwrap())
            .await
            .unwrap();
        call.abort();
        assert!(call.await.unwrap_err().is_cancelled());
        assert!(state.config_changes.try_lock().is_err());
        assert!(audit_entries(&state).is_empty());
        release.send(()).unwrap();
        let _next = state.config_changes.lock().await;
        assert_eq!(audit_entries(&state).len(), 1);
    }
}
