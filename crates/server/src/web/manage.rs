//! The manage runner (manage-api D4, D5): every manage route and MCP tool
//! describes its change as a [`ManageAction`] and runs it through
//! [`run_manage`], which answers a dry run from the action's preview and a
//! real run by applying it, audited, off the async workers.

use serde::{Deserialize, Serialize};

use super::audit::{audited, AuditSubject};
use super::auth::Identity;
use super::error::{ApiError, ErrorCode};
use super::privilege::Grant;
use super::WebState;

/// The `?dry_run=true|false` query parameter of every manage route
/// (default `false`; any other value is `invalid_argument`).
#[derive(Deserialize, Default)]
pub(super) struct DryRun {
    #[serde(default)]
    pub dry_run: bool,
}

/// What an action does (or did): the envelope's `summary`, `changed` and
/// `warning`.
pub(super) struct Preview {
    pub summary: String,
    /// False for a no-op, such as cancelling an ended query.
    pub changed: bool,
    pub warning: Option<String>,
}

/// An applied action: what it did, and its result.
pub(super) struct Applied<T> {
    pub preview: Preview,
    pub result: T,
}

/// The manage response envelope (D5), HTTP `200` on dry and real runs.
#[derive(Serialize)]
pub(super) struct ManageOutcome<T> {
    pub dry_run: bool,
    pub action: &'static str,
    pub target: String,
    pub summary: String,
    pub changed: bool,
    pub warning: Option<String>,
    /// Absent on a dry run.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<T>,
}

/// One manage change, already authorized and validated when it is built.
pub(super) trait ManageAction: Send + 'static {
    /// The `result` of a real run.
    type Output: Serialize + Send + 'static;

    /// The D5 action name, such as `query.cancel`.
    fn action(&self) -> &'static str;

    /// What the action acts on (the envelope and audit `target`).
    fn target(&self) -> String;

    /// The audit entry's `parameters`, before the `effect` is added.
    fn parameters(&self) -> serde_json::Value;

    /// Whether a real run is audited.
    fn audited(&self) -> bool {
        true
    }

    /// The audit `effect` of a real run that changed nothing.
    fn unchanged_effect(&self) -> &'static str {
        "no_change"
    }

    /// What a real run would do now, without doing it.
    fn preview(&self, state: &WebState) -> Result<Preview, ApiError>;

    /// Do it. Blocking: [`run_manage`] calls it from `spawn_blocking`.
    fn apply(self, state: &WebState) -> Result<Applied<Self::Output>, ApiError>;
}

/// Runs `action` for `who`, whose authorization `grant` proves. A dry run
/// returns the preview and changes nothing (not audited, logged at DEBUG on
/// `arneb::audit`); a real run applies it in `spawn_blocking`, audited when
/// [`ManageAction::audited`], with `parameters` plus an `effect` of
/// `applied` or the action's unchanged effect.
pub(super) async fn run_manage<A: ManageAction>(
    state: &WebState,
    who: &Identity,
    grant: Grant,
    dry_run: bool,
    action: A,
) -> Result<ManageOutcome<A::Output>, ApiError> {
    let _ = grant; // the proof only: authorization already passed
    let name = action.action();
    let target = action.target();
    if dry_run {
        let preview = action.preview(state)?;
        tracing::debug!(
            target: "arneb::audit",
            user = %who.user,
            action = name,
            target = %target,
            summary = %preview.summary,
            "manage dry run"
        );
        return Ok(outcome(true, name, target, preview, None));
    }
    let (state, who, audit_target) = (state.clone(), who.clone(), target.clone());
    let applied = tokio::task::spawn_blocking(move || {
        if !action.audited() {
            return action.apply(&state);
        }
        let parameters = action.parameters();
        let unchanged = action.unchanged_effect();
        audited(
            &state,
            &who,
            name,
            || action.apply(&state),
            |out| AuditSubject {
                target: audit_target,
                parameters: with_effect(
                    parameters,
                    out.ok().map(|a| {
                        if a.preview.changed {
                            "applied"
                        } else {
                            unchanged
                        }
                    }),
                ),
            },
        )
    })
    .await
    .map_err(|e| {
        tracing::error!(error = %e, action = name, "manage action task failed");
        ApiError::new(ErrorCode::Internal, "the manage action failed")
    })??;
    Ok(outcome(
        false,
        name,
        target,
        applied.preview,
        Some(applied.result),
    ))
}

fn outcome<T>(
    dry_run: bool,
    action: &'static str,
    target: String,
    preview: Preview,
    result: Option<T>,
) -> ManageOutcome<T> {
    ManageOutcome {
        dry_run,
        action,
        target,
        summary: preview.summary,
        changed: preview.changed,
        warning: preview.warning,
        result,
    }
}

/// `parameters` with `"effect": effect` added (an object is kept as is; any
/// other value is wrapped as `{"value": …}`). No effect on a failed run.
fn with_effect(parameters: serde_json::Value, effect: Option<&str>) -> serde_json::Value {
    let mut map = match parameters {
        serde_json::Value::Object(map) => map,
        serde_json::Value::Null => serde_json::Map::new(),
        other => serde_json::Map::from_iter([("value".to_owned(), other)]),
    };
    if let Some(effect) = effect {
        map.insert("effect".to_owned(), effect.into());
    }
    serde_json::Value::Object(map)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn effect_is_merged_into_the_parameters() {
        assert_eq!(
            with_effect(json!({"owner": "bob"}), Some("applied")),
            json!({"owner": "bob", "effect": "applied"})
        );
        assert_eq!(with_effect(json!(null), None), json!({}));
        assert_eq!(
            with_effect(json!(3), Some("no_change")),
            json!({"value": 3, "effect": "no_change"})
        );
    }

    #[test]
    fn a_dry_run_envelope_has_no_result() {
        let preview = Preview {
            summary: "does nothing".into(),
            changed: false,
            warning: None,
        };
        let v = serde_json::to_value(outcome::<u8>(
            true,
            "query.cancel",
            "q".into(),
            preview,
            None,
        ))
        .unwrap();
        assert_eq!(
            v,
            json!({"dry_run": true, "action": "query.cancel", "target": "q",
                   "summary": "does nothing", "changed": false, "warning": null})
        );
    }
}
