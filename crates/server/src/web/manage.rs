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

    /// Whether the change is kept in the state store. A change of a
    /// persisted action warns (D5) while the store is in memory.
    fn persisted(&self) -> bool {
        false
    }

    /// The audit `effect` of a real run that changed nothing.
    fn unchanged_effect(&self) -> &'static str {
        "no_change"
    }

    /// What a real run would do now, without doing it.
    fn preview(&self, state: &WebState) -> Result<Preview, ApiError>;

    /// Do it. Blocking: [`run_manage`] calls it from `spawn_blocking`.
    fn apply(self, state: &WebState) -> Result<Applied<Self::Output>, ApiError>;

    /// A share of the lock that serializes this kind of change, if any.
    /// [`run_manage`] keeps it until the audit entry is appended, so a
    /// request dropped mid-call still holds the lock across apply and
    /// append, and the audit order stays the apply order.
    fn change_lock(&self) -> Option<ChangeLock> {
        None
    }
}

/// A share of a change lock's guard (`WebState::admission_changes`,
/// `WebState::cordon_changes`).
pub(super) type ChangeLock = std::sync::Arc<tokio::sync::OwnedMutexGuard<()>>;

/// The dry-run `warning` of an audited action while the audit log is not
/// writable.
pub(super) const AUDIT_UNAVAILABLE_WARNING: &str =
    "the audit log is unavailable; the real call would be refused with audit_unavailable";

/// The `warning` of a change to a [`ManageAction::persisted`] action while
/// the state store is in memory.
pub(super) const IN_MEMORY_WARNING: &str =
    "the state store is in memory ([state] path is not set), so this change is lost on restart";

/// Appends `note` to `preview`'s warning.
fn add_warning(preview: &mut Preview, note: &str) {
    preview.warning = Some(match preview.warning.take() {
        Some(w) => format!("{w}; {note}"),
        None => note.to_owned(),
    });
}

/// `preview` plus the in-memory warning when it changes a persisted action
/// and the state store is not on disk.
fn warn_if_in_memory(state: &WebState, persisted: bool, preview: &mut Preview) {
    let on_disk = state.state.as_ref().is_some_and(|s| s.is_persistent());
    if persisted && preview.changed && !on_disk {
        add_warning(preview, IN_MEMORY_WARNING);
    }
}

/// Runs `action` for `who`, whose authorization `grant` proves. A dry run
/// returns the preview and changes nothing (not audited, logged at DEBUG on
/// `arneb::audit`; an audited action's preview warns when the audit log is
/// not writable); a real run applies it in `spawn_blocking`, audited when
/// [`ManageAction::audited`], with `parameters` plus an `effect` of
/// `applied` or the action's unchanged effect. Either warns when it changes
/// a [`ManageAction::persisted`] action and the state store is in memory.
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
    let persisted = action.persisted();
    if dry_run {
        let mut preview = action.preview(state)?;
        warn_if_in_memory(state, persisted, &mut preview);
        if action.audited() && !state.audit.is_writable() {
            add_warning(&mut preview, AUDIT_UNAVAILABLE_WARNING);
        }
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
    let (blocking_state, who, audit_target) = (state.clone(), who.clone(), target.clone());
    let held = action.change_lock();
    let mut applied = tokio::task::spawn_blocking(move || {
        let state = blocking_state;
        if !action.audited() {
            let applied = action.apply(&state);
            drop(held);
            return applied;
        }
        let parameters = action.parameters();
        let unchanged = action.unchanged_effect();
        #[cfg(test)]
        let lock_key = held.as_deref().map(test_hooks::key_of);
        let applied = audited(
            &state,
            &who,
            name,
            || action.apply(&state),
            |out| {
                #[cfg(test)]
                test_hooks::after_apply(lock_key);
                AuditSubject {
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
                }
            },
        );
        // Released only once the entry is appended.
        drop(held);
        applied
    })
    .await
    .map_err(|e| {
        tracing::error!(error = %e, action = name, "manage action task failed");
        ApiError::new(ErrorCode::Internal, "the manage action failed")
    })??;
    warn_if_in_memory(state, persisted, &mut applied.preview);
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

/// A test-only stop between a change's apply and its audit append.
#[cfg(test)]
pub(super) mod test_hooks {
    use std::sync::mpsc::{Receiver, Sender};
    use std::sync::Mutex;

    /// `(lock key, inside, release)`: the change whose lock has this key
    /// sends on `inside` once applied, then waits on `release`. One per
    /// test state, so tests running in parallel do not share a gate.
    type Gate = (usize, Sender<()>, Receiver<()>);
    static AFTER_APPLY: Mutex<Vec<Gate>> = Mutex::new(Vec::new());

    /// The key of a change lock: its mutex's address.
    pub(in crate::web) fn key(lock: &std::sync::Arc<tokio::sync::Mutex<()>>) -> usize {
        std::sync::Arc::as_ptr(lock) as usize
    }

    pub(super) fn key_of(guard: &tokio::sync::OwnedMutexGuard<()>) -> usize {
        key(tokio::sync::OwnedMutexGuard::mutex(guard))
    }

    /// Stop the next change applied under `lock` after its apply; returns
    /// `(inside, release)`.
    pub(in crate::web) fn stop_after_apply(
        lock: &std::sync::Arc<tokio::sync::Mutex<()>>,
    ) -> (Receiver<()>, Sender<()>) {
        let (inside_tx, inside_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        AFTER_APPLY
            .lock()
            .unwrap()
            .push((key(lock), inside_tx, release_rx));
        (inside_rx, release_tx)
    }

    pub(super) fn after_apply(lock_key: Option<usize>) {
        let Some(lock_key) = lock_key else { return };
        let gate = {
            let mut gates = AFTER_APPLY.lock().unwrap();
            let at = gates.iter().position(|(k, _, _)| *k == lock_key);
            at.map(|at| gates.swap_remove(at))
        };
        if let Some((_, inside, release)) = gate {
            inside.send(()).unwrap();
            release.recv().unwrap();
        }
    }
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
