//! Privilege layers for the Web API and MCP: the observe and manage layers
//! on top of the query rules (manage-api D1, D2, D12).
//!
//! - **Observe** (cluster, workers, admission, audit views) is open under
//!   `[auth] type = "none"` and, under password auth, open unless a
//!   `system_information` rules section exists and does not allow the user
//!   `read`. A manager can always observe.
//! - **Manage** (changing system state) needs password auth and either the
//!   user in `[access_control] admin_users` or a `system_information` rule
//!   allowing `write`. Nothing grants it by default.

use std::sync::Arc;

use arneb_catalog::AccessRules;

use super::auth::Identity;
use super::error::{ApiError, ErrorCode};

/// A privilege layer above the query rules.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Layer {
    /// Read cluster, worker, admission and audit state.
    Observe,
    /// Change system state.
    Manage,
}

/// Proof that [`Authorizer::check`] (or [`Authorizer::check_kill`]) passed.
/// Only this module can build one, so a manage function that takes a
/// `Grant` cannot be called without an authorization decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Grant(());

/// Why a kill was allowed (manage-api D2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KillGrant {
    /// The caller owns the query.
    Owner,
    /// A `queries` rule allows it, or there are no `queries` rules.
    Rule,
    /// The caller has the manage privilege.
    Manage,
}

impl KillGrant {
    /// The proof value for this grant.
    pub fn grant(self) -> Grant {
        Grant(())
    }

    /// Whether the caller killed their own query.
    pub fn is_owner(&self) -> bool {
        matches!(self, Self::Owner)
    }
}

/// Decides the observe and manage layers and the kill rule.
#[derive(Clone)]
pub struct Authorizer {
    rules: Option<Arc<AccessRules>>,
    admin_users: Vec<String>,
    password_auth: bool,
}

impl Authorizer {
    /// An authorizer over the loaded `rules`, the `[access_control]
    /// admin_users` and whether `[auth] type = "password"` is on.
    pub fn new(
        rules: Option<Arc<AccessRules>>,
        admin_users: Vec<String>,
        password_auth: bool,
    ) -> Self {
        Self {
            rules,
            admin_users,
            password_auth,
        }
    }

    fn manages(&self, who: &Identity) -> bool {
        self.password_auth
            && (self.admin_users.contains(&who.user)
                || self
                    .rules
                    .as_ref()
                    .is_some_and(|r| r.system_information_write(&who.user)))
    }

    fn observes(&self, who: &Identity) -> bool {
        !self.password_auth
            || self.manages(who)
            || self
                .rules
                .as_ref()
                .is_none_or(|r| r.system_information_read(&who.user))
    }

    /// Whether `who` holds `layer`.
    pub fn allows(&self, who: &Identity, layer: Layer) -> bool {
        match layer {
            Layer::Observe => self.observes(who),
            Layer::Manage => self.manages(who),
        }
    }

    /// `Ok(Grant)` when `who` holds `layer`; otherwise `forbidden` with a
    /// hint naming what would grant it, or `manage_requires_auth` for a
    /// manage call under `[auth] type = "none"`.
    pub fn check(&self, who: &Identity, layer: Layer) -> Result<Grant, ApiError> {
        if self.allows(who, layer) {
            return Ok(Grant(()));
        }
        let user = &who.user;
        Err(match layer {
            Layer::Manage if !self.password_auth => manage_requires_auth(),
            Layer::Manage => {
                ApiError::new(ErrorCode::Forbidden, "you do not have the manage privilege")
                    .with_hint(format!(
                        "needs a system_information rule that allows write for user {user}, \
                 or {user} in [access_control] admin_users"
                    ))
            }
            Layer::Observe => ApiError::new(
                ErrorCode::Forbidden,
                "you do not have the observe privilege",
            )
            .with_hint(format!(
                "needs a system_information rule that allows read for user {user}"
            )),
        })
    }

    /// The kill rule for a query of `owner` (`None`: a query with no user,
    /// owner `""`), in this order: the owner may always kill (any auth
    /// mode); otherwise `none` auth refuses; otherwise a `queries` rule
    /// allowing `kill`, or no `queries` rules; otherwise manage; otherwise
    /// `forbidden`.
    pub fn check_kill(&self, who: &Identity, owner: Option<&str>) -> Result<KillGrant, ApiError> {
        let owner = owner.unwrap_or_default();
        if who.user == owner {
            return Ok(KillGrant::Owner);
        }
        if !self.password_auth {
            return Err(manage_requires_auth());
        }
        if self
            .rules
            .as_ref()
            .is_none_or(|r| r.can_kill_query(&who.user, owner))
        {
            return Ok(KillGrant::Rule);
        }
        if self.manages(who) {
            return Ok(KillGrant::Manage);
        }
        Err(ApiError::new(
            ErrorCode::Forbidden,
            "the access rules do not allow you to kill this query",
        )
        .with_hint(format!(
            "cancelling a query of {owner} needs a queries rule that allows kill for \
             queryOwner {owner}, or the manage privilege"
        )))
    }
}

fn manage_requires_auth() -> ApiError {
    ApiError::new(
        ErrorCode::ManageRequiresAuth,
        "manage actions need an authenticated user; this server runs with [auth] type = \"none\"",
    )
    .with_hint("set [auth] type = \"password\" and add your user to [access_control] admin_users")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::web::auth::AuthVia;

    fn who(user: &str, password: bool) -> Identity {
        Identity {
            user: user.into(),
            via: if password {
                AuthVia::Basic
            } else {
                AuthVia::Unauthenticated
            },
        }
    }

    fn rules(json: &str) -> Option<Arc<AccessRules>> {
        Some(Arc::new(AccessRules::from_json(json).unwrap()))
    }

    fn code(r: Result<impl std::fmt::Debug, ApiError>) -> (ErrorCode, String) {
        let e = r.unwrap_err();
        (e.code, e.hint.unwrap_or_default())
    }

    #[test]
    fn observe_defaults_open_and_follows_the_section() {
        let alice = who("alice", true);
        assert!(Authorizer::new(None, vec![], true)
            .check(&alice, Layer::Observe)
            .is_ok());
        let sect = rules(r#"{"system_information":[{"user":"ops","allow":["read"]}]}"#);
        let a = Authorizer::new(sect.clone(), vec![], true);
        let (c, hint) = code(a.check(&alice, Layer::Observe));
        assert_eq!(c, ErrorCode::Forbidden);
        assert_eq!(
            hint,
            "needs a system_information rule that allows read for user alice"
        );
        assert!(a.check(&who("ops", true), Layer::Observe).is_ok());
        let a = Authorizer::new(sect.clone(), vec!["alice".into()], true);
        assert!(a.check(&alice, Layer::Observe).is_ok(), "admin observes");
        let a = Authorizer::new(sect, vec![], false);
        assert!(a.check(&who("alice", false), Layer::Observe).is_ok());
        // A write-only rule in an existing section: manage implies observe.
        let w = rules(
            r#"{"system_information":[{"user":"ops","allow":["write"]},
                {"user":"viewer","allow":["read"]}]}"#,
        );
        let a = Authorizer::new(w, vec![], true);
        assert!(a.check(&who("ops", true), Layer::Observe).is_ok());
        assert!(a.check(&alice, Layer::Observe).is_err());
    }

    #[test]
    fn manage_is_denied_unless_granted() {
        let alice = who("alice", true);
        let (c, hint) = code(Authorizer::new(None, vec![], true).check(&alice, Layer::Manage));
        assert_eq!(c, ErrorCode::Forbidden);
        assert_eq!(
            hint,
            "needs a system_information rule that allows write for user alice, \
             or alice in [access_control] admin_users"
        );
        let a = Authorizer::new(None, vec!["root".into()], true);
        assert!(a.check(&who("root", true), Layer::Manage).is_ok());
        assert!(a.check(&alice, Layer::Manage).is_err());
        let a = Authorizer::new(
            rules(r#"{"system_information":[{"user":"ops","allow":["write"]}]}"#),
            vec![],
            true,
        );
        assert!(a.check(&who("ops", true), Layer::Manage).is_ok());
        // Under none, even an admin_users name is refused.
        let a = Authorizer::new(None, vec!["root".into()], false);
        let e = a.check(&who("root", false), Layer::Manage).unwrap_err();
        assert_eq!(e.code, ErrorCode::ManageRequiresAuth);
        assert_eq!(
            e.message,
            "manage actions need an authenticated user; this server runs with [auth] type = \"none\""
        );
        assert_eq!(
            e.hint.as_deref(),
            Some(
                "set [auth] type = \"password\" and add your user to [access_control] admin_users"
            )
        );
    }

    #[test]
    fn kill_rule_order() {
        // 1. The owner, in any auth mode.
        let none = Authorizer::new(None, vec![], false);
        let k = none.check_kill(&who("bob", false), Some("bob")).unwrap();
        assert!(k.is_owner());
        // A query with no user is owned by "".
        assert_eq!(
            none.check_kill(&who("", false), None).unwrap(),
            KillGrant::Owner
        );
        // 2. Not the owner under none.
        let e = none
            .check_kill(&who("alice", false), Some("bob"))
            .unwrap_err();
        assert_eq!(e.code, ErrorCode::ManageRequiresAuth);
        // 3. No rules, or no queries section: a rule-less allow.
        let open = Authorizer::new(None, vec![], true);
        assert_eq!(
            open.check_kill(&who("alice", true), Some("bob")).unwrap(),
            KillGrant::Rule
        );
        // 3b. A rules file without a queries section behaves the same.
        let sys_only = Authorizer::new(
            rules(r#"{"system_information":[{"user":"ops","allow":["read"]}]}"#),
            vec![],
            true,
        );
        assert_eq!(
            sys_only
                .check_kill(&who("alice", true), Some("bob"))
                .unwrap(),
            KillGrant::Rule
        );
        let r = rules(
            r#"{"queries":[{"user":"ops","queryOwner":"bob","allow":["kill"]},
                {"allow":["execute"]}]}"#,
        );
        let a = Authorizer::new(r, vec!["root".into()], true);
        assert_eq!(
            a.check_kill(&who("ops", true), Some("bob")).unwrap(),
            KillGrant::Rule
        );
        // 5. A rules file that does not allow it.
        let (c, hint) = code(a.check_kill(&who("ops", true), Some("carol")));
        assert_eq!(c, ErrorCode::Forbidden);
        assert_eq!(
            hint,
            "cancelling a query of carol needs a queries rule that allows kill for \
             queryOwner carol, or the manage privilege"
        );
        // 4. Manage.
        assert_eq!(
            a.check_kill(&who("root", true), Some("carol")).unwrap(),
            KillGrant::Manage
        );
    }
}
