//! File-based access control (file-based-access-control).
//!
//! [`AccessRules`] is a strict subset of Trino's file-based access control
//! rules file: the `catalogs`, `schemas`, `tables` and `queries` sections,
//! matched by user name only. Within a section the first rule whose
//! patterns all match decides and no matching rule denies; a section left
//! out of the file allows everything it governs. Any rule Arneb cannot
//! enforce (row filters, column masks, groups, roles, other sections) is a
//! parse error rather than being ignored, so a Trino file never grants
//! more here than it does in Trino.

use regex::Regex;
use serde::Deserialize;

/// Why a rules file was refused.
#[derive(Debug, thiserror::Error)]
pub enum RulesError {
    /// Not valid JSON, or a field has the wrong shape.
    #[error("invalid access rules: {0}")]
    Invalid(String),
    /// A rule or section Arneb does not enforce.
    #[error("unsupported access rule: {0}")]
    Unsupported(String),
}

/// Access to a catalog, from the first matching `catalogs` rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogAccess {
    /// Read and (once DDL/DML executes) write.
    All,
    /// Read only.
    ReadOnly,
    /// Hidden and unreadable.
    None,
}

/// A parsed, compiled rules file. See the module docs for the semantics.
#[derive(Debug)]
pub struct AccessRules {
    catalogs: Option<Vec<CatalogRule>>,
    schemas: Option<Vec<SchemaRule>>,
    tables: Option<Vec<TableRule>>,
    queries: Option<Vec<QueryRule>>,
}

impl AccessRules {
    /// Parse a rules file's JSON.
    pub fn from_json(text: &str) -> Result<Self, RulesError> {
        let value: serde_json::Value =
            serde_json::from_str(text).map_err(|e| RulesError::Invalid(e.to_string()))?;
        reject_unsupported(&value)?;
        let file: RulesFile =
            serde_json::from_value(value).map_err(|e| RulesError::Invalid(e.to_string()))?;
        Ok(Self {
            catalogs: compile_all(file.catalogs, CatalogRule::compile)?,
            schemas: compile_all(file.schemas, SchemaRule::compile)?,
            tables: compile_all(file.tables, TableRule::compile)?,
            queries: compile_all(file.queries, QueryRule::compile)?,
        })
    }

    /// Rules per section, in the order `catalogs`, `schemas`, `tables`,
    /// `queries`; `None` for a section the file leaves out.
    pub fn section_sizes(&self) -> [Option<usize>; 4] {
        [
            self.catalogs.as_ref().map(Vec::len),
            self.schemas.as_ref().map(Vec::len),
            self.tables.as_ref().map(Vec::len),
            self.queries.as_ref().map(Vec::len),
        ]
    }

    /// `user`'s access to `catalog`.
    pub fn catalog_access(&self, user: &str, catalog: &str) -> CatalogAccess {
        let Some(rules) = &self.catalogs else {
            return CatalogAccess::All;
        };
        rules
            .iter()
            .find(|r| r.user.matches(user) && r.catalog.matches(catalog))
            .map_or(CatalogAccess::None, |r| r.allow)
    }

    /// Whether `user` may read `catalog.schema.table`.
    pub fn can_select(&self, user: &str, catalog: &str, schema: &str, table: &str) -> bool {
        self.catalog_access(user, catalog) != CatalogAccess::None
            && self
                .table_privileges(user, catalog, schema, table)
                .is_some_and(|p| {
                    p.iter()
                        .any(|p| matches!(p, Privilege::Select | Privilege::Ownership))
                })
    }

    /// Whether `catalog` is listed for `user`.
    pub fn catalog_visible(&self, user: &str, catalog: &str) -> bool {
        self.catalog_access(user, catalog) != CatalogAccess::None
    }

    /// Whether `catalog.schema` is listed for `user`: they own it, or a
    /// table rule grants them something in it (whatever table it names).
    pub fn schema_visible(&self, user: &str, catalog: &str, schema: &str) -> bool {
        if !self.catalog_visible(user, catalog) {
            return false;
        }
        let owner = match &self.schemas {
            None => true,
            Some(rules) => rules
                .iter()
                .find(|r| {
                    r.user.matches(user) && r.catalog.matches(catalog) && r.schema.matches(schema)
                })
                .is_some_and(|r| r.owner),
        };
        owner
            || match &self.tables {
                None => true,
                Some(rules) => rules.iter().any(|r| {
                    r.user.matches(user)
                        && r.catalog.matches(catalog)
                        && r.schema.matches(schema)
                        && !r.privileges.is_empty()
                }),
            }
    }

    /// Whether `catalog.schema.table` is listed for `user`.
    pub fn table_visible(&self, user: &str, catalog: &str, schema: &str, table: &str) -> bool {
        self.catalog_visible(user, catalog)
            && self
                .table_privileges(user, catalog, schema, table)
                .is_some_and(|p| !p.is_empty())
    }

    /// Whether `user` may run queries at all.
    pub fn can_execute(&self, user: &str) -> bool {
        let Some(rules) = &self.queries else {
            return true;
        };
        rules
            .iter()
            .filter(|r| r.query_owner.is_none())
            .find(|r| r.user.matches(user))
            .is_some_and(|r| r.allow.contains(&QueryAccess::Execute))
    }

    /// Whether `viewer` may see a query run by `owner`. Their own queries
    /// are always visible.
    pub fn can_view_query(&self, viewer: &str, owner: &str) -> bool {
        if viewer == owner {
            return true;
        }
        let Some(rules) = &self.queries else {
            return true;
        };
        rules
            .iter()
            .find(|r| {
                r.user.matches(viewer) && r.query_owner.as_ref().is_none_or(|o| o.matches(owner))
            })
            .is_some_and(|r| r.allow.contains(&QueryAccess::View))
    }

    fn table_privileges(
        &self,
        user: &str,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> Option<&[Privilege]> {
        let Some(rules) = &self.tables else {
            return Some(&[Privilege::Ownership]);
        };
        rules
            .iter()
            .find(|r| {
                r.user.matches(user)
                    && r.catalog.matches(catalog)
                    && r.schema.matches(schema)
                    && r.table.matches(table)
            })
            .map(|r| r.privileges.as_slice())
    }
}

/// Trino rule fields and sections Arneb parses but cannot enforce, with
/// what they would have done.
const UNSUPPORTED_FIELDS: &[(&str, &str)] = &[
    ("group", "group matching (Arneb has no group provider)"),
    ("role", "role matching (Arneb has no roles)"),
    ("filter", "row filters"),
    ("filter_environment", "row filters"),
    ("columns", "column masks and column restrictions"),
];
const SECTIONS: &[&str] = &["catalogs", "schemas", "tables", "queries"];

fn reject_unsupported(value: &serde_json::Value) -> Result<(), RulesError> {
    let Some(top) = value.as_object() else {
        return Err(RulesError::Invalid(
            "the rules file must be a JSON object".into(),
        ));
    };
    for (section, rules) in top {
        if !SECTIONS.contains(&section.as_str()) {
            return Err(RulesError::Unsupported(format!(
                "section `{section}` (Arneb enforces only {})",
                SECTIONS.join(", ")
            )));
        }
        for (i, rule) in rules.as_array().into_iter().flatten().enumerate() {
            for (field, what) in UNSUPPORTED_FIELDS {
                if rule.get(field).is_some() {
                    return Err(RulesError::Unsupported(format!(
                        "field `{field}` in {section} rule {} ({what} are not enforced)",
                        i + 1
                    )));
                }
            }
        }
    }
    Ok(())
}

fn compile_all<S, R>(
    specs: Option<Vec<S>>,
    compile: impl Fn(S) -> Result<R, RulesError>,
) -> Result<Option<Vec<R>>, RulesError> {
    specs
        .map(|specs| specs.into_iter().map(compile).collect())
        .transpose()
}

/// A pattern matched against a whole name, as Java's `String.matches`.
#[derive(Debug)]
struct Pattern(Regex);

impl Pattern {
    fn compile(pattern: Option<String>) -> Result<Self, RulesError> {
        let pattern = pattern.as_deref().unwrap_or(".*");
        Regex::new(&format!("^(?:{pattern})$"))
            .map(Self)
            .map_err(|e| RulesError::Invalid(format!("pattern `{pattern}`: {e}")))
    }

    fn matches(&self, name: &str) -> bool {
        self.0.is_match(name)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RulesFile {
    catalogs: Option<Vec<CatalogRuleSpec>>,
    schemas: Option<Vec<SchemaRuleSpec>>,
    tables: Option<Vec<TableRuleSpec>>,
    queries: Option<Vec<QueryRuleSpec>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CatalogRuleSpec {
    user: Option<String>,
    catalog: Option<String>,
    allow: AllowSpec,
}

/// `allow` in a catalog rule: `all` / `read-only` / `none`, or Trino's
/// legacy boolean.
#[derive(Deserialize)]
#[serde(untagged)]
enum AllowSpec {
    Bool(bool),
    Level(AllowLevel),
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
enum AllowLevel {
    All,
    ReadOnly,
    None,
}

#[derive(Debug)]
struct CatalogRule {
    user: Pattern,
    catalog: Pattern,
    allow: CatalogAccess,
}

impl CatalogRule {
    fn compile(spec: CatalogRuleSpec) -> Result<Self, RulesError> {
        let allow = match spec.allow {
            AllowSpec::Bool(true) | AllowSpec::Level(AllowLevel::All) => CatalogAccess::All,
            AllowSpec::Level(AllowLevel::ReadOnly) => CatalogAccess::ReadOnly,
            AllowSpec::Bool(false) | AllowSpec::Level(AllowLevel::None) => CatalogAccess::None,
        };
        Ok(Self {
            user: Pattern::compile(spec.user)?,
            catalog: Pattern::compile(spec.catalog)?,
            allow,
        })
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SchemaRuleSpec {
    user: Option<String>,
    catalog: Option<String>,
    schema: Option<String>,
    owner: bool,
}

#[derive(Debug)]
struct SchemaRule {
    user: Pattern,
    catalog: Pattern,
    schema: Pattern,
    owner: bool,
}

impl SchemaRule {
    fn compile(spec: SchemaRuleSpec) -> Result<Self, RulesError> {
        Ok(Self {
            user: Pattern::compile(spec.user)?,
            catalog: Pattern::compile(spec.catalog)?,
            schema: Pattern::compile(spec.schema)?,
            owner: spec.owner,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum Privilege {
    Select,
    Insert,
    Delete,
    Update,
    Ownership,
    GrantSelect,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TableRuleSpec {
    user: Option<String>,
    catalog: Option<String>,
    schema: Option<String>,
    table: Option<String>,
    privileges: Vec<Privilege>,
}

#[derive(Debug)]
struct TableRule {
    user: Pattern,
    catalog: Pattern,
    schema: Pattern,
    table: Pattern,
    privileges: Vec<Privilege>,
}

impl TableRule {
    fn compile(spec: TableRuleSpec) -> Result<Self, RulesError> {
        Ok(Self {
            user: Pattern::compile(spec.user)?,
            catalog: Pattern::compile(spec.catalog)?,
            schema: Pattern::compile(spec.schema)?,
            table: Pattern::compile(spec.table)?,
            privileges: spec.privileges,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
enum QueryAccess {
    Execute,
    View,
    Kill,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct QueryRuleSpec {
    user: Option<String>,
    #[serde(rename = "queryOwner")]
    query_owner: Option<String>,
    allow: Vec<QueryAccess>,
}

#[derive(Debug)]
struct QueryRule {
    user: Pattern,
    query_owner: Option<Pattern>,
    allow: Vec<QueryAccess>,
}

impl QueryRule {
    fn compile(spec: QueryRuleSpec) -> Result<Self, RulesError> {
        if spec.query_owner.is_some() && spec.allow.contains(&QueryAccess::Execute) {
            return Err(RulesError::Invalid(
                "a queries rule with `queryOwner` cannot allow `execute`".into(),
            ));
        }
        Ok(Self {
            user: Pattern::compile(spec.user)?,
            query_owner: spec
                .query_owner
                .map(|o| Pattern::compile(Some(o)))
                .transpose()?,
            allow: spec.allow,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rules(json: &str) -> AccessRules {
        AccessRules::from_json(json).unwrap()
    }

    #[test]
    fn an_empty_file_allows_everything() {
        let r = rules("{}");
        assert!(r.can_select("anyone", "hive", "tpch", "lineitem"));
        assert!(r.schema_visible("anyone", "hive", "tpch"));
        assert!(r.can_execute("anyone"));
        assert!(r.can_view_query("a", "b"));
        assert_eq!(r.section_sizes(), [None; 4]);
    }

    #[test]
    fn first_matching_table_rule_wins_and_no_match_denies() {
        let r = rules(
            r#"{"tables": [
                {"user": "alice", "table": "secret", "privileges": []},
                {"user": "alice|bob", "privileges": ["SELECT"]}
            ]}"#,
        );
        assert!(!r.can_select("alice", "hive", "s", "secret"));
        assert!(!r.table_visible("alice", "hive", "s", "secret"));
        assert!(r.can_select("alice", "hive", "s", "orders"));
        assert!(r.can_select("bob", "hive", "s", "secret"));
        assert!(
            !r.can_select("carol", "hive", "s", "orders"),
            "no rule matches carol"
        );
    }

    #[test]
    fn patterns_match_whole_names() {
        let r = rules(r#"{"tables": [{"table": "line", "privileges": ["SELECT"]}]}"#);
        assert!(r.can_select("u", "c", "s", "line"));
        assert!(!r.can_select("u", "c", "s", "lineitem"));
    }

    #[test]
    fn catalog_access_levels_and_the_legacy_boolean() {
        let r = rules(
            r#"{"catalogs": [
                {"catalog": "pg", "allow": "none"},
                {"user": "admin", "catalog": "hive", "allow": "all"},
                {"catalog": "hive", "allow": "read-only"},
                {"catalog": "legacy", "allow": false},
                {"allow": true}
            ]}"#,
        );
        assert_eq!(r.catalog_access("admin", "hive"), CatalogAccess::All);
        assert_eq!(r.catalog_access("alice", "hive"), CatalogAccess::ReadOnly);
        assert!(
            r.can_select("alice", "hive", "s", "t"),
            "read-only can read"
        );
        assert!(!r.can_select("alice", "pg", "public", "accounts"));
        assert!(!r.catalog_visible("alice", "pg"));
        assert!(!r.catalog_visible("alice", "legacy"));
        assert!(r.catalog_visible("alice", "other"));
    }

    #[test]
    fn a_schema_is_visible_to_its_owner_or_with_any_table_privilege_in_it() {
        let r = rules(
            r#"{
                "schemas": [{"user": "alice", "schema": "mine", "owner": true},
                            {"owner": false}],
                "tables": [{"user": "alice", "schema": "sales", "table": "orders",
                            "privileges": ["SELECT"]}]
            }"#,
        );
        assert!(r.schema_visible("alice", "hive", "mine"));
        assert!(r.schema_visible("alice", "hive", "sales"));
        assert!(!r.schema_visible("alice", "hive", "hr"));
        assert!(!r.table_visible("alice", "hive", "sales", "customers"));
    }

    #[test]
    fn queries_rules_govern_execute_and_view() {
        let r = rules(
            r#"{"queries": [
                {"user": "admin", "allow": ["execute", "view", "kill"]},
                {"user": "alice", "queryOwner": "bob", "allow": ["view"]},
                {"user": "alice|bob", "allow": ["execute"]}
            ]}"#,
        );
        assert!(r.can_execute("admin"));
        assert!(
            r.can_execute("alice"),
            "queryOwner rules do not decide execute"
        );
        assert!(!r.can_execute("carol"));
        assert!(r.can_view_query("admin", "bob"));
        assert!(r.can_view_query("alice", "bob"));
        assert!(!r.can_view_query("bob", "alice"));
        assert!(
            r.can_view_query("bob", "bob"),
            "own queries are always visible"
        );
    }

    #[test]
    fn rules_arneb_cannot_enforce_are_refused() {
        for (json, named) in [
            (
                r#"{"tables": [{"filter": "x > 1", "privileges": []}]}"#,
                "`filter`",
            ),
            (
                r#"{"tables": [{"columns": [], "privileges": []}]}"#,
                "`columns`",
            ),
            (
                r#"{"catalogs": [{"group": "eng", "allow": "all"}]}"#,
                "`group`",
            ),
            (r#"{"impersonation": []}"#, "`impersonation`"),
        ] {
            let err = AccessRules::from_json(json).unwrap_err().to_string();
            assert!(err.contains(named), "{err}");
        }
        let err = AccessRules::from_json(r#"{"tables": [{"tabel": "x", "privileges": []}]}"#)
            .unwrap_err()
            .to_string();
        assert!(err.contains("tabel"), "{err}");
        assert!(
            AccessRules::from_json(r#"{"tables": [{"table": "(", "privileges": []}]}"#).is_err()
        );
        assert!(AccessRules::from_json(
            r#"{"queries": [{"queryOwner": "x", "allow": ["execute"]}]}"#
        )
        .is_err());
    }
}
