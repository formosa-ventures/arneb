use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, Result};
use arneb_common::types::DataType;
use arneb_common::ServerConfig;
use arneb_connectors::{CloudStorageConfig, S3StorageConfig};
use arneb_protocol::{AuthMethod, PgwireTlsMode, ScramVerifier, UserCredentials};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct AppConfig {
    #[serde(flatten)]
    pub server: ServerConfig,

    #[serde(default)]
    pub tables: Vec<TableConfig>,

    #[serde(default)]
    pub cluster: ClusterConfig,

    /// Global storage backend configuration.
    #[serde(default)]
    pub storage: StorageConfig,

    /// External catalog configurations (e.g., Hive Metastore).
    #[serde(default)]
    pub catalogs: Vec<CatalogConfig>,

    /// Memory budget configuration for spillable operators.
    #[serde(default)]
    pub memory: MemoryConfig,

    /// Client authentication for the PostgreSQL wire protocol.
    #[serde(default)]
    pub auth: AuthConfig,

    /// Trino client REST protocol listener (coordinator/standalone only).
    #[serde(default)]
    pub trino: TrinoProtocolConfig,

    /// TLS for the client-facing listeners (pgwire, Trino). Raw file values;
    /// the effective settings (env applied, validated) are in
    /// [`AppConfig::tls_settings`].
    #[serde(default)]
    pub tls: Option<TlsConfig>,

    /// Effective TLS settings after env overrides and validation; `None`
    /// means TLS is off.
    #[serde(skip)]
    pub tls_settings: Option<TlsSettings>,

    /// Web UI listener (coordinator/standalone only).
    #[serde(default)]
    pub web: WebConfig,

    /// Admission control (coordinator/standalone only); effective values
    /// after [`AppConfig::load`].
    #[serde(default)]
    pub admission: AdmissionConfig,

    /// Agent query API limits (coordinator/standalone only); effective
    /// values after [`AppConfig::load`].
    #[serde(default)]
    pub agent_api: AgentApiConfig,

    /// MCP endpoint (`/mcp` on the Web port; coordinator/standalone only);
    /// effective values after [`AppConfig::load`].
    #[serde(default)]
    pub mcp: McpConfig,

    /// Query metrics and the slow-query log (coordinator/standalone only);
    /// effective values after [`AppConfig::load`].
    #[serde(default)]
    pub observability: ObservabilityConfig,

    /// Access rules (coordinator/standalone only); effective values after
    /// [`AppConfig::load`].
    #[serde(default)]
    pub access_control: AccessControlConfig,

    /// Coordinator state store (users, API tokens); effective values after
    /// [`AppConfig::load`].
    #[serde(default)]
    pub state: StateConfig,
}

/// `[state]` section: where the coordinator keeps its mutable state (users,
/// API tokens).
///
/// ```toml
/// [state]
/// path = "/var/lib/arneb/state.json"   # ARNEB_STATE_PATH
/// ```
///
/// Unset: the state lives in memory only and API tokens are lost on restart.
/// Config-seeded users follow `[[auth.users]]` on every start: a changed
/// `password_hash` or a removed user revokes that user's tokens.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct StateConfig {
    /// Where the state file lives; `None` keeps the state in memory.
    pub path: Option<PathBuf>,
}

impl StateConfig {
    /// Apply `ARNEB_STATE_PATH` (env > file); an empty value is an error.
    pub fn apply_env(&mut self, env: impl Fn(&str) -> Option<String>) -> Result<()> {
        if let Some(v) = env("ARNEB_STATE_PATH") {
            let v = v.trim();
            anyhow::ensure!(!v.is_empty(), "ARNEB_STATE_PATH is set but empty");
            self.path = Some(v.into());
        }
        Ok(())
    }
}

/// `[access_control]` section (file-based-access-control).
///
/// ```toml
/// [access_control]
/// rules_file = "/etc/arneb/rules.json"   # ARNEB_ACCESS_CONTROL_RULES_FILE
/// admin_users = ["root"]                 # ARNEB_ADMIN_USERS (comma list)
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AccessControlConfig {
    /// A rules file in Trino's file-based access control format. Unset:
    /// every user may read every table.
    pub rules_file: Option<std::path::PathBuf>,
    /// Users who hold the manage privilege (password auth only).
    pub admin_users: Vec<String>,
}

impl AccessControlConfig {
    /// Apply `ARNEB_ACCESS_CONTROL_RULES_FILE` (env > file); an empty value
    /// is an error. `ARNEB_ADMIN_USERS` is a comma list, trimmed with blanks
    /// dropped; a value with no names left counts as unset.
    pub fn apply_env(&mut self, env: impl Fn(&str) -> Option<String>) -> Result<()> {
        if let Some(v) = env("ARNEB_ACCESS_CONTROL_RULES_FILE") {
            let v = v.trim();
            anyhow::ensure!(
                !v.is_empty(),
                "ARNEB_ACCESS_CONTROL_RULES_FILE is set but empty"
            );
            self.rules_file = Some(v.into());
        }
        // An empty value counts as unset; a list is trimmed, blanks dropped.
        if let Some(v) = env("ARNEB_ADMIN_USERS") {
            let users: Vec<String> = v
                .split(',')
                .map(str::trim)
                .filter(|u| !u.is_empty())
                .map(str::to_owned)
                .collect();
            if !users.is_empty() {
                self.admin_users = users;
            }
        }
        Ok(())
    }

    /// Read and parse the rules file, if one is set.
    pub fn load_rules(&self) -> Result<Option<arneb_catalog::AccessRules>> {
        let Some(path) = &self.rules_file else {
            return Ok(None);
        };
        let text = std::fs::read_to_string(path).map_err(|e| {
            anyhow::anyhow!("cannot read access rules file {}: {e}", path.display())
        })?;
        arneb_catalog::AccessRules::from_json(&text)
            .map(Some)
            .map_err(|e| anyhow::anyhow!("access rules file {}: {e}", path.display()))
    }
}

/// `[web]` section.
///
/// ```toml
/// [web]
/// bind_address = "0.0.0.0"   # ARNEB_WEB_BIND_ADDRESS
/// metrics_auth = false       # ARNEB_WEB_METRICS_AUTH
/// ```
///
/// Under `[auth] type = "password"` the Web UI and `/api/v1` require sign-in,
/// but the Web port serves plain HTTP and shows query text, so it binds to
/// `127.0.0.1` unless this is set.
/// The port stays at the pgwire port + 1000.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WebConfig {
    #[serde(default)]
    pub bind_address: Option<String>,
    /// Require credentials for `/metrics` under password auth. Default:
    /// `false` (unauthenticated, so scrapers work unchanged).
    #[serde(default)]
    pub metrics_auth: bool,
}

impl WebConfig {
    /// Apply `ARNEB_WEB_BIND_ADDRESS` and `ARNEB_WEB_METRICS_AUTH` (env > file).
    pub fn apply_env_overrides(&mut self, env: impl Fn(&str) -> Option<String>) -> Result<()> {
        if let Some(v) = env("ARNEB_WEB_BIND_ADDRESS") {
            self.bind_address = Some(v);
        }
        if let Some(v) = env("ARNEB_WEB_METRICS_AUTH") {
            self.metrics_auth = parse_env_bool("ARNEB_WEB_METRICS_AUTH", &v)?;
        }
        Ok(())
    }
}

/// Why the Web UI binds where it does (logged on `arneb::config`).
pub const WEB_BIND_DEFAULT: &str = "default";
pub const WEB_BIND_LOOPBACK: &str = "loopback (auth=password)";
pub const WEB_BIND_EXPLICIT: &str = "explicit";

/// Resolve the Web UI bind host: an explicit `[web] bind_address` wins;
/// otherwise `127.0.0.1` under password auth and the server's
/// `bind_address` without it. Returns `(host, reason)`.
pub fn resolve_web_bind(
    auth: AuthType,
    server_bind: &str,
    web_bind: Option<&str>,
) -> Result<(String, &'static str)> {
    let (host, reason) = match (web_bind.map(str::trim), auth) {
        (Some(explicit), _) => (explicit.to_owned(), WEB_BIND_EXPLICIT),
        (None, AuthType::Password) => ("127.0.0.1".to_owned(), WEB_BIND_LOOPBACK),
        (None, AuthType::None) => (server_bind.to_owned(), WEB_BIND_DEFAULT),
    };
    if host != "localhost" && host.parse::<std::net::IpAddr>().is_err() {
        bail!("invalid [web] bind_address '{host}' (expected an IP address)");
    }
    Ok((host, reason))
}

/// Whether a bind host only listens on loopback.
pub fn is_loopback_host(host: &str) -> bool {
    host == "localhost"
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
}

/// `[tls]` section: one certificate for both client-facing listeners.
///
/// ```toml
/// [tls]
/// cert_path = "/etc/arneb/server.crt"   # PEM chain, leaf first  (ARNEB_TLS_CERT_PATH)
/// key_path  = "/etc/arneb/server.key"   # PEM private key        (ARNEB_TLS_KEY_PATH)
/// pgwire    = "required"                # "required" (default) | "optional" (ARNEB_TLS_PGWIRE)
/// ```
///
/// Setting both `ARNEB_TLS_*_PATH` variables enables TLS without the section.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    #[serde(default)]
    pub cert_path: Option<PathBuf>,
    #[serde(default)]
    pub key_path: Option<PathBuf>,
    #[serde(default)]
    pub pgwire: Option<String>,
}

/// Validated `[tls]` settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsSettings {
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
    pub pgwire: PgwireTlsMode,
}

impl TlsConfig {
    /// Apply `ARNEB_TLS_*` overrides (looked up through `env`) on top of the
    /// file section and validate. `Ok(None)` means TLS is off.
    pub fn resolve(
        file: Option<&TlsConfig>,
        env: impl Fn(&str) -> Option<String>,
    ) -> Result<Option<TlsSettings>> {
        let mut c = file.cloned().unwrap_or_default();
        let section = file.is_some();
        if let Some(v) = env("ARNEB_TLS_CERT_PATH") {
            c.cert_path = Some(PathBuf::from(v));
        }
        if let Some(v) = env("ARNEB_TLS_KEY_PATH") {
            c.key_path = Some(PathBuf::from(v));
        }
        if let Some(v) = env("ARNEB_TLS_PGWIRE") {
            c.pgwire = Some(v);
        }
        let pgwire = match c.pgwire.as_deref() {
            None => PgwireTlsMode::default(),
            Some(m) => m
                .parse()
                .map_err(|e| anyhow::anyhow!("[tls] pgwire: {e}"))?,
        };
        match (c.cert_path, c.key_path) {
            (Some(cert_path), Some(key_path)) => Ok(Some(TlsSettings {
                cert_path,
                key_path,
                pgwire,
            })),
            (None, None) if !section && c.pgwire.is_none() => Ok(None),
            (Some(_), None) => {
                bail!("[tls] key_path is required when cert_path is set (or ARNEB_TLS_KEY_PATH)")
            }
            (None, Some(_)) => {
                bail!("[tls] cert_path is required when key_path is set (or ARNEB_TLS_CERT_PATH)")
            }
            (None, None) => bail!(
                "[tls] cert_path and key_path are required (or ARNEB_TLS_CERT_PATH / ARNEB_TLS_KEY_PATH)"
            ),
        }
    }
}

/// `[admission]` section: how many queries run at once, how many may wait,
/// and the server-wide statement timeout. After [`AppConfig::load`] the
/// fields hold the effective values (env applied, validated).
///
/// ```toml
/// [admission]
/// max_running_queries = 8     # ARNEB_MAX_RUNNING_QUERIES; 0 = unlimited
/// max_queued_queries  = 256   # ARNEB_MAX_QUEUED_QUERIES
/// statement_timeout   = "0"   # ARNEB_STATEMENT_TIMEOUT; e.g. "30s", "5min", 30000 (ms)
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AdmissionConfig {
    /// `0` = unlimited (admission-control decisions log #1: default 8).
    pub max_running_queries: usize,
    /// Decisions log #2: default 256.
    pub max_queued_queries: usize,
    /// Server default for sessions that set no timeout; `None` = off.
    /// Resolved by [`AdmissionConfig::apply_env`] from
    /// `ARNEB_STATEMENT_TIMEOUT` or, failing that, the file value.
    #[serde(skip)]
    pub statement_timeout: Option<std::time::Duration>,
    /// The file's `statement_timeout`, unparsed: it is validated only once
    /// the env override is known, so a valid `ARNEB_STATEMENT_TIMEOUT`
    /// still wins over a bad file value (env > file).
    #[serde(
        rename = "statement_timeout",
        deserialize_with = "deserialize_time_limit_text"
    )]
    statement_timeout_file: Option<String>,
}

impl Default for AdmissionConfig {
    fn default() -> Self {
        Self {
            max_running_queries: 8,
            max_queued_queries: 256,
            statement_timeout: None,
            statement_timeout_file: None,
        }
    }
}

/// A time limit written as milliseconds (`30000`) or a string (`"30s"`),
/// kept as text; [`AdmissionConfig::apply_env`] parses it.
fn deserialize_time_limit_text<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> std::result::Result<Option<String>, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Value {
        Millis(u64),
        Text(String),
    }
    let text = match Value::deserialize(d)? {
        Value::Millis(ms) => ms.to_string(),
        Value::Text(t) => t,
    };
    Ok(Some(text))
}

impl AdmissionConfig {
    /// Apply `ARNEB_MAX_RUNNING_QUERIES`, `ARNEB_MAX_QUEUED_QUERIES` and
    /// `ARNEB_STATEMENT_TIMEOUT` (looked up through `env`) over the file
    /// values (env > file > defaults), then validate the effective
    /// statement timeout, naming its source in the error.
    pub fn apply_env(&mut self, env: impl Fn(&str) -> Option<String>) -> Result<()> {
        for (var, field) in [
            ("ARNEB_MAX_RUNNING_QUERIES", &mut self.max_running_queries),
            ("ARNEB_MAX_QUEUED_QUERIES", &mut self.max_queued_queries),
        ] {
            if let Some(v) = env(var) {
                *field = v.trim().parse().map_err(|_| {
                    anyhow::anyhow!("invalid {var} value '{v}' (expected a non-negative integer)")
                })?;
            }
        }
        let source = match env("ARNEB_STATEMENT_TIMEOUT") {
            Some(v) => Some(("ARNEB_STATEMENT_TIMEOUT", v)),
            None => self
                .statement_timeout_file
                .clone()
                .map(|v| ("[admission] statement_timeout", v)),
        };
        if let Some((name, v)) = source {
            self.statement_timeout = arneb_common::duration::parse_time_limit(&v)
                .map_err(|e| anyhow::anyhow!("invalid {name} value: {e}"))?;
        }
        Ok(())
    }
}

/// `[agent_api]` section: limits of the agent query API. After
/// [`AppConfig::load`] the fields hold the effective values (env applied,
/// validated).
///
/// ```toml
/// [agent_api]
/// default_max_rows   = 1000      # ARNEB_AGENT_DEFAULT_MAX_ROWS
/// max_rows           = 10000     # ARNEB_AGENT_MAX_ROWS
/// default_page_bytes = 262144    # ARNEB_AGENT_DEFAULT_PAGE_BYTES
/// max_page_bytes     = 4194304   # ARNEB_AGENT_MAX_PAGE_BYTES
/// max_cell_bytes     = 8192      # ARNEB_AGENT_MAX_CELL_BYTES
/// max_retained_bytes = 67108864  # ARNEB_AGENT_MAX_RETAINED_BYTES
/// max_total_retained_bytes = 1073741824  # ARNEB_AGENT_MAX_TOTAL_RETAINED_BYTES
/// max_wait_ms        = 10000     # ARNEB_AGENT_MAX_WAIT_MS
/// idle_timeout_secs  = 600       # ARNEB_AGENT_IDLE_TIMEOUT_SECS
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AgentApiConfig {
    /// Rows a statement retains when the request names no `max_rows`, and
    /// the default listing `limit`.
    pub default_max_rows: usize,
    /// Ceiling for a statement's `max_rows` and a listing's `limit`.
    pub max_rows: usize,
    /// Response bytes per result page when the request names no limit.
    pub default_page_bytes: usize,
    /// Ceiling for a requested page size.
    pub max_page_bytes: usize,
    /// Longest cell, in bytes of a string's UTF-8 text (other values: of
    /// their JSON); longer cells are cut.
    pub max_cell_bytes: usize,
    /// Arrow memory one finished statement may retain; must not exceed
    /// `max_total_retained_bytes`.
    pub max_retained_bytes: usize,
    /// Arrow memory all finished statements together may retain.
    pub max_total_retained_bytes: usize,
    /// Longest any request waits before answering "still running".
    pub max_wait_ms: u64,
    /// Idle time before a statement is reaped; at least twice
    /// `max_wait_ms`.
    pub idle_timeout_secs: u64,
}

impl Default for AgentApiConfig {
    fn default() -> Self {
        let d = arneb_protocol::agent::AgentLimits::default();
        Self {
            default_max_rows: d.default_max_rows,
            max_rows: d.max_rows,
            default_page_bytes: d.default_page_bytes,
            max_page_bytes: d.max_page_bytes,
            max_cell_bytes: d.max_cell_bytes,
            max_retained_bytes: d.max_retained_bytes,
            max_total_retained_bytes: d.max_total_retained_bytes,
            max_wait_ms: d.max_wait.as_millis() as u64,
            idle_timeout_secs: d.idle_timeout.as_secs(),
        }
    }
}

impl AgentApiConfig {
    /// The limits as the agent service consumes them.
    pub fn limits(&self) -> arneb_protocol::agent::AgentLimits {
        arneb_protocol::agent::AgentLimits {
            default_max_rows: self.default_max_rows,
            max_rows: self.max_rows,
            default_page_bytes: self.default_page_bytes,
            max_page_bytes: self.max_page_bytes,
            max_cell_bytes: self.max_cell_bytes,
            max_retained_bytes: self.max_retained_bytes,
            max_total_retained_bytes: self.max_total_retained_bytes,
            max_wait: std::time::Duration::from_millis(self.max_wait_ms),
            idle_timeout: std::time::Duration::from_secs(self.idle_timeout_secs),
        }
    }

    /// Apply the `ARNEB_AGENT_*` variables (looked up through `env`) over
    /// the file values (env > file > defaults), then validate the result.
    pub fn apply_env(&mut self, env: impl Fn(&str) -> Option<String>) -> Result<()> {
        fn set<T: std::str::FromStr>(
            env: &impl Fn(&str) -> Option<String>,
            var: &str,
            field: &mut T,
        ) -> Result<()> {
            if let Some(v) = env(var) {
                *field = v.trim().parse().map_err(|_| {
                    anyhow::anyhow!("invalid {var} value '{v}' (expected a non-negative integer)")
                })?;
            }
            Ok(())
        }
        set(
            &env,
            "ARNEB_AGENT_DEFAULT_MAX_ROWS",
            &mut self.default_max_rows,
        )?;
        set(&env, "ARNEB_AGENT_MAX_ROWS", &mut self.max_rows)?;
        set(
            &env,
            "ARNEB_AGENT_DEFAULT_PAGE_BYTES",
            &mut self.default_page_bytes,
        )?;
        set(&env, "ARNEB_AGENT_MAX_PAGE_BYTES", &mut self.max_page_bytes)?;
        set(&env, "ARNEB_AGENT_MAX_CELL_BYTES", &mut self.max_cell_bytes)?;
        set(
            &env,
            "ARNEB_AGENT_MAX_RETAINED_BYTES",
            &mut self.max_retained_bytes,
        )?;
        set(
            &env,
            "ARNEB_AGENT_MAX_TOTAL_RETAINED_BYTES",
            &mut self.max_total_retained_bytes,
        )?;
        set(&env, "ARNEB_AGENT_MAX_WAIT_MS", &mut self.max_wait_ms)?;
        set(
            &env,
            "ARNEB_AGENT_IDLE_TIMEOUT_SECS",
            &mut self.idle_timeout_secs,
        )?;
        self.limits()
            .validate()
            .map_err(|e| anyhow::anyhow!("invalid [agent_api] settings: {e}"))
    }
}

/// `[mcp]` section: the MCP endpoint served at `/mcp` on the Web port.
/// After [`AppConfig::load`] the fields hold the effective values (env
/// applied, validated).
///
/// ```toml
/// [mcp]
/// enabled            = true      # ARNEB_MCP_ENABLED
/// allowed_hosts      = ["db.internal", "db.internal:6432"]  # ARNEB_MCP_ALLOWED_HOSTS (comma-separated; "*" = any)
/// allowed_origins    = ["https://app.example:443"]  # ARNEB_MCP_ALLOWED_ORIGINS (comma-separated; omit = refuse any Origin)
/// default_page_bytes = 32768     # ARNEB_MCP_DEFAULT_PAGE_BYTES
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct McpConfig {
    /// Serve `/mcp` (coordinator and standalone).
    pub enabled: bool,
    /// Accepted `Host` header values (`host` or `host:port`); `"*"`
    /// accepts any, also when mixed with other hosts. An empty list is
    /// rejected. `None` = any host under password auth, loopback under
    /// `[auth] type = "none"`.
    pub allowed_hosts: Option<Vec<String>>,
    /// Accepted browser `Origin` values (`scheme://host[:port]`, `:*` for
    /// any port, or `null`). A request without `Origin` always passes.
    /// `None` = refuse every request that carries an `Origin`; an empty
    /// list is rejected.
    pub allowed_origins: Option<Vec<String>>,
    /// Page size of tool results when the caller gives no `max_bytes`.
    pub default_page_bytes: usize,
}

impl Default for McpConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            allowed_hosts: None,
            allowed_origins: None,
            default_page_bytes: 32 * 1024,
        }
    }
}

impl McpConfig {
    /// Apply the `ARNEB_MCP_*` variables (looked up through `env`) over the
    /// file values (env > file > defaults).
    /// File lists lose their blank entries the way the comma-separated env
    /// values do; a list left empty is refused by [`Self::validate`].
    pub fn apply_env(&mut self, env: impl Fn(&str) -> Option<String>) -> Result<()> {
        for entries in [&mut self.allowed_hosts, &mut self.allowed_origins]
            .into_iter()
            .flatten()
        {
            *entries = non_blank(entries.iter().map(String::as_str));
        }
        if let Some(v) = env("ARNEB_MCP_ENABLED") {
            self.enabled = parse_env_bool("ARNEB_MCP_ENABLED", &v)?;
        }
        // An empty value counts as unset (file or default applies).
        if let Some(v) = env("ARNEB_MCP_ALLOWED_HOSTS").filter(|v| !v.trim().is_empty()) {
            let hosts = non_blank(v.split(','));
            if hosts.is_empty() {
                bail!(
                    "ARNEB_MCP_ALLOWED_HOSTS '{v}' lists no hosts; unset it for the default \
                     or use * for any host"
                );
            }
            self.allowed_hosts = Some(hosts);
        }
        if let Some(v) = env("ARNEB_MCP_ALLOWED_ORIGINS").filter(|v| !v.trim().is_empty()) {
            let origins = non_blank(v.split(','));
            if origins.is_empty() {
                bail!(
                    "ARNEB_MCP_ALLOWED_ORIGINS '{v}' lists no origins; unset it to refuse \
                     every request that carries an Origin"
                );
            }
            self.allowed_origins = Some(origins);
        }
        if let Some(v) = env("ARNEB_MCP_DEFAULT_PAGE_BYTES") {
            self.default_page_bytes = v.trim().parse().map_err(|_| {
                anyhow::anyhow!(
                    "invalid ARNEB_MCP_DEFAULT_PAGE_BYTES value '{v}' \
                     (expected a non-negative integer)"
                )
            })?;
        }
        Ok(())
    }

    /// `default_page_bytes` must be in `1..=max_page_bytes` (the agent API's
    /// ceiling).
    pub fn validate(&self, max_page_bytes: usize) -> std::result::Result<(), String> {
        if let Some(hosts) = &self.allowed_hosts {
            if hosts.iter().all(|h| h.trim().is_empty()) {
                return Err(
                    "[mcp] allowed_hosts is empty; omit it for the default or use \
                            [\"*\"] for any host"
                        .into(),
                );
            }
        }
        if let Some(origins) = &self.allowed_origins {
            if origins.iter().all(|o| o.trim().is_empty()) {
                return Err(
                    "[mcp] allowed_origins is empty; omit it to refuse every request that \
                     carries an Origin"
                        .into(),
                );
            }
            if let Some(bad) = origins.iter().find(|o| {
                let o = o.trim();
                !o.is_empty() && !o.contains("://") && !o.eq_ignore_ascii_case("null")
            }) {
                return Err(format!(
                    "[mcp] allowed_origins entry '{bad}' has no scheme; write it as \
                     scheme://host[:port] (for example https://{bad}) or null"
                ));
            }
        }
        if self.default_page_bytes == 0 {
            return Err("default_page_bytes must be greater than 0".into());
        }
        if self.default_page_bytes > max_page_bytes {
            return Err(format!(
                "default_page_bytes ({}) must not exceed [agent_api] max_page_bytes ({max_page_bytes})",
                self.default_page_bytes
            ));
        }
        Ok(())
    }
}

/// A boolean env value: `true`/`1`/`yes`/`on` or `false`/`0`/`no`/`off`,
/// trimmed and in any case. Anything else is an error naming `name`.
fn parse_env_bool(name: &str, value: &str) -> Result<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "on" | "yes" => Ok(true),
        "0" | "false" | "off" | "no" => Ok(false),
        other => bail!("invalid {name} value '{other}' (expected true/false)"),
    }
}

/// The trimmed, non-blank entries of `entries`.
fn non_blank<'a>(entries: impl Iterator<Item = &'a str>) -> Vec<String> {
    entries
        .map(str::trim)
        .filter(|e| !e.is_empty())
        .map(str::to_owned)
        .collect()
}

/// `[observability]` section.
///
/// ```toml
/// [observability]
/// slow_query_threshold_ms = 10000   # ARNEB_SLOW_QUERY_THRESHOLD_MS; 0 = slow-query log off
/// query_history_size      = 100     # ARNEB_QUERY_HISTORY_SIZE; finished queries kept, > 0
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ObservabilityConfig {
    /// Statements running at least this long are logged on the
    /// `arneb::slow_query` target; `0` turns the log off.
    pub slow_query_threshold_ms: u64,
    /// Finished queries the query tracker keeps for the Web UI and
    /// `system.runtime.queries`; must be positive.
    pub query_history_size: usize,
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            slow_query_threshold_ms: arneb_common::metrics::DEFAULT_SLOW_QUERY_THRESHOLD_MS,
            query_history_size: arneb_scheduler::DEFAULT_QUERY_HISTORY_SIZE,
        }
    }
}

impl ObservabilityConfig {
    /// Apply `ARNEB_SLOW_QUERY_THRESHOLD_MS` and `ARNEB_QUERY_HISTORY_SIZE`
    /// (looked up through `env`) over the file values, then validate.
    pub fn apply_env(&mut self, env: impl Fn(&str) -> Option<String>) -> Result<()> {
        if let Some(v) = env("ARNEB_SLOW_QUERY_THRESHOLD_MS") {
            self.slow_query_threshold_ms = v.trim().parse().map_err(|_| {
                anyhow::anyhow!(
                    "invalid ARNEB_SLOW_QUERY_THRESHOLD_MS value '{v}' (expected milliseconds, 0 = off)"
                )
            })?;
        }
        if let Some(v) = env("ARNEB_QUERY_HISTORY_SIZE") {
            self.query_history_size = v.trim().parse().map_err(|_| {
                anyhow::anyhow!(
                    "invalid ARNEB_QUERY_HISTORY_SIZE value '{v}' (expected a positive integer)"
                )
            })?;
        }
        // 0 would evict every statement the moment it finished.
        anyhow::ensure!(
            self.query_history_size > 0,
            "[observability] query_history_size / ARNEB_QUERY_HISTORY_SIZE must be positive"
        );
        Ok(())
    }

    /// The slow-query threshold; `None` when the log is off.
    pub fn slow_query_threshold(&self) -> Option<std::time::Duration> {
        (self.slow_query_threshold_ms > 0)
            .then(|| std::time::Duration::from_millis(self.slow_query_threshold_ms))
    }
}

/// `[auth]` section: client authentication for the server.
///
/// One credential store for every client-facing listener. Password auth is
/// enforced on pgwire (SCRAM) and on the Trino listener (HTTP Basic against
/// the same verifiers).
///
/// ```toml
/// [auth]
/// type = "password"            # "none" (default) | "password"
///
/// [[auth.users]]
/// name = "alice"
/// password_hash = "SCRAM-SHA-256$4096:<salt>$<StoredKey>:<ServerKey>"
/// ```
///
/// `password` mode runs a SCRAM-SHA-256 exchange; only PostgreSQL-format
/// SCRAM verifiers are stored (generate one with `arneb hash-password`).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthConfig {
    /// Authentication mode.
    #[serde(rename = "type", default)]
    pub auth_type: AuthType,
    /// Users allowed to connect when `type = "password"`.
    #[serde(default)]
    pub users: Vec<AuthUserConfig>,
}

/// Authentication mode for pgwire connections.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AuthType {
    /// No authentication: every connection is accepted (default).
    #[default]
    None,
    /// SCRAM-SHA-256 password authentication.
    Password,
}

/// One `[[auth.users]]` entry.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthUserConfig {
    /// Login name (matched against the startup `user` parameter).
    pub name: String,
    /// PostgreSQL-format SCRAM-SHA-256 verifier.
    pub password_hash: String,
}

impl std::fmt::Debug for AuthUserConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthUserConfig")
            .field("name", &self.name)
            .field("password_hash", &"<redacted>")
            .finish()
    }
}

impl AuthConfig {
    /// Parse the configured `password_hash` strings, in file order. These
    /// seed the state store, so they are parsed, never regenerated: the seed
    /// comparison must be deterministic. An empty or duplicate name is an
    /// error.
    pub fn user_verifiers(&self) -> Result<Vec<(String, ScramVerifier)>> {
        let mut seen = std::collections::HashSet::new();
        self.users
            .iter()
            .map(|u| {
                anyhow::ensure!(!u.name.is_empty(), "[auth] user name must not be empty");
                anyhow::ensure!(
                    seen.insert(u.name.as_str()),
                    "[auth] duplicate user '{}'",
                    u.name
                );
                let verifier: ScramVerifier = u
                    .password_hash
                    .parse()
                    .map_err(|e| anyhow::anyhow!("[auth] user '{}': {e}", u.name))?;
                Ok((u.name.clone(), verifier))
            })
            .collect()
    }

    /// Validate the section and build the protocol-layer [`AuthMethod`],
    /// plus the parsed users (empty under `none`) that seed the state store.
    /// Every `password_hash` is parsed and validated once, through
    /// [`AuthConfig::user_verifiers`].
    pub fn to_auth_method(&self) -> Result<(AuthMethod, Vec<(String, ScramVerifier)>)> {
        match self.auth_type {
            AuthType::None => {
                if !self.users.is_empty() {
                    tracing::warn!(
                        users = self.users.len(),
                        "[auth] users are configured but type = \"none\"; they are ignored"
                    );
                }
                Ok((AuthMethod::None, Vec::new()))
            }
            AuthType::Password => {
                let users = self.user_verifiers()?;
                let creds = UserCredentials::new(users.iter().cloned())
                    .map_err(|e| anyhow::anyhow!("[auth] {e}"))?;
                Ok((AuthMethod::ScramSha256(Arc::new(creds)), users))
            }
        }
    }
}

/// Fail closed: `[cluster.tls]` without a `[cluster]` secret encrypts the
/// Flight port but authenticates no peer (the server asks for no client
/// certificate), so on a non-loopback bind any host that reaches it could
/// submit tasks and read exchange data.
pub fn check_cluster_transport(secret: bool, tls: bool, bind_address: &str) -> Result<()> {
    if tls && !secret && !is_loopback_host(bind_address) {
        bail!(
            "[cluster.tls] is set without a [cluster] secret on non-loopback bind_address \
             '{bind_address}': TLS encrypts cluster RPC but does not authenticate peers. Set \
             [cluster] secret (or ARNEB_CLUSTER_SECRET) on every node"
        );
    }
    Ok(())
}

/// Fail closed: password auth on an enabled Trino listener needs `[tls]`.
/// Trino clients only send passwords over HTTPS, so a plain-HTTP listener
/// under password auth would be either unusable or a bypass. Stricter than
/// the `TrinoServer::serve` guard, which still allows a loopback-only
/// listener (tests, local tunnels).
pub fn check_trino_transport(trino_enabled: bool, password_auth: bool, tls: bool) -> Result<()> {
    if trino_enabled && password_auth && !tls {
        bail!(
            "[auth] type = \"password\" requires TLS on the Trino listener: configure [tls] \
             (or ARNEB_TLS_CERT_PATH / ARNEB_TLS_KEY_PATH), or disable the listener with \
             --no-trino / ARNEB_TRINO_ENABLED=false"
        );
    }
    Ok(())
}

/// Trino client REST protocol (`POST /v1/statement`) listener.
///
/// ```toml
/// [trino]
/// enabled = true   # ARNEB_TRINO_ENABLED, CLI --no-trino
/// port = 8080      # ARNEB_TRINO_PORT,    CLI --trino-port
/// ```
///
/// Served on `bind_address:port` by coordinators and standalone nodes;
/// workers never serve it.
#[derive(Debug, Clone, Deserialize)]
pub struct TrinoProtocolConfig {
    /// Whether to serve the Trino client protocol. Default: `true`.
    #[serde(default = "default_trino_enabled")]
    pub enabled: bool,
    /// HTTP port. Default: `8080` (Trino's default, so clients connect
    /// without extra configuration).
    #[serde(default = "default_trino_port")]
    pub port: u16,
}

fn default_trino_enabled() -> bool {
    true
}

fn default_trino_port() -> u16 {
    8080
}

impl Default for TrinoProtocolConfig {
    fn default() -> Self {
        Self {
            enabled: default_trino_enabled(),
            port: default_trino_port(),
        }
    }
}

impl TrinoProtocolConfig {
    /// Applies `ARNEB_TRINO_ENABLED` / `ARNEB_TRINO_PORT` (env > file).
    pub fn apply_env_overrides(&mut self) -> Result<()> {
        if let Ok(val) = std::env::var("ARNEB_TRINO_ENABLED") {
            self.enabled = parse_env_bool("ARNEB_TRINO_ENABLED", &val)?;
        }
        if let Ok(val) = std::env::var("ARNEB_TRINO_PORT") {
            if arneb_common::config::is_service_link(&val) {
                tracing::warn!(
                    value = %val,
                    "ignoring ARNEB_TRINO_PORT: it is a Kubernetes service link for a \
                     Service named \"arneb-trino\", not a port"
                );
                return self.check_port();
            }
            self.port = val
                .trim()
                .parse()
                .map_err(|_| anyhow::anyhow!("invalid ARNEB_TRINO_PORT value '{val}'"))?;
        }
        self.check_port()
    }

    fn check_port(&self) -> Result<()> {
        if self.enabled && self.port == 0 {
            bail!("trino.port must be > 0");
        }
        Ok(())
    }
}

/// Memory budget for spillable operators (currently SemiJoinExec build
/// side; HashJoinExec coming in Phase 3).
///
/// Default behavior is *unbounded* (matches single-node and standalone
/// runs). In container deployments, set `spill_budget_bytes` to a
/// fraction of the container's cgroup limit so the build phase will
/// spill batches to disk instead of being OOM-killed by the kernel.
///
/// When `spill_budget_bytes` is `None` and cgroup auto-detection is
/// enabled (default) the server tries to read the cgroup limit and
/// applies `cgroup_ratio` to derive a budget. Set `cgroup_ratio = 0.0`
/// to disable auto-detect entirely.
#[derive(Debug, Clone, Deserialize)]
pub struct MemoryConfig {
    /// Hard byte budget for spillable operators. When set, overrides
    /// cgroup auto-detection. `None` means "use cgroup auto-detect, or
    /// unbounded if unavailable".
    #[serde(default)]
    pub spill_budget_bytes: Option<u64>,

    /// Fraction of the cgroup memory limit reserved for spillable
    /// operators when `spill_budget_bytes` is unset. Defaults to 0.5;
    /// the other 50% goes to Arrow scratch, hash tables outside the
    /// pool, RPC buffers, etc.
    #[serde(default = "default_cgroup_ratio")]
    pub cgroup_ratio: f64,

    /// Trino-style per-query memory limit on each node
    /// (`query.max-memory-per-node`). Every query gets its own
    /// `QueryMemoryPool` of this many bytes over the node pool — per
    /// statement on the coordinator / standalone, per query id on a worker —
    /// so one query fails with `ResourceExhausted` (pgwire `53200`, Trino
    /// `EXCEEDED_LOCAL_MEMORY_LIMIT`) instead of taking the node's memory
    /// from the others. It is per node, not cluster-wide: a query on 2
    /// workers can use up to 2× this. Recommended 0.5–0.8 × the node spill
    /// budget. `None` (default): a query is limited only by the node pool.
    #[serde(default)]
    pub query_max_memory_per_node: Option<u64>,

    /// DEPRECATED (Phase A, 2026-05-23): the per-worker admission
    /// semaphore that this field configured was removed because it
    /// held a permit across the entire task body, deadlocking
    /// downstream stream back-pressure. The field is still
    /// deserialised so existing config files don't fail to load, but
    /// it has no runtime effect. `main.rs` emits a `tracing::warn!`
    /// when this is set to a non-default value.
    ///
    /// History (kept for context): formerly Trino's `task.max-worker-
    /// threads` equivalent (default `2 × cpu_cores`). For arneb on a
    /// 6 GiB Docker quota with 5-way joins like TPC-H Q09, 10+
    /// concurrent fragments each holding scan + outputbuffer +
    /// in-flight join state used to overflow the cgroup before any
    /// single task's spill path fired. Phase 3b.6a (2026-05-21) made
    /// it tunable; Phase A (2026-05-23) removed the semaphore entirely
    /// because it conflicted with per-batch streaming back-pressure.
    #[serde(default = "default_task_concurrency")]
    pub task_concurrency: usize,

    /// Worker RSS threshold (bytes) above which `task_manager`
    /// refuses new task admission until jemalloc's `stats.resident`
    /// drops back below. `None` disables (count-based admission only).
    ///
    /// Phase 3b.6b (2026-05-21): catches the untracked Arrow
    /// allocations (FilterExec / Project / Repartition channels /
    /// FlightDecoder) that the per-operator `MemoryPool` doesn't
    /// account for. Set this conservatively below the container's
    /// memory limit (e.g. 70-80% of `cgroup memory.max`) so the
    /// kernel OOM-killer never wins the race against arneb's
    /// admission gate.
    #[serde(default)]
    pub task_admission_threshold_bytes: Option<u64>,
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            spill_budget_bytes: None,
            cgroup_ratio: default_cgroup_ratio(),
            query_max_memory_per_node: None,
            task_concurrency: default_task_concurrency(),
            task_admission_threshold_bytes: None,
        }
    }
}

fn default_cgroup_ratio() -> f64 {
    0.5
}

pub(crate) fn default_task_concurrency() -> usize {
    // DEPRECATED (Phase A, 2026-05-23): kept only so main.rs can
    // detect "user explicitly set a non-default value" and warn.
    // The value itself has no runtime effect now that the admission
    // semaphore is gone.
    16
}

impl MemoryConfig {
    /// Resolve the effective budget in bytes. Order:
    ///   1. `ARNEB_SPILL_BUDGET_BYTES` env var if set (runtime override)
    ///   2. `spill_budget_bytes` config field if set
    ///   3. cgroup-derived `limit × cgroup_ratio` if `cgroup_ratio > 0`
    ///   4. None (unbounded)
    ///
    /// Returns `(Some(bytes), source)` for logging, where `source` is
    /// "env", "config", "cgroup_v2", "cgroup_v1", or "unbounded".
    ///
    /// The env override exists because the bench config is COPYed into
    /// the docker image at build time, so changing the config field
    /// requires an image rebuild. `ARNEB_SPILL_BUDGET_BYTES` lets a
    /// deployment or A/B experiment retune the budget without rebuilding
    /// — per the runtime-tunable convention in docs/guide/configuration.md.
    pub fn resolve_budget(&self) -> (Option<usize>, &'static str) {
        if let Some(b) = std::env::var("ARNEB_SPILL_BUDGET_BYTES")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
        {
            return (Some(b as usize), "env");
        }
        if let Some(b) = self.spill_budget_bytes {
            return (Some(b as usize), "config");
        }
        if self.cgroup_ratio <= 0.0 {
            return (None, "unbounded");
        }
        // cgroup v2 first, then v1.
        for (path, source) in [
            ("/sys/fs/cgroup/memory.max", "cgroup_v2"),
            ("/sys/fs/cgroup/memory/memory.limit_in_bytes", "cgroup_v1"),
        ] {
            if let Ok(s) = std::fs::read_to_string(path) {
                let s = s.trim();
                // cgroup v2 writes "max" for unbounded.
                if s == "max" {
                    continue;
                }
                if let Ok(limit) = s.parse::<u64>() {
                    // Treat absurdly large values (e.g. host-mode
                    // unconstrained cgroup v1 returns ~2^63) as
                    // unbounded — applying 50% would still be huge,
                    // but the threshold here keeps the log honest.
                    if limit > (1u64 << 60) {
                        continue;
                    }
                    let budget = ((limit as f64) * self.cgroup_ratio) as usize;
                    return (Some(budget), source);
                }
            }
        }
        (None, "unbounded")
    }

    /// Resolve the per-query memory limit. `ARNEB_QUERY_MAX_MEMORY_BYTES`
    /// env var overrides the `query_max_memory_per_node` config field —
    /// same runtime-tunable rationale as `resolve_budget` (config is baked
    /// into the docker image at build time).
    pub fn resolve_query_cap(&self) -> Option<u64> {
        if let Some(b) = std::env::var("ARNEB_QUERY_MAX_MEMORY_BYTES")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
        {
            return Some(b);
        }
        self.query_max_memory_per_node
    }
}

/// External catalog configuration.
#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct CatalogConfig {
    /// Catalog name (used in SQL: `SELECT * FROM <name>.schema.table`).
    pub name: String,
    /// Catalog type: `"hive"` (Iceberg tables in the metastore are read
    /// through it) or `"postgresql"`.
    #[serde(rename = "type")]
    pub catalog_type: String,
    /// Hive Metastore URI (e.g., "thrift://hms.internal:9083").
    pub metastore_uri: Option<String>,
    /// Default schema name within this catalog (used when it is the
    /// default catalog). Unset: `default`, or `public` for `postgresql`;
    /// see [`effective_default_schema`](Self::effective_default_schema).
    #[serde(default)]
    pub default_schema: Option<String>,
    /// Per-catalog storage configuration (overrides global [storage]).
    pub storage: Option<StorageConfig>,

    // ---- postgresql only ----
    /// `postgresql://host:port/database`. Redacted in `Debug`: it may carry
    /// a password.
    pub connection_url: Option<RedactedString>,
    /// Remote user; overrides one in `connection_url`.
    pub user: Option<String>,
    /// Remote password; `ARNEB_CATALOG_<NAME>_PASSWORD` overrides it.
    pub password: Option<RedactedString>,
    /// Connection pool size per node (default 8).
    pub max_connections: Option<usize>,
    /// Minimum estimated rows per primary-key range split (default 500000).
    pub split_min_rows: Option<u64>,
    /// Maximum splits per table (default 16).
    pub max_splits: Option<usize>,
    /// Scale for unconstrained `numeric` columns (default 10).
    pub unconstrained_numeric_scale: Option<i8>,
    /// Seconds allowed to open one connection (default 10).
    pub connect_timeout_secs: Option<u64>,
    /// Seconds to wait for a free pooled connection (default 60).
    pub pool_wait_timeout_secs: Option<u64>,
    /// A dynamic-filter `IN` list longer than this is pushed as a range
    /// (default 1000).
    pub domain_compaction_threshold: Option<usize>,
}

impl CatalogConfig {
    /// The configured default schema, or the type's conventional one.
    pub fn effective_default_schema(&self) -> &str {
        match (&self.default_schema, self.catalog_type.as_str()) {
            (Some(s), _) => s,
            (None, "postgresql") => "public",
            (None, _) => "default",
        }
    }

    /// The environment variable that overrides this catalog's password:
    /// `ARNEB_CATALOG_<NAME>_PASSWORD`, with the name upper-cased and every
    /// non-alphanumeric character replaced by `_`.
    pub fn password_env_var(&self) -> String {
        let name: String = self
            .name
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() {
                    c.to_ascii_uppercase()
                } else {
                    '_'
                }
            })
            .collect();
        format!("ARNEB_CATALOG_{name}_PASSWORD")
    }

    /// Apply `ARNEB_*` overrides (currently the password).
    pub fn apply_env(&mut self, env: impl Fn(&str) -> Option<String>) {
        if let Some(password) = env(&self.password_env_var()) {
            self.password = Some(RedactedString(password));
        }
    }

    /// The PostgreSQL connector settings, or an error naming the missing
    /// field.
    pub fn postgres_config(&self) -> Result<arneb_postgresql::PostgresConfig> {
        let url = self.connection_url.as_ref().ok_or_else(|| {
            anyhow::anyhow!("postgresql catalog '{}' requires connection_url", self.name)
        })?;
        let mut c = arneb_postgresql::PostgresConfig::new(url.0.clone());
        c.user = self.user.clone();
        c.password = self.password.as_ref().map(|p| p.0.clone());
        if let Some(v) = self.max_connections {
            anyhow::ensure!(
                v > 0,
                "catalog '{}': max_connections must be > 0",
                self.name
            );
            c.max_connections = v;
        }
        if let Some(v) = self.split_min_rows {
            anyhow::ensure!(v > 0, "catalog '{}': split_min_rows must be > 0", self.name);
            c.split_min_rows = v;
        }
        if let Some(v) = self.max_splits {
            anyhow::ensure!(v > 0, "catalog '{}': max_splits must be > 0", self.name);
            c.max_splits = v;
        }
        if let Some(v) = self.unconstrained_numeric_scale {
            anyhow::ensure!(
                (0..=38).contains(&v),
                "catalog '{}': unconstrained_numeric_scale must be within 0..=38",
                self.name
            );
            c.unconstrained_numeric_scale = v;
        }
        if let Some(v) = self.connect_timeout_secs {
            anyhow::ensure!(
                v > 0,
                "catalog '{}': connect_timeout_secs must be > 0",
                self.name
            );
            c.connect_timeout_secs = v;
        }
        if let Some(v) = self.pool_wait_timeout_secs {
            anyhow::ensure!(
                v > 0,
                "catalog '{}': pool_wait_timeout_secs must be > 0",
                self.name
            );
            c.pool_wait_timeout_secs = v;
        }
        if let Some(v) = self.domain_compaction_threshold {
            anyhow::ensure!(
                v > 0,
                "catalog '{}': domain_compaction_threshold must be > 0",
                self.name
            );
            c.domain_compaction_threshold = v;
        }
        Ok(c)
    }
}

/// Refuse two PostgreSQL catalogs whose names normalise to the same
/// `ARNEB_CATALOG_<NAME>_PASSWORD` (`pg-prod` and `pg_prod`): one variable
/// would silently set both passwords.
fn check_password_env_vars(catalogs: &[CatalogConfig]) -> Result<()> {
    let mut seen: std::collections::HashMap<String, &str> = std::collections::HashMap::new();
    for c in catalogs.iter().filter(|c| c.catalog_type == "postgresql") {
        if let Some(other) = seen.insert(c.password_env_var(), &c.name) {
            anyhow::bail!(
                "catalogs '{other}' and '{}' would share {}; rename one",
                c.name,
                c.password_env_var()
            );
        }
    }
    Ok(())
}

/// Global storage backend configuration.
#[derive(Debug, Default, Clone, Deserialize)]
#[allow(dead_code)]
pub struct StorageConfig {
    /// AWS S3 configuration.
    pub s3: Option<S3Config>,
    /// Google Cloud Storage configuration.
    pub gcs: Option<GcsConfig>,
    /// Azure Blob Storage configuration.
    pub azure: Option<AzureConfig>,
}

/// AWS S3 storage configuration.
///
/// Credential resolution: config > env var (`AWS_ACCESS_KEY_ID`) > IAM role.
#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct S3Config {
    /// AWS region (e.g., "us-east-1").
    pub region: Option<String>,
    /// Custom endpoint URL (for MinIO, LocalStack, etc.).
    pub endpoint: Option<String>,
    /// Allow HTTP (non-TLS) connections. Default: false.
    #[serde(default)]
    pub allow_http: bool,
    /// AWS access key ID. Overrides `AWS_ACCESS_KEY_ID` env var.
    pub access_key_id: Option<String>,
    /// AWS secret access key. Overrides `AWS_SECRET_ACCESS_KEY` env var.
    pub secret_access_key: Option<String>,
}

/// Google Cloud Storage configuration.
#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct GcsConfig {
    /// Path to service account JSON key file.
    pub service_account_path: Option<String>,
}

/// Azure Blob Storage configuration.
#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct AzureConfig {
    /// Azure storage account name.
    pub storage_account: Option<String>,
    /// Azure storage access key.
    pub access_key: Option<String>,
}

impl StorageConfig {
    /// Merge per-catalog storage config over global config.
    ///
    /// Per-catalog fields take precedence when present;
    /// global fields are used as fallback.
    pub fn merge(global: &StorageConfig, catalog: Option<&StorageConfig>) -> StorageConfig {
        let catalog = match catalog {
            Some(c) => c,
            None => return global.clone(),
        };

        StorageConfig {
            s3: match (&catalog.s3, &global.s3) {
                (Some(cat), _) => Some(cat.clone()),
                (None, global_s3) => global_s3.clone(),
            },
            gcs: match (&catalog.gcs, &global.gcs) {
                (Some(cat), _) => Some(cat.clone()),
                (None, global_gcs) => global_gcs.clone(),
            },
            azure: match (&catalog.azure, &global.azure) {
                (Some(cat), _) => Some(cat.clone()),
                (None, global_azure) => global_azure.clone(),
            },
        }
    }

    /// Convert to the connector-layer `CloudStorageConfig`.
    pub fn to_cloud_config(&self) -> CloudStorageConfig {
        CloudStorageConfig {
            s3: self.s3.as_ref().map(|s3| S3StorageConfig {
                region: s3.region.clone(),
                endpoint: s3.endpoint.clone(),
                allow_http: s3.allow_http,
                access_key_id: s3.access_key_id.clone(),
                secret_access_key: s3.secret_access_key.clone(),
            }),
        }
    }
}

/// Cluster configuration for distributed mode.
#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct ClusterConfig {
    /// Server role: "standalone" (default), "coordinator", or "worker".
    #[serde(default = "default_role")]
    pub role: String,

    /// Address of the coordinator (for workers to connect to).
    #[serde(default)]
    pub coordinator_address: Option<String>,

    /// Port for inter-node RPC communication (Flight + gRPC).
    #[serde(default = "default_rpc_port")]
    pub rpc_port: u16,

    /// Unique worker ID (auto-generated if not set).
    #[serde(default)]
    pub worker_id: Option<String>,

    /// Address this node advertises to peers for incoming RPC (e.g.
    /// `arneb-worker-1:9090` in docker). Workers send this in their
    /// heartbeat so the coordinator knows how to route task RPCs back.
    /// Defaults to `bind_address:rpc_port`, which only works when peers
    /// share the same loopback (e.g. all on one host or k8s pod).
    #[serde(default)]
    pub advertised_address: Option<String>,

    /// Shared secret every node sends as `authorization: Bearer` on cluster
    /// RPC (`ARNEB_CLUSTER_SECRET`, preferred). At least 32 characters.
    #[serde(default)]
    pub secret: Option<RedactedString>,

    /// TLS between nodes (`[cluster.tls]`).
    #[serde(default)]
    pub tls: Option<ClusterTlsConfig>,

    /// Seconds between a worker's heartbeats (`ARNEB_HEARTBEAT_INTERVAL_SECS`).
    #[serde(default)]
    pub heartbeat_interval_secs: Option<u64>,

    /// Seconds without a heartbeat after which the coordinator treats a
    /// worker as lost and fails its queries (`ARNEB_HEARTBEAT_TIMEOUT_SECS`).
    #[serde(default)]
    pub heartbeat_timeout_secs: Option<u64>,

    /// Longest a worker waits on SIGTERM for its queries to finish before
    /// exiting (`ARNEB_DRAIN_TIMEOUT_SECS`).
    #[serde(default)]
    pub drain_timeout_secs: Option<u64>,

    /// `none` (default) or `query`: re-run a read-only query that failed
    /// because one of its workers was lost (`ARNEB_RETRY_POLICY`).
    #[serde(default)]
    pub retry_policy: Option<String>,

    /// Most re-runs of one query under `retry_policy = "query"`
    /// (`ARNEB_QUERY_RETRY_ATTEMPTS`).
    #[serde(default)]
    pub query_retry_attempts: Option<u32>,
}

/// Default `[cluster] drain_timeout_secs`.
pub const DEFAULT_DRAIN_TIMEOUT_SECS: u64 = 120;

/// Default `[cluster] query_retry_attempts`.
pub const DEFAULT_QUERY_RETRY_ATTEMPTS: u32 = 4;

/// Default `[cluster] heartbeat_interval_secs`.
pub const DEFAULT_HEARTBEAT_INTERVAL_SECS: u64 = 2;
/// Default `[cluster] heartbeat_timeout_secs`.
pub const DEFAULT_HEARTBEAT_TIMEOUT_SECS: u64 = 10;

/// Effective heartbeat settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeartbeatSettings {
    /// Seconds between a worker's heartbeats.
    pub interval_secs: u64,
    /// Seconds of silence after which a worker is lost.
    pub timeout_secs: u64,
}

/// A string that never appears in `Debug` output.
#[derive(Clone, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct RedactedString(pub String);

impl std::fmt::Debug for RedactedString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("<redacted>")
    }
}

/// `[cluster.tls]`: this node's certificate and key, plus the CA that
/// signed every node's certificate (all PEM).
///
/// ```toml
/// [cluster.tls]
/// cert_path = "/etc/arneb/node.crt"   # ARNEB_CLUSTER_TLS_CERT_PATH
/// key_path  = "/etc/arneb/node.key"   # ARNEB_CLUSTER_TLS_KEY_PATH
/// ca_path   = "/etc/arneb/ca.crt"     # ARNEB_CLUSTER_TLS_CA_PATH
/// ```
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterTlsConfig {
    #[serde(default)]
    pub cert_path: Option<PathBuf>,
    #[serde(default)]
    pub key_path: Option<PathBuf>,
    #[serde(default)]
    pub ca_path: Option<PathBuf>,
}

/// A whole-seconds setting of at least 1: `var` from `env`, else `file`,
/// else `default`.
fn positive_secs(
    env: &impl Fn(&str) -> Option<String>,
    var: &str,
    file: Option<u64>,
    default: u64,
    what: &str,
) -> Result<u64> {
    let value = match env(var) {
        Some(v) => v
            .trim()
            .parse::<u64>()
            .map_err(|_| anyhow::anyhow!("invalid {var} value '{v}' (expected whole seconds)"))?,
        None => file.unwrap_or(default),
    };
    anyhow::ensure!(value >= 1, "{var} / {what} must be at least 1 s");
    Ok(value)
}

impl ClusterConfig {
    /// Apply `ARNEB_CLUSTER_COORDINATOR_ADDRESS`, `ARNEB_CLUSTER_WORKER_ID`
    /// and `ARNEB_CLUSTER_ADVERTISED_ADDRESS` (env > file), so one config
    /// file can serve every worker. An empty value is an error.
    pub fn apply_env(&mut self, env: impl Fn(&str) -> Option<String>) -> Result<()> {
        for (var, field) in [
            (
                "ARNEB_CLUSTER_COORDINATOR_ADDRESS",
                &mut self.coordinator_address,
            ),
            ("ARNEB_CLUSTER_WORKER_ID", &mut self.worker_id),
            (
                "ARNEB_CLUSTER_ADVERTISED_ADDRESS",
                &mut self.advertised_address,
            ),
        ] {
            if let Some(v) = env(var) {
                let v = v.trim();
                anyhow::ensure!(!v.is_empty(), "{var} is set but empty");
                *field = Some(v.to_owned());
            }
        }
        Ok(())
    }

    /// `[cluster] heartbeat_*_secs` with `ARNEB_HEARTBEAT_*_SECS` overrides
    /// (env > file > defaults), validated: both at least 1, timeout greater
    /// than interval.
    pub fn heartbeat(&self, env: impl Fn(&str) -> Option<String>) -> Result<HeartbeatSettings> {
        let read = |var, file, default| {
            positive_secs(&env, var, file, default, "[cluster] heartbeat setting")
        };
        let interval_secs = read(
            "ARNEB_HEARTBEAT_INTERVAL_SECS",
            self.heartbeat_interval_secs,
            DEFAULT_HEARTBEAT_INTERVAL_SECS,
        )?;
        let timeout_secs = read(
            "ARNEB_HEARTBEAT_TIMEOUT_SECS",
            self.heartbeat_timeout_secs,
            DEFAULT_HEARTBEAT_TIMEOUT_SECS,
        )?;
        anyhow::ensure!(
            timeout_secs > interval_secs,
            "heartbeat_timeout_secs ({timeout_secs}) must be greater than \
             heartbeat_interval_secs ({interval_secs})"
        );
        Ok(HeartbeatSettings {
            interval_secs,
            timeout_secs,
        })
    }

    /// `[cluster] drain_timeout_secs` with the `ARNEB_DRAIN_TIMEOUT_SECS`
    /// override (env > file > default), at least 1.
    pub fn drain_timeout_secs(&self, env: impl Fn(&str) -> Option<String>) -> Result<u64> {
        positive_secs(
            &env,
            "ARNEB_DRAIN_TIMEOUT_SECS",
            self.drain_timeout_secs,
            DEFAULT_DRAIN_TIMEOUT_SECS,
            "[cluster] drain_timeout_secs",
        )
    }

    /// `[cluster] retry_policy` / `query_retry_attempts` with the
    /// `ARNEB_RETRY_POLICY` / `ARNEB_QUERY_RETRY_ATTEMPTS` overrides (env >
    /// file > defaults). `None` when retry is off.
    pub fn query_retry(
        &self,
        env: impl Fn(&str) -> Option<String>,
    ) -> Result<Option<crate::coordinator::QueryRetry>> {
        let policy = env("ARNEB_RETRY_POLICY")
            .or_else(|| self.retry_policy.clone())
            .unwrap_or_else(|| "none".to_string());
        match policy.trim().to_ascii_lowercase().as_str() {
            "none" => return Ok(None),
            "query" => {}
            "task" => bail!(
                "retry_policy 'task' is not supported (it needs exchange spooling); \
                 use 'query' or 'none'"
            ),
            other => bail!(
                "invalid ARNEB_RETRY_POLICY / [cluster] retry_policy '{other}' \
                 (expected 'none' or 'query')"
            ),
        }
        let attempts = match env("ARNEB_QUERY_RETRY_ATTEMPTS") {
            Some(v) => v.trim().parse::<u32>().map_err(|_| {
                anyhow::anyhow!("invalid ARNEB_QUERY_RETRY_ATTEMPTS value '{v}' (expected a count)")
            })?,
            None => self
                .query_retry_attempts
                .unwrap_or(DEFAULT_QUERY_RETRY_ATTEMPTS),
        };
        anyhow::ensure!(
            attempts >= 1,
            "ARNEB_QUERY_RETRY_ATTEMPTS / [cluster] query_retry_attempts must be at least 1"
        );
        Ok(Some(crate::coordinator::QueryRetry::new(attempts)))
    }

    /// Build the cluster RPC security from `[cluster] secret` /
    /// `[cluster.tls]` with `ARNEB_CLUSTER_*` overrides (looked up through
    /// `env`). Reads the PEM files.
    pub fn security(
        &self,
        env: impl Fn(&str) -> Option<String>,
    ) -> Result<arneb_rpc::ClusterSecurity> {
        let secret = env("ARNEB_CLUSTER_SECRET")
            .or_else(|| self.secret.as_ref().map(|s| s.0.clone()))
            .map(arneb_rpc::ClusterSecret::new)
            .transpose()
            .map_err(|e| anyhow::anyhow!(e))?;

        let mut tls = self.tls.clone();
        for (var, field) in [
            ("ARNEB_CLUSTER_TLS_CERT_PATH", 0),
            ("ARNEB_CLUSTER_TLS_KEY_PATH", 1),
            ("ARNEB_CLUSTER_TLS_CA_PATH", 2),
        ] {
            if let Some(v) = env(var) {
                let t = tls.get_or_insert_with(ClusterTlsConfig::default);
                let slot = match field {
                    0 => &mut t.cert_path,
                    1 => &mut t.key_path,
                    _ => &mut t.ca_path,
                };
                *slot = Some(PathBuf::from(v));
            }
        }
        let tls = match tls {
            None => None,
            Some(ClusterTlsConfig {
                cert_path: Some(cert),
                key_path: Some(key),
                ca_path: Some(ca),
            }) => {
                let read = |name: &str, p: &Path| -> Result<Vec<u8>> {
                    std::fs::read(p).map_err(|e| {
                        anyhow::anyhow!("[cluster.tls] cannot read {name} {}: {e}", p.display())
                    })
                };
                Some(arneb_rpc::ClusterTls {
                    cert_pem: read("cert_path", &cert)?,
                    key_pem: read("key_path", &key)?,
                    ca_pem: read("ca_path", &ca)?,
                })
            }
            Some(_) => bail!(
                "[cluster.tls] needs cert_path, key_path and ca_path (or \
                 ARNEB_CLUSTER_TLS_CERT_PATH / _KEY_PATH / _CA_PATH)"
            ),
        };
        Ok(arneb_rpc::ClusterSecurity { secret, tls })
    }
}

fn default_role() -> String {
    "standalone".to_string()
}

fn default_rpc_port() -> u16 {
    9090
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            role: default_role(),
            coordinator_address: None,
            rpc_port: default_rpc_port(),
            worker_id: None,
            advertised_address: None,
            secret: None,
            tls: None,
            heartbeat_interval_secs: None,
            heartbeat_timeout_secs: None,
            drain_timeout_secs: None,
            retry_policy: None,
            query_retry_attempts: None,
        }
    }
}

/// Parsed server role.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerRole {
    /// Single-node mode: acts as both coordinator and worker.
    Standalone,
    /// Coordinator: accepts client connections, plans queries, schedules tasks.
    Coordinator,
    /// Worker: executes tasks assigned by the coordinator.
    Worker,
}

impl ServerRole {
    pub fn parse(s: &str) -> Result<Self> {
        match s {
            "standalone" => Ok(Self::Standalone),
            "coordinator" => Ok(Self::Coordinator),
            "worker" => Ok(Self::Worker),
            other => bail!(
                "unknown server role: '{other}' (expected: standalone, coordinator, or worker)"
            ),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct TableConfig {
    pub name: String,
    pub path: String,
    pub format: String,
    pub schema: Option<Vec<ColumnSchema>>,
}

#[derive(Debug, Deserialize)]
pub struct ColumnSchema {
    pub name: String,
    #[serde(rename = "type")]
    pub r#type: String,
}

impl AppConfig {
    pub fn load(path: Option<&Path>) -> Result<Self> {
        let config: AppConfig = match path {
            Some(p) => {
                let content = std::fs::read_to_string(p)
                    .map_err(|e| anyhow::anyhow!("failed to read {}: {e}", p.display()))?;
                toml::from_str(&content)
                    .map_err(|e| anyhow::anyhow!("failed to parse {}: {e}", p.display()))?
            }
            None => {
                let default_path = Path::new("./arneb.toml");
                if default_path.exists() {
                    let content = std::fs::read_to_string(default_path)?;
                    toml::from_str(&content)
                        .map_err(|e| anyhow::anyhow!("failed to parse arneb.toml: {e}"))?
                } else {
                    AppConfig {
                        server: ServerConfig::default(),
                        tables: Vec::new(),
                        cluster: ClusterConfig::default(),
                        storage: StorageConfig::default(),
                        catalogs: Vec::new(),
                        memory: MemoryConfig::default(),
                        auth: AuthConfig::default(),
                        trino: TrinoProtocolConfig::default(),
                        tls: None,
                        tls_settings: None,
                        web: WebConfig::default(),
                        admission: AdmissionConfig::default(),
                        agent_api: AgentApiConfig::default(),
                        mcp: McpConfig::default(),
                        observability: ObservabilityConfig::default(),
                        access_control: AccessControlConfig::default(),
                        state: StateConfig::default(),
                    }
                }
            }
        };

        let mut server = config.server;
        server.apply_env_overrides()?;
        server.validate()?;
        let mut trino = config.trino;
        trino.apply_env_overrides()?;
        let tls_settings = TlsConfig::resolve(config.tls.as_ref(), |k| std::env::var(k).ok())?;
        let mut web = config.web;
        web.apply_env_overrides(|k| std::env::var(k).ok())?;
        let mut admission = config.admission;
        admission.apply_env(|k| std::env::var(k).ok())?;
        let mut agent_api = config.agent_api;
        agent_api.apply_env(|k| std::env::var(k).ok())?;
        let mut mcp = config.mcp;
        mcp.apply_env(|k| std::env::var(k).ok())?;
        mcp.validate(agent_api.max_page_bytes)
            .map_err(|e| anyhow::anyhow!("invalid [mcp] settings: {e}"))?;
        let mut observability = config.observability;
        observability.apply_env(|k| std::env::var(k).ok())?;
        let mut cluster = config.cluster;
        cluster.apply_env(|k| std::env::var(k).ok())?;
        let mut access_control = config.access_control;
        access_control.apply_env(|k| std::env::var(k).ok())?;
        let mut state = config.state;
        state.apply_env(|k| std::env::var(k).ok())?;
        let mut catalogs = config.catalogs;
        check_password_env_vars(&catalogs)?;
        for catalog in &mut catalogs {
            catalog.apply_env(|k| std::env::var(k).ok());
        }

        Ok(AppConfig {
            server,
            tables: config.tables,
            cluster,
            storage: config.storage,
            catalogs,
            memory: config.memory,
            auth: config.auth,
            trino,
            tls: config.tls,
            tls_settings,
            web,
            admission,
            agent_api,
            mcp,
            observability,
            access_control,
            state,
        })
    }
}

/// Resolve the adaptive hash-partition policy (`dist-adaptive-partition`):
/// `(target_rows_per_partition, max_partitions)`. Runtime-tunable per the
/// build-time-vs-runtime convention (`docs/guide/configuration.md`):
/// `ARNEB_HASH_PARTITION_TARGET_ROWS` overrides the rows-per-partition target
/// and `ARNEB_MAX_HASH_PARTITIONS` overrides the fan-out cap. Both fall back to
/// the planner defaults. The effective values are logged by the coordinator at
/// fragmentation time.
pub fn resolve_hash_partition_policy() -> (u64, usize) {
    let target_rows = std::env::var("ARNEB_HASH_PARTITION_TARGET_ROWS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(arneb_planner::DEFAULT_HASH_PARTITION_TARGET_ROWS);
    let max_partitions = std::env::var("ARNEB_MAX_HASH_PARTITIONS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n >= 2)
        .unwrap_or(arneb_planner::DEFAULT_MAX_HASH_PARTITIONS);
    (target_rows, max_partitions)
}

pub fn parse_data_type(type_name: &str) -> Result<DataType> {
    match type_name {
        "boolean" => Ok(DataType::Boolean),
        "int8" => Ok(DataType::Int8),
        "int16" => Ok(DataType::Int16),
        "int32" => Ok(DataType::Int32),
        "int64" => Ok(DataType::Int64),
        "float32" => Ok(DataType::Float32),
        "float64" => Ok(DataType::Float64),
        "utf8" => Ok(DataType::Utf8),
        "date32" => Ok(DataType::Date32),
        "timestamp" => Ok(DataType::Timestamp {
            unit: arneb_common::types::TimeUnit::Microsecond,
            timezone: None,
        }),
        other => bail!("unsupported data type: '{other}'"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_bools_share_one_spelling() {
        for t in ["1", "true", "ON", " yes "] {
            assert!(parse_env_bool("ARNEB_X", t).unwrap(), "{t}");
        }
        for f in ["0", "False", "off", "NO"] {
            assert!(!parse_env_bool("ARNEB_X", f).unwrap(), "{f}");
        }
        let err = parse_env_bool("ARNEB_X", "maybe").unwrap_err().to_string();
        assert!(err.contains("ARNEB_X") && err.contains("maybe"), "{err}");
    }

    #[test]
    fn test_app_config_with_tables() {
        let toml_str = r#"
bind_address = "0.0.0.0"
port = 5433

[[tables]]
name = "lineitem"
path = "/data/lineitem.parquet"
format = "parquet"

[[tables]]
name = "orders"
path = "/data/orders.csv"
format = "csv"
schema = [
    { name = "id", type = "int32" },
    { name = "total", type = "float64" },
]
"#;
        let config: AppConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.server.bind_address, "0.0.0.0");
        assert_eq!(config.server.port, 5433);
        assert_eq!(config.tables.len(), 2);

        assert_eq!(config.tables[0].name, "lineitem");
        assert_eq!(config.tables[0].format, "parquet");
        assert!(config.tables[0].schema.is_none());

        assert_eq!(config.tables[1].name, "orders");
        assert_eq!(config.tables[1].format, "csv");
        let schema = config.tables[1].schema.as_ref().unwrap();
        assert_eq!(schema.len(), 2);
        assert_eq!(schema[0].name, "id");
        assert_eq!(schema[0].r#type, "int32");
    }

    #[test]
    fn test_trino_config_defaults_and_overrides() {
        let config: AppConfig = toml::from_str("port = 5432").unwrap();
        assert!(config.trino.enabled);
        assert_eq!(config.trino.port, 8080);

        let config: AppConfig = toml::from_str(
            r#"
port = 5432

[trino]
enabled = false
port = 18080
"#,
        )
        .unwrap();
        assert!(!config.trino.enabled);
        assert_eq!(config.trino.port, 18080);
    }

    fn env_from(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: std::collections::HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |k| map.get(k).cloned()
    }

    #[test]
    fn test_tls_section_omitted_is_off() {
        let config: AppConfig = toml::from_str("port = 5432").unwrap();
        assert!(config.tls.is_none());
        assert_eq!(TlsConfig::resolve(None, env_from(&[])).unwrap(), None);
    }

    #[test]
    fn test_tls_section_defaults_to_required() {
        let config: AppConfig = toml::from_str(
            r#"
[tls]
cert_path = "/c.pem"
key_path = "/k.pem"
"#,
        )
        .unwrap();
        let tls = TlsConfig::resolve(config.tls.as_ref(), env_from(&[]))
            .unwrap()
            .unwrap();
        assert_eq!(tls.cert_path, PathBuf::from("/c.pem"));
        assert_eq!(tls.key_path, PathBuf::from("/k.pem"));
        assert_eq!(tls.pgwire, PgwireTlsMode::Required);
    }

    #[test]
    fn test_tls_env_overrides_file() {
        let config: AppConfig = toml::from_str(
            r#"
[tls]
cert_path = "/c.pem"
key_path = "/k.pem"
pgwire = "required"
"#,
        )
        .unwrap();
        let tls = TlsConfig::resolve(
            config.tls.as_ref(),
            env_from(&[
                ("ARNEB_TLS_CERT_PATH", "/env.pem"),
                ("ARNEB_TLS_PGWIRE", "optional"),
            ]),
        )
        .unwrap()
        .unwrap();
        assert_eq!(tls.cert_path, PathBuf::from("/env.pem"));
        assert_eq!(tls.key_path, PathBuf::from("/k.pem"));
        assert_eq!(tls.pgwire, PgwireTlsMode::Optional);

        // Both path variables enable TLS without a [tls] section.
        let tls = TlsConfig::resolve(
            None,
            env_from(&[("ARNEB_TLS_CERT_PATH", "/c"), ("ARNEB_TLS_KEY_PATH", "/k")]),
        )
        .unwrap();
        assert!(tls.is_some());
    }

    #[test]
    fn test_tls_validation_errors() {
        let only_cert: AppConfig = toml::from_str("[tls]\ncert_path = \"/c.pem\"\n").unwrap();
        let err = TlsConfig::resolve(only_cert.tls.as_ref(), env_from(&[])).unwrap_err();
        assert!(err.to_string().contains("key_path"), "{err}");

        let bad_mode: AppConfig =
            toml::from_str("[tls]\ncert_path = \"/c\"\nkey_path = \"/k\"\npgwire = \"prefer\"\n")
                .unwrap();
        let err = TlsConfig::resolve(bad_mode.tls.as_ref(), env_from(&[])).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("required") && msg.contains("optional"),
            "{msg}"
        );

        assert!(toml::from_str::<AppConfig>("[tls]\ncert = \"/c\"\n").is_err());

        let err =
            TlsConfig::resolve(None, env_from(&[("ARNEB_TLS_PGWIRE", "optional")])).unwrap_err();
        assert!(err.to_string().contains("cert_path"), "{err}");
    }

    #[test]
    fn test_trino_password_auth_fails_closed_without_tls() {
        let err = check_trino_transport(true, true, false)
            .unwrap_err()
            .to_string();
        assert!(err.contains("[tls]") && err.contains("--no-trino"), "{err}");
        check_trino_transport(true, true, true).unwrap();
        check_trino_transport(false, true, false).unwrap();
        check_trino_transport(true, false, false).unwrap();
    }

    #[test]
    fn test_cluster_tls_without_secret_fails_closed_off_loopback() {
        let err = check_cluster_transport(false, true, "0.0.0.0")
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("[cluster] secret") && err.contains("0.0.0.0"),
            "{err}"
        );
        check_cluster_transport(true, true, "0.0.0.0").unwrap();
        check_cluster_transport(false, true, "127.0.0.1").unwrap();
        check_cluster_transport(false, true, "localhost").unwrap();
        check_cluster_transport(false, false, "0.0.0.0").unwrap();
    }

    #[test]
    fn test_resolve_web_bind() {
        assert_eq!(
            resolve_web_bind(AuthType::None, "0.0.0.0", None).unwrap(),
            ("0.0.0.0".to_owned(), WEB_BIND_DEFAULT)
        );
        assert_eq!(
            resolve_web_bind(AuthType::Password, "0.0.0.0", None).unwrap(),
            ("127.0.0.1".to_owned(), WEB_BIND_LOOPBACK)
        );
        for auth in [AuthType::None, AuthType::Password] {
            assert_eq!(
                resolve_web_bind(auth, "127.0.0.1", Some("0.0.0.0")).unwrap(),
                ("0.0.0.0".to_owned(), WEB_BIND_EXPLICIT)
            );
        }
        assert!(resolve_web_bind(AuthType::Password, "0.0.0.0", Some("not an ip")).is_err());
        assert!(is_loopback_host("127.0.0.1") && is_loopback_host("::1"));
        assert!(!is_loopback_host("0.0.0.0"));
    }

    #[test]
    fn test_web_bind_env_overrides_file() {
        let config: AppConfig = toml::from_str("[web]\nbind_address = \"10.0.0.5\"\n").unwrap();
        let mut web = config.web;
        assert_eq!(web.bind_address.as_deref(), Some("10.0.0.5"));
        web.apply_env_overrides(env_from(&[("ARNEB_WEB_BIND_ADDRESS", "0.0.0.0")]))
            .unwrap();
        assert_eq!(web.bind_address.as_deref(), Some("0.0.0.0"));
        assert!(toml::from_str::<AppConfig>("[web]\nport = 1\n").is_err());
    }

    #[test]
    fn test_cluster_secret_from_env_and_validation() {
        let config: AppConfig = toml::from_str("[cluster]\nrole = \"worker\"\n").unwrap();
        let sec = config
            .cluster
            .security(env_from(&[("ARNEB_CLUSTER_SECRET", &"k".repeat(40))]))
            .unwrap();
        assert!(sec.secret.is_some() && sec.tls.is_none());

        let short: AppConfig = toml::from_str("[cluster]\nsecret = \"tooshort\"\n").unwrap();
        let err = short
            .cluster
            .security(env_from(&[]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("32"), "{err}");
        assert!(!format!("{:?}", short.cluster).contains("tooshort"));

        let none: AppConfig = toml::from_str("port = 5432").unwrap();
        let sec = none.cluster.security(env_from(&[])).unwrap();
        assert!(sec.secret.is_none() && sec.tls.is_none());
    }

    #[test]
    fn test_cluster_tls_requires_all_paths() {
        let config: AppConfig =
            toml::from_str("[cluster.tls]\ncert_path = \"/nope.crt\"\n").unwrap();
        let err = config
            .cluster
            .security(env_from(&[]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("key_path") && err.contains("ca_path"), "{err}");
        assert!(toml::from_str::<AppConfig>("[cluster.tls]\ncert = \"/x\"\n").is_err());
    }

    /// `[admission]` with env overrides applied the way `load` does.
    fn admission(toml_text: &str, env: &[(&str, &str)]) -> Result<AdmissionConfig> {
        let mut c = toml::from_str::<AppConfig>(toml_text)?.admission;
        c.apply_env(env_from(env))?;
        Ok(c)
    }

    /// `[agent_api]` with env overrides applied the way `load` does.
    fn agent_api(toml_text: &str, env: &[(&str, &str)]) -> Result<AgentApiConfig> {
        let mut c = toml::from_str::<AppConfig>(toml_text)?.agent_api;
        c.apply_env(env_from(env))?;
        Ok(c)
    }

    #[test]
    fn agent_api_defaults_match_documented_values() {
        let c = agent_api("port = 5432", &[]).unwrap();
        assert_eq!(
            toml::from_str::<AppConfig>("port = 5432")
                .unwrap()
                .agent_api
                .limits(),
            arneb_protocol::agent::AgentLimits::default()
        );
        assert_eq!(c.limits().max_rows, 10_000);
        assert_eq!(c.limits().max_wait, std::time::Duration::from_secs(10));
    }

    #[test]
    fn agent_api_env_overrides_file() {
        let file = "[agent_api]\nmax_rows = 500\ndefault_max_rows = 100\n";
        assert_eq!(agent_api(file, &[]).unwrap().max_rows, 500);
        let c = agent_api(file, &[("ARNEB_AGENT_MAX_ROWS", "700")]).unwrap();
        assert_eq!(c.max_rows, 700);
    }

    #[test]
    fn agent_api_rejects_default_above_max() {
        let err = agent_api(
            "[agent_api]\ndefault_max_rows = 2000\nmax_rows = 1000\n",
            &[],
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("default_max_rows"), "{err}");
    }

    #[test]
    fn agent_api_retained_byte_limits_from_file_and_env() {
        let c = agent_api("port = 5432", &[]).unwrap();
        assert_eq!(
            (c.max_retained_bytes, c.max_total_retained_bytes),
            (64 << 20, 1 << 30)
        );
        let file = "[agent_api]\nmax_retained_bytes = 1000\nmax_total_retained_bytes = 5000\n";
        let c = agent_api(file, &[("ARNEB_AGENT_MAX_TOTAL_RETAINED_BYTES", "9000")]).unwrap();
        assert_eq!(
            (c.max_retained_bytes, c.max_total_retained_bytes),
            (1000, 9000)
        );
        let err = agent_api(file, &[("ARNEB_AGENT_MAX_RETAINED_BYTES", "6000")])
            .unwrap_err()
            .to_string();
        assert!(err.contains("max_total_retained_bytes"), "{err}");
        let err = agent_api(
            "[agent_api]\nmax_wait_ms = 10000\nidle_timeout_secs = 10\n",
            &[],
        )
        .unwrap_err()
        .to_string();
        assert!(
            err.contains("idle_timeout_secs") && err.contains("max_wait_ms"),
            "{err}"
        );
    }

    #[test]
    fn agent_api_rejects_unparsable_env() {
        let err = agent_api("port = 5432", &[("ARNEB_AGENT_MAX_WAIT_MS", "abc")])
            .unwrap_err()
            .to_string();
        assert!(err.contains("ARNEB_AGENT_MAX_WAIT_MS"), "{err}");
    }

    /// `[mcp]` with env overrides applied the way `load` does.
    fn mcp(toml_text: &str, env: &[(&str, &str)]) -> Result<McpConfig> {
        let config = toml::from_str::<AppConfig>(toml_text)?;
        let mut c = config.mcp;
        c.apply_env(env_from(env))?;
        c.validate(config.agent_api.max_page_bytes)
            .map_err(|e| anyhow::anyhow!(e))?;
        Ok(c)
    }

    #[test]
    fn mcp_defaults() {
        let c = mcp("port = 5432", &[]).unwrap();
        assert_eq!(
            c,
            McpConfig {
                enabled: true,
                allowed_hosts: None,
                allowed_origins: None,
                default_page_bytes: 32768
            }
        );
        let c = mcp("[mcp]\nenabled = false\nallowed_hosts = [\"a\"]\n", &[]).unwrap();
        assert!(!c.enabled);
        assert_eq!(c.allowed_hosts, Some(vec!["a".to_owned()]));
    }

    #[test]
    fn mcp_env_overrides_file() {
        let file = "[mcp]\nenabled = true\nallowed_hosts = [\"file.example\"]\ndefault_page_bytes = 1000\n";
        let c = mcp(
            file,
            &[
                ("ARNEB_MCP_ENABLED", "false"),
                ("ARNEB_MCP_ALLOWED_HOSTS", "a.example, b.example:8443"),
                ("ARNEB_MCP_DEFAULT_PAGE_BYTES", "4096"),
            ],
        )
        .unwrap();
        assert!(!c.enabled);
        assert_eq!(
            c.allowed_hosts,
            Some(vec!["a.example".to_owned(), "b.example:8443".to_owned()])
        );
        assert_eq!(c.default_page_bytes, 4096);
        let c = mcp(file, &[("ARNEB_MCP_ALLOWED_HOSTS", "*")]).unwrap();
        assert_eq!(c.allowed_hosts, Some(vec!["*".to_owned()]));
        assert!(mcp(file, &[("ARNEB_MCP_ENABLED", "maybe")]).is_err());
    }

    #[test]
    fn mcp_refuses_empty_host_lists_and_treats_empty_env_as_unset() {
        let err = mcp("[mcp]\nallowed_hosts = []\n", &[])
            .unwrap_err()
            .to_string();
        assert!(err.contains("allowed_hosts is empty"), "{err}");
        let err = mcp("port = 5432", &[("ARNEB_MCP_ALLOWED_HOSTS", " , ")])
            .unwrap_err()
            .to_string();
        assert!(err.contains("ARNEB_MCP_ALLOWED_HOSTS"), "{err}");
        // Empty env = unset: the file value (or default) stays.
        let file = "[mcp]\nallowed_hosts = [\"file.example\"]\n";
        let c = mcp(file, &[("ARNEB_MCP_ALLOWED_HOSTS", "")]).unwrap();
        assert_eq!(c.allowed_hosts, Some(vec!["file.example".to_owned()]));
        let c = mcp("port = 5432", &[("ARNEB_MCP_ALLOWED_HOSTS", "  ")]).unwrap();
        assert_eq!(c.allowed_hosts, None);
    }

    #[test]
    fn mcp_host_lists_drop_blank_entries() {
        let c = mcp("[mcp]\nallowed_hosts = [\"\", \" a.example \"]\n", &[]).unwrap();
        assert_eq!(c.allowed_hosts, Some(vec!["a.example".to_owned()]));
        let err = mcp("[mcp]\nallowed_hosts = [\" \"]\n", &[])
            .unwrap_err()
            .to_string();
        assert!(err.contains("allowed_hosts is empty"), "{err}");
    }

    #[test]
    fn mcp_allowed_origins_file_env_and_validation() {
        assert_eq!(mcp("port = 5432", &[]).unwrap().allowed_origins, None);
        let file = "[mcp]\nallowed_origins = [\"https://app.example:443\", \"null\", \"\"]\n";
        let c = mcp(file, &[]).unwrap();
        assert_eq!(
            c.allowed_origins,
            Some(vec![
                "https://app.example:443".to_owned(),
                "null".to_owned()
            ])
        );
        let c = mcp(
            file,
            &[(
                "ARNEB_MCP_ALLOWED_ORIGINS",
                "https://a.example, http://localhost:*",
            )],
        )
        .unwrap();
        assert_eq!(
            c.allowed_origins,
            Some(vec![
                "https://a.example".to_owned(),
                "http://localhost:*".to_owned()
            ])
        );
        // Empty env = unset.
        let c = mcp(file, &[("ARNEB_MCP_ALLOWED_ORIGINS", " ")]).unwrap();
        assert_eq!(c.allowed_origins.unwrap().len(), 2);
        let err = mcp("port = 5432", &[("ARNEB_MCP_ALLOWED_ORIGINS", " , ")])
            .unwrap_err()
            .to_string();
        assert!(err.contains("ARNEB_MCP_ALLOWED_ORIGINS"), "{err}");
        for bad in ["[]", "[\" \"]"] {
            let err = mcp(&format!("[mcp]\nallowed_origins = {bad}\n"), &[])
                .unwrap_err()
                .to_string();
            assert!(err.contains("allowed_origins is empty"), "{bad}: {err}");
        }
        let err = mcp("[mcp]\nallowed_origins = [\"app.example\"]\n", &[])
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("app.example") && err.contains("scheme"),
            "{err}"
        );
        let err = mcp(
            "port = 5432",
            &[("ARNEB_MCP_ALLOWED_ORIGINS", "app.example")],
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("scheme"), "{err}");
    }

    #[test]
    fn mcp_validation_and_unknown_keys() {
        let err = mcp("[mcp]\ndefault_page_bytes = 0\n", &[])
            .unwrap_err()
            .to_string();
        assert!(err.contains("default_page_bytes"), "{err}");
        let err = mcp(
            "[agent_api]\nmax_page_bytes = 100000\ndefault_page_bytes = 1000\n[mcp]\ndefault_page_bytes = 200000\n",
            &[],
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("max_page_bytes"), "{err}");
        assert!(toml::from_str::<AppConfig>("[mcp]\nbogus = 1\n").is_err());
    }

    #[test]
    fn test_admission_defaults_file_and_env() {
        let none = admission("port = 5432", &[]).unwrap();
        assert_eq!(none, AdmissionConfig::default());
        assert_eq!(
            (none.max_running_queries, none.max_queued_queries),
            (8, 256)
        );
        assert_eq!(none.statement_timeout, None);

        let file = "[admission]\nmax_running_queries = 2\nmax_queued_queries = 5\nstatement_timeout = \"30s\"\n";
        let s = admission(file, &[]).unwrap();
        assert_eq!(s.max_running_queries, 2);
        assert_eq!(s.max_queued_queries, 5);
        assert_eq!(
            s.statement_timeout,
            Some(std::time::Duration::from_secs(30))
        );

        let s = admission(
            file,
            &[
                ("ARNEB_MAX_RUNNING_QUERIES", "0"),
                ("ARNEB_STATEMENT_TIMEOUT", "2min"),
            ],
        )
        .unwrap();
        assert_eq!(s.max_running_queries, 0);
        assert_eq!(s.max_queued_queries, 5);
        assert_eq!(
            s.statement_timeout,
            Some(std::time::Duration::from_secs(120))
        );

        let ms = admission("[admission]\nstatement_timeout = 1500\n", &[]).unwrap();
        assert_eq!(
            ms.statement_timeout,
            Some(std::time::Duration::from_millis(1500))
        );
        // Only the named knob moves; the others keep their defaults.
        assert_eq!(ms.max_running_queries, 8);
    }

    #[test]
    fn test_observability_defaults_file_and_env() {
        let load = |toml_text: &str, env: &[(&str, &str)]| {
            let mut c = toml::from_str::<AppConfig>(toml_text)
                .unwrap()
                .observability;
            let env: Vec<(String, String)> = env
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            c.apply_env(|k| env.iter().find(|(n, _)| n == k).map(|(_, v)| v.clone()))
                .map(|()| c)
        };
        let none = load("port = 5432", &[]).unwrap();
        assert_eq!(
            none.slow_query_threshold(),
            Some(std::time::Duration::from_secs(10))
        );
        let file = "[observability]\nslow_query_threshold_ms = 2500\n";
        assert_eq!(load(file, &[]).unwrap().slow_query_threshold_ms, 2500);
        let off = load(file, &[("ARNEB_SLOW_QUERY_THRESHOLD_MS", "0")]).unwrap();
        assert_eq!(off.slow_query_threshold(), None);
        let err = load(file, &[("ARNEB_SLOW_QUERY_THRESHOLD_MS", "soon")]).unwrap_err();
        assert!(
            err.to_string().contains("ARNEB_SLOW_QUERY_THRESHOLD_MS"),
            "{err}"
        );

        assert_eq!(none.query_history_size, 100);
        let history = "[observability]\nquery_history_size = 500\n";
        assert_eq!(load(history, &[]).unwrap().query_history_size, 500);
        let env = load(history, &[("ARNEB_QUERY_HISTORY_SIZE", "7")]).unwrap();
        assert_eq!(env.query_history_size, 7);
        for zero in [
            load("[observability]\nquery_history_size = 0\n", &[]),
            load(history, &[("ARNEB_QUERY_HISTORY_SIZE", "0")]),
        ] {
            assert!(zero.unwrap_err().to_string().contains("must be positive"));
        }
    }

    /// #150 review: a bad file `statement_timeout` is not rejected at parse
    /// time, so a valid `ARNEB_STATEMENT_TIMEOUT` still overrides it.
    #[test]
    fn test_admission_env_overrides_bad_file_timeout() {
        let bad_file = "[admission]\nstatement_timeout = \"soon\"\n";
        assert!(toml::from_str::<AppConfig>(bad_file).is_ok());
        let s = admission(bad_file, &[("ARNEB_STATEMENT_TIMEOUT", "45s")]).unwrap();
        assert_eq!(
            s.statement_timeout,
            Some(std::time::Duration::from_secs(45))
        );
        let off = admission(bad_file, &[("ARNEB_STATEMENT_TIMEOUT", "0")]).unwrap();
        assert_eq!(off.statement_timeout, None);
        assert!(admission(bad_file, &[]).is_err());
    }

    #[test]
    fn test_admission_validation() {
        assert!(admission("port = 5432", &[("ARNEB_MAX_RUNNING_QUERIES", "-1")]).is_err());
        assert!(admission("port = 5432", &[("ARNEB_STATEMENT_TIMEOUT", "soon")]).is_err());
        let err = admission("[admission]\nstatement_timeout = \"soon\"\n", &[])
            .unwrap_err()
            .to_string();
        assert!(err.contains("[admission] statement_timeout"), "{err}");
        let err = admission(
            "[admission]\nstatement_timeout = \"30s\"\n",
            &[("ARNEB_STATEMENT_TIMEOUT", "soon")],
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("ARNEB_STATEMENT_TIMEOUT"), "{err}");
        assert!(toml::from_str::<AppConfig>("[admission]\nmax_running = 1\n").is_err());
    }

    #[test]
    fn test_app_config_no_tables() {
        let toml_str = r#"
bind_address = "127.0.0.1"
port = 5432
"#;
        let config: AppConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.server.port, 5432);
        assert!(config.tables.is_empty());
    }

    #[test]
    fn test_app_config_with_storage() {
        let toml_str = r#"
bind_address = "0.0.0.0"
port = 5432

[storage.s3]
region = "us-east-1"
endpoint = "http://localhost:9000"
allow_http = true

[storage.gcs]
service_account_path = "/path/to/sa.json"

[[tables]]
name = "remote"
path = "s3://my-bucket/data/events.parquet"
format = "parquet"
"#;
        let config: AppConfig = toml::from_str(toml_str).unwrap();
        let s3 = config.storage.s3.unwrap();
        assert_eq!(s3.region.as_deref(), Some("us-east-1"));
        assert_eq!(s3.endpoint.as_deref(), Some("http://localhost:9000"));
        assert!(s3.allow_http);

        let gcs = config.storage.gcs.unwrap();
        assert_eq!(
            gcs.service_account_path.as_deref(),
            Some("/path/to/sa.json")
        );

        assert!(config.storage.azure.is_none());

        assert_eq!(config.tables[0].path, "s3://my-bucket/data/events.parquet");
    }

    #[test]
    fn test_app_config_with_hive_catalog() {
        let toml_str = r#"
bind_address = "0.0.0.0"
port = 5432

[[catalogs]]
name = "datalake"
type = "hive"
metastore_uri = "thrift://hms.internal:9083"
default_schema = "analytics"

[catalogs.storage]
[catalogs.storage.s3]
region = "us-west-2"
"#;
        let config: AppConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.catalogs.len(), 1);
        let cat = &config.catalogs[0];
        assert_eq!(cat.name, "datalake");
        assert_eq!(cat.catalog_type, "hive");
        assert_eq!(
            cat.metastore_uri.as_deref(),
            Some("thrift://hms.internal:9083")
        );
        assert_eq!(cat.effective_default_schema(), "analytics");
        let s3 = cat.storage.as_ref().unwrap().s3.as_ref().unwrap();
        assert_eq!(s3.region.as_deref(), Some("us-west-2"));
    }

    #[test]
    fn test_app_config_with_postgresql_catalog() {
        let toml_str = r#"
[[catalogs]]
name = "pg-prod"
type = "postgresql"
connection_url = "postgresql://db.internal:5432/sales"
user = "arneb"
password = "file-secret"
max_splits = 4
domain_compaction_threshold = 250

[[catalogs]]
name = "lake"
type = "hive"
metastore_uri = "127.0.0.1:9083"
"#;
        let config: AppConfig = toml::from_str(toml_str).unwrap();
        let mut pg = config.catalogs[0].clone();
        assert_eq!(pg.catalog_type, "postgresql");
        assert_eq!(pg.password_env_var(), "ARNEB_CATALOG_PG_PROD_PASSWORD");
        assert!(!format!("{pg:?}").contains("file-secret"));

        let c = pg.postgres_config().unwrap();
        assert_eq!(c.password.as_deref(), Some("file-secret"));
        assert_eq!(c.max_splits, 4);
        assert_eq!(c.domain_compaction_threshold, 250);
        assert_eq!(c.max_connections, arneb_postgresql::DEFAULT_MAX_CONNECTIONS);
        assert_eq!(c.split_min_rows, arneb_postgresql::DEFAULT_SPLIT_MIN_ROWS);
        assert_eq!(pg.effective_default_schema(), "public");
        assert_eq!(config.catalogs[1].effective_default_schema(), "default");

        pg.apply_env(env_from(&[(
            "ARNEB_CATALOG_PG_PROD_PASSWORD",
            "env-secret",
        )]));
        assert_eq!(
            pg.postgres_config().unwrap().password.as_deref(),
            Some("env-secret")
        );
        // Hive entries ignore the postgresql fields.
        assert!(config.catalogs[1].connection_url.is_none());
    }

    #[test]
    fn test_postgresql_catalog_validation() {
        let toml_str = r#"
[[catalogs]]
name = "pg"
type = "postgresql"
"#;
        let config: AppConfig = toml::from_str(toml_str).unwrap();
        let err = config.catalogs[0]
            .postgres_config()
            .unwrap_err()
            .to_string();
        assert!(err.contains("connection_url"), "{err}");

        let toml_str = r#"
[[catalogs]]
name = "pg"
type = "postgresql"
connection_url = "postgresql://h/d"
max_splits = 0
"#;
        let config: AppConfig = toml::from_str(toml_str).unwrap();
        assert!(config.catalogs[0].postgres_config().is_err());

        let toml_str = r#"
[[catalogs]]
name = "pg"
type = "postgresql"
connection_url = "postgresql://h/d"
domain_compaction_threshold = 0
"#;
        let config: AppConfig = toml::from_str(toml_str).unwrap();
        let err = config.catalogs[0]
            .postgres_config()
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("'pg'") && err.contains("domain_compaction_threshold"),
            "{err}"
        );
    }

    #[test]
    fn test_postgresql_password_env_var_collision_is_refused() {
        let toml_str = r#"
[[catalogs]]
name = "pg-prod"
type = "postgresql"
connection_url = "postgresql://h/a"

[[catalogs]]
name = "pg_prod"
type = "postgresql"
connection_url = "postgresql://h/b"
"#;
        let config: AppConfig = toml::from_str(toml_str).unwrap();
        let err = check_password_env_vars(&config.catalogs)
            .unwrap_err()
            .to_string();
        assert!(err.contains("ARNEB_CATALOG_PG_PROD_PASSWORD"), "{err}");

        let config: AppConfig = toml::from_str(&toml_str.replace("pg_prod", "pg_dev")).unwrap();
        assert!(check_password_env_vars(&config.catalogs).is_ok());
    }

    #[test]
    fn test_app_config_multiple_catalogs() {
        let toml_str = r#"
bind_address = "0.0.0.0"
port = 5432

[[catalogs]]
name = "prod"
type = "hive"
metastore_uri = "thrift://hms-prod:9083"

[[catalogs]]
name = "staging"
type = "hive"
metastore_uri = "thrift://hms-staging:9083"
"#;
        let config: AppConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.catalogs.len(), 2);
        assert_eq!(config.catalogs[0].name, "prod");
        assert_eq!(config.catalogs[1].name, "staging");
    }

    #[test]
    fn test_app_config_no_catalogs() {
        let toml_str = r#"
bind_address = "0.0.0.0"
port = 5432
"#;
        let config: AppConfig = toml::from_str(toml_str).unwrap();
        assert!(config.catalogs.is_empty());
    }

    #[test]
    fn test_parse_data_type_all_supported() {
        assert!(matches!(
            parse_data_type("boolean").unwrap(),
            DataType::Boolean
        ));
        assert!(matches!(parse_data_type("int8").unwrap(), DataType::Int8));
        assert!(matches!(parse_data_type("int16").unwrap(), DataType::Int16));
        assert!(matches!(parse_data_type("int32").unwrap(), DataType::Int32));
        assert!(matches!(parse_data_type("int64").unwrap(), DataType::Int64));
        assert!(matches!(
            parse_data_type("float32").unwrap(),
            DataType::Float32
        ));
        assert!(matches!(
            parse_data_type("float64").unwrap(),
            DataType::Float64
        ));
        assert!(matches!(parse_data_type("utf8").unwrap(), DataType::Utf8));
        assert!(matches!(
            parse_data_type("date32").unwrap(),
            DataType::Date32
        ));
        assert!(matches!(
            parse_data_type("timestamp").unwrap(),
            DataType::Timestamp { .. }
        ));
    }

    #[test]
    fn test_parse_data_type_unknown() {
        assert!(parse_data_type("unknown_type").is_err());
    }

    #[test]
    fn test_app_config_flatten_with_tables() {
        let toml_str = r#"
bind_address = "10.0.0.1"
port = 9999
max_worker_threads = 4
max_memory_mb = 2048

[[tables]]
name = "test"
path = "/tmp/test.parquet"
format = "parquet"
"#;
        let config: AppConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.server.bind_address, "10.0.0.1");
        assert_eq!(config.server.port, 9999);
        assert_eq!(config.server.max_worker_threads, 4);
        assert_eq!(config.server.max_memory_mb, 2048);
        assert_eq!(config.tables.len(), 1);
    }

    #[test]
    fn state_path_from_file_env_and_default() {
        let config: AppConfig = toml::from_str("port = 5432\n").unwrap();
        assert_eq!(config.state.path, None);

        let mut state = toml::from_str::<AppConfig>("[state]\npath = \"/var/arneb/state.json\"\n")
            .unwrap()
            .state;
        assert_eq!(state.path, Some(PathBuf::from("/var/arneb/state.json")));
        state.apply_env(env_from(&[])).unwrap();
        assert_eq!(state.path, Some(PathBuf::from("/var/arneb/state.json")));
        state
            .apply_env(env_from(&[("ARNEB_STATE_PATH", "/data/s.json")]))
            .unwrap();
        assert_eq!(state.path, Some(PathBuf::from("/data/s.json")));
        assert!(state
            .apply_env(env_from(&[("ARNEB_STATE_PATH", " ")]))
            .is_err());
    }

    #[test]
    fn metrics_auth_defaults_off_and_env_overrides() {
        let mut web = toml::from_str::<AppConfig>("port = 5432\n").unwrap().web;
        assert!(!web.metrics_auth);
        web.apply_env_overrides(env_from(&[("ARNEB_WEB_METRICS_AUTH", "true")]))
            .unwrap();
        assert!(web.metrics_auth);
        web.apply_env_overrides(env_from(&[("ARNEB_WEB_METRICS_AUTH", "off")]))
            .unwrap();
        assert!(!web.metrics_auth);
        assert!(web
            .apply_env_overrides(env_from(&[("ARNEB_WEB_METRICS_AUTH", "maybe")]))
            .is_err());
        let from_file = toml::from_str::<AppConfig>("[web]\nmetrics_auth = true\n").unwrap();
        assert!(from_file.web.metrics_auth);
    }

    #[test]
    fn user_verifiers_parse_the_configured_hashes() {
        let a = ScramVerifier::generate("a").unwrap().to_string();
        let b = ScramVerifier::generate("b").unwrap().to_string();
        let config: AppConfig = toml::from_str(&format!(
            "[auth]\ntype = \"password\"\n\
             [[auth.users]]\nname = \"alice\"\npassword_hash = \"{a}\"\n\
             [[auth.users]]\nname = \"bob\"\npassword_hash = \"{b}\"\n"
        ))
        .unwrap();
        let v = config.auth.user_verifiers().unwrap();
        assert_eq!(v.len(), 2);
        assert_eq!(v[0].0, "alice");
        // The parsed verifier round-trips to the configured text.
        assert_eq!(v[0].1.to_string(), a);
        assert_eq!(v[1].1.to_string(), b);

        let dup = AuthConfig {
            users: vec![
                AuthUserConfig {
                    name: "x".into(),
                    password_hash: a.clone(),
                },
                AuthUserConfig {
                    name: "x".into(),
                    password_hash: b.clone(),
                },
            ],
            ..AuthConfig::default()
        };
        assert!(dup.user_verifiers().unwrap_err().to_string().contains("x"));
        let empty = AuthConfig {
            users: vec![AuthUserConfig {
                name: String::new(),
                password_hash: a,
            }],
            ..AuthConfig::default()
        };
        assert!(empty.user_verifiers().is_err());
        let bad = AuthConfig {
            users: vec![AuthUserConfig {
                name: "z".into(),
                password_hash: "md5x".into(),
            }],
            ..AuthConfig::default()
        };
        assert!(bad.user_verifiers().unwrap_err().to_string().contains("z"));
    }

    #[test]
    fn test_auth_defaults_to_none() {
        let config: AppConfig = toml::from_str("port = 5432\n").unwrap();
        assert_eq!(config.auth.auth_type, AuthType::None);
        assert!(matches!(
            config.auth.to_auth_method().unwrap().0,
            AuthMethod::None
        ));
    }

    #[test]
    fn test_auth_password_section() {
        let hash = ScramVerifier::generate("pw").unwrap().to_string();
        let toml_str = format!(
            r#"
port = 5432

[auth]
type = "password"

[[auth.users]]
name = "alice"
password_hash = "{hash}"

[[auth.users]]
name = "bob"
password_hash = "{hash}"
"#
        );
        let config: AppConfig = toml::from_str(&toml_str).unwrap();
        assert_eq!(config.auth.auth_type, AuthType::Password);
        assert_eq!(config.auth.users.len(), 2);
        match config.auth.to_auth_method().unwrap().0 {
            AuthMethod::ScramSha256(creds) => assert_eq!(creds.user_count(), 2),
            other => panic!("expected password auth, got {other:?}"),
        }
        // The Debug output must never contain the stored verifier.
        assert!(!format!("{:?}", config.auth).contains(&hash));
    }

    #[test]
    fn to_auth_method_validates_like_user_verifiers() {
        let a = ScramVerifier::generate("a").unwrap().to_string();
        let config: AppConfig = toml::from_str(&format!(
            "[auth]\ntype = \"password\"\n\
             [[auth.users]]\nname = \"\"\npassword_hash = \"{a}\"\n"
        ))
        .unwrap();
        let err = config.auth.to_auth_method().unwrap_err().to_string();
        assert!(err.contains("must not be empty"), "{err}");
        let config: AppConfig = toml::from_str(&format!(
            "[auth]\ntype = \"password\"\n\
             [[auth.users]]\nname = \"alice\"\npassword_hash = \"{a}\"\n"
        ))
        .unwrap();
        let (_, users) = config.auth.to_auth_method().unwrap();
        assert_eq!(users.len(), 1);
        assert_eq!(users[0].1.to_string(), a);
    }

    #[test]
    fn test_auth_password_requires_users() {
        let config: AppConfig = toml::from_str("[auth]\ntype = \"password\"\n").unwrap();
        let err = config.auth.to_auth_method().unwrap_err().to_string();
        assert!(err.contains("at least one user"), "{err}");
    }

    #[test]
    fn test_auth_rejects_bad_hash_and_unknown_fields() {
        let config: AppConfig = toml::from_str(
            r#"
[auth]
type = "password"
[[auth.users]]
name = "alice"
password_hash = "md5deadbeef"
"#,
        )
        .unwrap();
        let err = config.auth.to_auth_method().unwrap_err().to_string();
        assert!(err.contains("alice"), "{err}");

        // A plaintext `password` key is a misuse and must not parse.
        let res: std::result::Result<AppConfig, _> = toml::from_str(
            r#"
[auth]
type = "password"
[[auth.users]]
name = "alice"
password = "hunter2"
"#,
        );
        assert!(res.is_err());

        let res: std::result::Result<AppConfig, _> = toml::from_str("[auth]\ntype = \"md5\"\n");
        assert!(res.is_err());
    }

    #[test]
    fn heartbeat_settings_defaults_overrides_and_validation() {
        let cluster = ClusterConfig::default();
        let h = cluster.heartbeat(env_from(&[])).unwrap();
        assert_eq!((h.interval_secs, h.timeout_secs), (2, 10));

        let cluster: ClusterConfig =
            toml::from_str("heartbeat_interval_secs = 3\nheartbeat_timeout_secs = 15").unwrap();
        let h = cluster.heartbeat(env_from(&[])).unwrap();
        assert_eq!((h.interval_secs, h.timeout_secs), (3, 15));
        let h = cluster
            .heartbeat(env_from(&[("ARNEB_HEARTBEAT_TIMEOUT_SECS", "20")]))
            .unwrap();
        assert_eq!((h.interval_secs, h.timeout_secs), (3, 20));

        let cluster: ClusterConfig =
            toml::from_str("heartbeat_interval_secs = 2\nheartbeat_timeout_secs = 2").unwrap();
        let err = cluster.heartbeat(env_from(&[])).unwrap_err().to_string();
        assert!(
            err.contains("heartbeat_timeout_secs") && err.contains("heartbeat_interval_secs"),
            "{err}"
        );
        assert!(ClusterConfig::default()
            .heartbeat(env_from(&[("ARNEB_HEARTBEAT_INTERVAL_SECS", "0")]))
            .is_err());
        assert!(ClusterConfig::default()
            .heartbeat(env_from(&[("ARNEB_HEARTBEAT_INTERVAL_SECS", "x")]))
            .is_err());
    }

    #[test]
    fn query_retry_defaults_overrides_and_validation() {
        let read = |toml_src: &str, env: &[(&str, &str)]| {
            let cluster: ClusterConfig = toml::from_str(toml_src).unwrap();
            cluster
                .query_retry(env_from(env))
                .map(|r| r.map(|r| r.max_retries))
        };
        assert_eq!(read("", &[]).unwrap(), None);
        assert_eq!(read("retry_policy = \"none\"", &[]).unwrap(), None);
        assert_eq!(read("retry_policy = \"QUERY\"", &[]).unwrap(), Some(4));
        assert_eq!(
            read("retry_policy = \"query\"\nquery_retry_attempts = 2", &[]).unwrap(),
            Some(2)
        );
        assert_eq!(
            read(
                "query_retry_attempts = 2",
                &[
                    ("ARNEB_RETRY_POLICY", "query"),
                    ("ARNEB_QUERY_RETRY_ATTEMPTS", "1")
                ]
            )
            .unwrap(),
            Some(1)
        );
        assert_eq!(
            read(
                "retry_policy = \"query\"",
                &[("ARNEB_RETRY_POLICY", "none")]
            )
            .unwrap(),
            None
        );
        let err = read("retry_policy = \"task\"", &[])
            .unwrap_err()
            .to_string();
        assert!(err.contains("not supported"), "{err}");
        assert!(read("retry_policy = \"always\"", &[]).is_err());
        assert!(read("retry_policy = \"query\"\nquery_retry_attempts = 0", &[]).is_err());
        assert!(read(
            "",
            &[
                ("ARNEB_RETRY_POLICY", "query"),
                ("ARNEB_QUERY_RETRY_ATTEMPTS", "x")
            ]
        )
        .is_err());
    }

    #[test]
    fn drain_timeout_defaults_overrides_and_validation() {
        let read = |toml_src: &str, env: &[(&str, &str)]| {
            let cluster: ClusterConfig = toml::from_str(toml_src).unwrap();
            cluster.drain_timeout_secs(env_from(env))
        };
        assert_eq!(read("", &[]).unwrap(), 120);
        assert_eq!(read("drain_timeout_secs = 30", &[]).unwrap(), 30);
        assert_eq!(
            read(
                "drain_timeout_secs = 30",
                &[("ARNEB_DRAIN_TIMEOUT_SECS", "45")]
            )
            .unwrap(),
            45
        );
        let err = read("drain_timeout_secs = 0", &[]).unwrap_err().to_string();
        assert!(err.contains("drain_timeout_secs"), "{err}");
        assert!(read("", &[("ARNEB_DRAIN_TIMEOUT_SECS", "soon")]).is_err());
    }

    #[test]
    fn cluster_addresses_and_worker_id_from_env() {
        let file = r#"
            coordinator_address = "coord:9090"
            worker_id = "a"
        "#;
        let mut cluster: ClusterConfig = toml::from_str(file).unwrap();
        cluster.apply_env(env_from(&[])).unwrap();
        assert_eq!(cluster.worker_id.as_deref(), Some("a"));
        assert_eq!(cluster.advertised_address, None);

        cluster
            .apply_env(env_from(&[
                ("ARNEB_CLUSTER_WORKER_ID", "b"),
                ("ARNEB_CLUSTER_ADVERTISED_ADDRESS", " 10.0.0.7:9090 "),
                ("ARNEB_CLUSTER_COORDINATOR_ADDRESS", "arneb:9090"),
            ]))
            .unwrap();
        assert_eq!(cluster.worker_id.as_deref(), Some("b"));
        assert_eq!(cluster.advertised_address.as_deref(), Some("10.0.0.7:9090"));
        assert_eq!(cluster.coordinator_address.as_deref(), Some("arneb:9090"));

        let err = cluster
            .apply_env(env_from(&[("ARNEB_CLUSTER_WORKER_ID", "")]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("ARNEB_CLUSTER_WORKER_ID"), "{err}");
    }

    #[test]
    fn access_control_rules_file_from_file_or_env() {
        let dir = std::env::temp_dir().join(format!("arneb-rules-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let good = dir.join("good.json");
        std::fs::write(
            &good,
            r#"{"tables": [{"user": "a", "privileges": ["SELECT"]}]}"#,
        )
        .unwrap();
        let bad = dir.join("bad.json");
        std::fs::write(&bad, r#"{"tables": [{"filter": "x", "privileges": []}]}"#).unwrap();

        let mut cfg: AccessControlConfig = toml::from_str("").unwrap();
        cfg.apply_env(env_from(&[])).unwrap();
        assert!(cfg.load_rules().unwrap().is_none());

        let mut cfg: AccessControlConfig =
            toml::from_str(&format!("rules_file = {:?}", bad.display().to_string())).unwrap();
        let err = cfg.load_rules().unwrap_err().to_string();
        assert!(err.contains("bad.json") && err.contains("filter"), "{err}");
        cfg.apply_env(env_from(&[(
            "ARNEB_ACCESS_CONTROL_RULES_FILE",
            good.to_str().unwrap(),
        )]))
        .unwrap();
        let rules = cfg.load_rules().unwrap().unwrap();
        assert_eq!(rules.section_sizes(), [None, None, Some(1), None, None]);

        cfg.rules_file = Some(dir.join("missing.json"));
        assert!(cfg
            .load_rules()
            .unwrap_err()
            .to_string()
            .contains("missing.json"));
        assert!(cfg
            .apply_env(env_from(&[("ARNEB_ACCESS_CONTROL_RULES_FILE", "")]))
            .is_err());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn admin_users_parse_and_env_override() {
        let mut cfg: AccessControlConfig =
            toml::from_str(r#"admin_users = ["root", "ops"]"#).unwrap();
        assert_eq!(cfg.admin_users, ["root", "ops"]);
        cfg.apply_env(env_from(&[("ARNEB_ADMIN_USERS", "")]))
            .unwrap();
        assert_eq!(cfg.admin_users, ["root", "ops"], "empty counts as unset");
        cfg.apply_env(env_from(&[("ARNEB_ADMIN_USERS", " , ,")]))
            .unwrap();
        assert_eq!(
            cfg.admin_users,
            ["root", "ops"],
            "all blanks counts as unset"
        );
        cfg.apply_env(env_from(&[("ARNEB_ADMIN_USERS", " a, ,b ")]))
            .unwrap();
        assert_eq!(cfg.admin_users, ["a", "b"]);
        assert!(toml::from_str::<AccessControlConfig>("admin_user = []").is_err());
    }
}
