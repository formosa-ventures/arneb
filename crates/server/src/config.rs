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

    /// Admission control (coordinator/standalone only). Raw file values;
    /// the effective settings are in [`AppConfig::admission_settings`].
    #[serde(default)]
    pub admission: AdmissionConfig,

    /// Effective admission settings (env applied, validated).
    #[serde(skip)]
    pub admission_settings: AdmissionSettings,
}

/// `[web]` section.
///
/// ```toml
/// [web]
/// bind_address = "0.0.0.0"   # ARNEB_WEB_BIND_ADDRESS
/// ```
///
/// The Web UI has no login and shows query text, so under
/// `[auth] type = "password"` it binds to `127.0.0.1` unless this is set.
/// The port stays at the pgwire port + 1000.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WebConfig {
    #[serde(default)]
    pub bind_address: Option<String>,
}

impl WebConfig {
    /// Apply `ARNEB_WEB_BIND_ADDRESS` (env > file).
    pub fn apply_env_overrides(&mut self, env: impl Fn(&str) -> Option<String>) {
        if let Some(v) = env("ARNEB_WEB_BIND_ADDRESS") {
            self.bind_address = Some(v);
        }
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
/// and the server-wide statement timeout.
///
/// ```toml
/// [admission]
/// max_running_queries = 8     # ARNEB_MAX_RUNNING_QUERIES; 0 = unlimited
/// max_queued_queries  = 256   # ARNEB_MAX_QUEUED_QUERIES
/// statement_timeout   = "0"   # ARNEB_STATEMENT_TIMEOUT; e.g. "30s", "5min", 30000 (ms)
/// ```
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdmissionConfig {
    #[serde(default)]
    pub max_running_queries: Option<usize>,
    #[serde(default)]
    pub max_queued_queries: Option<usize>,
    #[serde(default)]
    pub statement_timeout: Option<TimeLimitValue>,
}

/// A time limit written as milliseconds (`30000`) or a string (`"30s"`).
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum TimeLimitValue {
    Millis(u64),
    Text(String),
}

/// Default `max_running_queries` (admission-control decisions log #1).
pub const DEFAULT_MAX_RUNNING_QUERIES: usize = 8;
/// Default `max_queued_queries` (decisions log #2).
pub const DEFAULT_MAX_QUEUED_QUERIES: usize = 256;

/// Validated `[admission]` settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmissionSettings {
    /// `0` = unlimited.
    pub max_running_queries: usize,
    pub max_queued_queries: usize,
    /// Server default for sessions that set no timeout; `None` = off.
    pub statement_timeout: Option<std::time::Duration>,
}

impl Default for AdmissionSettings {
    fn default() -> Self {
        Self {
            max_running_queries: DEFAULT_MAX_RUNNING_QUERIES,
            max_queued_queries: DEFAULT_MAX_QUEUED_QUERIES,
            statement_timeout: None,
        }
    }
}

impl AdmissionConfig {
    /// Apply `ARNEB_MAX_RUNNING_QUERIES`, `ARNEB_MAX_QUEUED_QUERIES` and
    /// `ARNEB_STATEMENT_TIMEOUT` (looked up through `env`) over the file
    /// values and validate.
    pub fn resolve(&self, env: impl Fn(&str) -> Option<String>) -> Result<AdmissionSettings> {
        let count = |var: &str, file: Option<usize>, default: usize| -> Result<usize> {
            match env(var) {
                Some(v) => v.trim().parse().map_err(|_| {
                    anyhow::anyhow!("invalid {var} value '{v}' (expected a non-negative integer)")
                }),
                None => Ok(file.unwrap_or(default)),
            }
        };
        let max_running_queries = count(
            "ARNEB_MAX_RUNNING_QUERIES",
            self.max_running_queries,
            DEFAULT_MAX_RUNNING_QUERIES,
        )?;
        let max_queued_queries = count(
            "ARNEB_MAX_QUEUED_QUERIES",
            self.max_queued_queries,
            DEFAULT_MAX_QUEUED_QUERIES,
        )?;
        let timeout_text = match (env("ARNEB_STATEMENT_TIMEOUT"), &self.statement_timeout) {
            (Some(v), _) => Some(v),
            (None, Some(TimeLimitValue::Millis(ms))) => Some(ms.to_string()),
            (None, Some(TimeLimitValue::Text(t))) => Some(t.clone()),
            (None, None) => None,
        };
        let statement_timeout = match timeout_text {
            None => None,
            Some(t) => arneb_common::duration::parse_time_limit(&t)
                .map_err(|e| anyhow::anyhow!("[admission] statement_timeout: {e}"))?,
        };
        Ok(AdmissionSettings {
            max_running_queries,
            max_queued_queries,
            statement_timeout,
        })
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
    /// Validate the section and build the protocol-layer [`AuthMethod`].
    pub fn to_auth_method(&self) -> Result<AuthMethod> {
        match self.auth_type {
            AuthType::None => {
                if !self.users.is_empty() {
                    tracing::warn!(
                        users = self.users.len(),
                        "[auth] users are configured but type = \"none\"; they are ignored"
                    );
                }
                Ok(AuthMethod::None)
            }
            AuthType::Password => {
                let users = self
                    .users
                    .iter()
                    .map(|u| {
                        let verifier: ScramVerifier = u
                            .password_hash
                            .parse()
                            .map_err(|e| anyhow::anyhow!("[auth] user '{}': {e}", u.name))?;
                        Ok((u.name.clone(), verifier))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let creds =
                    UserCredentials::new(users).map_err(|e| anyhow::anyhow!("[auth] {e}"))?;
                Ok(AuthMethod::ScramSha256(Arc::new(creds)))
            }
        }
    }
}

/// Fail closed: password auth on an enabled Trino listener needs `[tls]`.
/// Trino clients only send passwords over HTTPS, so a plain-HTTP listener
/// under password auth would be either unusable or a bypass.
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
            self.enabled = match val.trim().to_ascii_lowercase().as_str() {
                "1" | "true" | "on" | "yes" => true,
                "0" | "false" | "off" | "no" => false,
                other => bail!("invalid ARNEB_TRINO_ENABLED value '{other}' (expected true/false)"),
            };
        }
        if let Ok(val) = std::env::var("ARNEB_TRINO_PORT") {
            self.port = val
                .trim()
                .parse()
                .map_err(|_| anyhow::anyhow!("invalid ARNEB_TRINO_PORT value '{val}'"))?;
        }
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
    /// Catalog type (currently only "hive" is supported; Iceberg tables in
    /// the metastore are read through it).
    #[serde(rename = "type")]
    pub catalog_type: String,
    /// Hive Metastore URI (e.g., "thrift://hms.internal:9083").
    pub metastore_uri: Option<String>,
    /// Default schema name within this catalog.
    #[serde(default = "default_catalog_schema")]
    pub default_schema: String,
    /// Per-catalog storage configuration (overrides global [storage]).
    pub storage: Option<StorageConfig>,
}

fn default_catalog_schema() -> String {
    "default".to_string()
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

impl ClusterConfig {
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
                        admission_settings: AdmissionSettings::default(),
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
        web.apply_env_overrides(|k| std::env::var(k).ok());
        let admission_settings = config.admission.resolve(|k| std::env::var(k).ok())?;

        Ok(AppConfig {
            server,
            tables: config.tables,
            cluster: config.cluster,
            storage: config.storage,
            catalogs: config.catalogs,
            memory: config.memory,
            auth: config.auth,
            trino,
            tls: config.tls,
            tls_settings,
            web,
            admission: config.admission,
            admission_settings,
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
        web.apply_env_overrides(env_from(&[("ARNEB_WEB_BIND_ADDRESS", "0.0.0.0")]));
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

    #[test]
    fn test_admission_defaults_file_and_env() {
        let none: AppConfig = toml::from_str("port = 5432").unwrap();
        assert_eq!(
            none.admission.resolve(env_from(&[])).unwrap(),
            AdmissionSettings::default()
        );

        let file: AppConfig = toml::from_str(
            "[admission]\nmax_running_queries = 2\nmax_queued_queries = 5\nstatement_timeout = \"30s\"\n",
        )
        .unwrap();
        let s = file.admission.resolve(env_from(&[])).unwrap();
        assert_eq!(s.max_running_queries, 2);
        assert_eq!(s.max_queued_queries, 5);
        assert_eq!(
            s.statement_timeout,
            Some(std::time::Duration::from_secs(30))
        );

        let s = file
            .admission
            .resolve(env_from(&[
                ("ARNEB_MAX_RUNNING_QUERIES", "0"),
                ("ARNEB_STATEMENT_TIMEOUT", "2min"),
            ]))
            .unwrap();
        assert_eq!(s.max_running_queries, 0);
        assert_eq!(
            s.statement_timeout,
            Some(std::time::Duration::from_secs(120))
        );

        let ms: AppConfig = toml::from_str("[admission]\nstatement_timeout = 1500\n").unwrap();
        assert_eq!(
            ms.admission
                .resolve(env_from(&[]))
                .unwrap()
                .statement_timeout,
            Some(std::time::Duration::from_millis(1500))
        );
    }

    #[test]
    fn test_admission_validation() {
        let none: AppConfig = toml::from_str("port = 5432").unwrap();
        assert!(none
            .admission
            .resolve(env_from(&[("ARNEB_MAX_RUNNING_QUERIES", "-1")]))
            .is_err());
        assert!(none
            .admission
            .resolve(env_from(&[("ARNEB_STATEMENT_TIMEOUT", "soon")]))
            .is_err());
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
        assert_eq!(cat.default_schema, "analytics");
        let s3 = cat.storage.as_ref().unwrap().s3.as_ref().unwrap();
        assert_eq!(s3.region.as_deref(), Some("us-west-2"));
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
    fn test_auth_defaults_to_none() {
        let config: AppConfig = toml::from_str("port = 5432\n").unwrap();
        assert_eq!(config.auth.auth_type, AuthType::None);
        assert!(matches!(
            config.auth.to_auth_method().unwrap(),
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
        match config.auth.to_auth_method().unwrap() {
            AuthMethod::ScramSha256(creds) => assert_eq!(creds.len(), 2),
            other => panic!("expected password auth, got {other:?}"),
        }
        // The Debug output must never contain the stored verifier.
        assert!(!format!("{:?}", config.auth).contains(&hash));
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
}
