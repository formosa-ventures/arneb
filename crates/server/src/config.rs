use std::path::Path;

use anyhow::{bail, Result};
use arneb_common::types::DataType;
use arneb_common::ServerConfig;
use arneb_connectors::{CloudStorageConfig, S3StorageConfig};
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

    /// Trino-style per-task query memory cap. When set, each worker
    /// task wraps the global spill pool with a [`QueryMemoryPool`]
    /// that fails the query (structured `ResourceExhausted`) when the
    /// task's cumulative allocation crosses this cap — covers the
    /// swiss-cheese gap where untracked operators (Filter / Project /
    /// scan buffers / Repartition queue / etc.) accumulate uncounted
    /// allocations and OOM the worker before any single tracked
    /// operator (SemiJoinExec / HashJoinExec) hits its own
    /// `spill_budget_bytes`. Recommended ≥ `spill_budget_bytes`
    /// (the SemiJoin/HashJoin build alone can take that much; the
    /// query needs some headroom on top). `None` disables (no
    /// query-level cap; only the operator-level spill budget fires).
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

    /// Resolve the per-task query memory cap. `ARNEB_QUERY_MAX_MEMORY_BYTES`
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
    /// Catalog type (currently only "hive" is supported).
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
                    }
                }
            }
        };

        let mut server = config.server;
        server.apply_env_overrides()?;
        server.validate()?;

        Ok(AppConfig {
            server,
            tables: config.tables,
            cluster: config.cluster,
            storage: config.storage,
            catalogs: config.catalogs,
            memory: config.memory,
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
}
