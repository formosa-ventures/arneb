//! Storage abstraction over local filesystem and cloud object stores.
//!
//! Provides [`StorageRegistry`] for managing [`ObjectStore`] instances
//! and [`StorageUri`] for parsing storage URIs.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;

use arneb_common::error::ConnectorError;

/// AWS S3 storage configuration for lazy ObjectStore creation.
///
/// Credential resolution order: **config > env var > IAM role**.
/// If `access_key_id`/`secret_access_key` are set here, they override
/// `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` environment variables.
#[derive(Debug, Clone, Default)]
pub struct S3StorageConfig {
    /// AWS region (e.g., "us-east-1").
    pub region: Option<String>,
    /// Custom endpoint URL (for MinIO, LocalStack, etc.).
    pub endpoint: Option<String>,
    /// Allow HTTP (non-TLS) connections. Default: false.
    pub allow_http: bool,
    /// AWS access key ID. Overrides `AWS_ACCESS_KEY_ID` env var when set.
    pub access_key_id: Option<String>,
    /// AWS secret access key. Overrides `AWS_SECRET_ACCESS_KEY` env var when set.
    pub secret_access_key: Option<String>,
}

/// Cloud storage configuration for [`StorageRegistry`].
#[derive(Debug, Clone, Default)]
pub struct CloudStorageConfig {
    /// AWS S3 configuration.
    pub s3: Option<S3StorageConfig>,
}

/// Parsed storage URI with scheme, bucket, and object path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageUri {
    /// Storage scheme (e.g., "s3", "gs", "abfss", "file", "local")
    pub scheme: StorageScheme,
    /// Bucket or container name (empty for local filesystem)
    pub bucket: String,
    /// Object path within the bucket (or local filesystem path)
    pub path: String,
}

/// Supported storage schemes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum StorageScheme {
    /// Local filesystem
    Local,
    /// AWS S3
    S3,
    /// Google Cloud Storage
    Gcs,
    /// Azure Blob Storage
    Azure,
}

impl std::fmt::Display for StorageScheme {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageScheme::Local => write!(f, "local"),
            StorageScheme::S3 => write!(f, "s3"),
            StorageScheme::Gcs => write!(f, "gs"),
            StorageScheme::Azure => write!(f, "abfss"),
        }
    }
}

impl StorageUri {
    /// Parse a URI string into a `StorageUri`.
    ///
    /// Supports:
    /// - `s3://bucket/path/to/file` → S3
    /// - `s3a://bucket/path/to/file` → S3 (Hadoop-style)
    /// - `gs://bucket/path/to/file` → GCS
    /// - `abfss://container@account.dfs.core.windows.net/path` → Azure
    /// - `az://container/path` → Azure
    /// - `file:///path/to/file` → Local
    /// - `/path/to/file` or `relative/path` → Local
    pub fn parse(uri: &str) -> Result<Self, ConnectorError> {
        if let Some(rest) = uri
            .strip_prefix("s3://")
            .or_else(|| uri.strip_prefix("s3a://"))
        {
            let (bucket, path) = split_bucket_path(rest)?;
            Ok(StorageUri {
                scheme: StorageScheme::S3,
                bucket,
                path,
            })
        } else if let Some(rest) = uri.strip_prefix("gs://") {
            let (bucket, path) = split_bucket_path(rest)?;
            Ok(StorageUri {
                scheme: StorageScheme::Gcs,
                bucket,
                path,
            })
        } else if let Some(rest) = uri.strip_prefix("abfss://") {
            // abfss://container@account.dfs.core.windows.net/path
            let (container, path) = if let Some(at_pos) = rest.find('@') {
                let container = rest[..at_pos].to_string();
                let after_at = &rest[at_pos + 1..];
                let path = after_at
                    .find('/')
                    .map(|i| after_at[i + 1..].to_string())
                    .unwrap_or_default();
                (container, path)
            } else {
                split_bucket_path(rest)?
            };
            Ok(StorageUri {
                scheme: StorageScheme::Azure,
                bucket: container,
                path,
            })
        } else if let Some(rest) = uri.strip_prefix("az://") {
            let (container, path) = split_bucket_path(rest)?;
            Ok(StorageUri {
                scheme: StorageScheme::Azure,
                bucket: container,
                path,
            })
        } else if let Some(rest) = uri.strip_prefix("file://") {
            Ok(StorageUri {
                scheme: StorageScheme::Local,
                bucket: String::new(),
                path: rest.to_string(),
            })
        } else {
            // Plain local path
            Ok(StorageUri {
                scheme: StorageScheme::Local,
                bucket: String::new(),
                path: uri.to_string(),
            })
        }
    }

    /// Get the object store path for this URI.
    pub fn object_path(&self) -> ObjectPath {
        ObjectPath::from(self.path.as_str())
    }

    /// Get the cache key for StorageRegistry (scheme + bucket).
    pub fn cache_key(&self) -> String {
        match self.scheme {
            StorageScheme::Local => "local".to_string(),
            _ => format!("{}://{}", self.scheme, self.bucket),
        }
    }
}

fn split_bucket_path(s: &str) -> Result<(String, String), ConnectorError> {
    match s.find('/') {
        Some(i) => Ok((s[..i].to_string(), s[i + 1..].to_string())),
        None => Ok((s.to_string(), String::new())),
    }
}

/// Registry that manages and caches ObjectStore instances.
///
/// Stores are cached by scheme+bucket to avoid recreating clients.
/// When constructed with [`CloudStorageConfig`], cloud stores are
/// lazy-created on first access using the provided configuration.
pub struct StorageRegistry {
    stores: RwLock<HashMap<String, Arc<dyn ObjectStore>>>,
    local: Arc<dyn ObjectStore>,
    config: CloudStorageConfig,
}

impl std::fmt::Debug for StorageRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let stores = self.stores.read().unwrap();
        f.debug_struct("StorageRegistry")
            .field("cached_stores", &stores.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl Default for StorageRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl StorageRegistry {
    /// Create a new StorageRegistry with local filesystem support only.
    pub fn new() -> Self {
        Self {
            stores: RwLock::new(HashMap::new()),
            local: Arc::new(LocalFileSystem::new()),
            config: CloudStorageConfig::default(),
        }
    }

    /// Create a new StorageRegistry with cloud storage configuration.
    ///
    /// Cloud ObjectStore instances (S3, GCS, Azure) are lazy-created
    /// on first access using the provided config.
    pub fn with_config(config: CloudStorageConfig) -> Self {
        Self {
            stores: RwLock::new(HashMap::new()),
            local: Arc::new(LocalFileSystem::new()),
            config,
        }
    }

    /// Get or create an ObjectStore for the given URI.
    ///
    /// For local URIs, returns the built-in `LocalFileSystem`.
    /// For cloud URIs, checks the cache first, then lazy-creates
    /// from config if not found.
    pub fn get_store(&self, uri: &StorageUri) -> Result<Arc<dyn ObjectStore>, ConnectorError> {
        match uri.scheme {
            StorageScheme::Local => Ok(self.local.clone()),
            _ => {
                let key = uri.cache_key();
                // Check cache first
                {
                    let stores = self.stores.read().unwrap();
                    if let Some(store) = stores.get(&key) {
                        return Ok(store.clone());
                    }
                }
                // Cache miss — try to build from config
                let store = self.build_store(uri)?;
                let mut stores = self.stores.write().unwrap();
                // Double-check after acquiring write lock
                if let Some(existing) = stores.get(&key) {
                    return Ok(existing.clone());
                }
                stores.insert(key, store.clone());
                Ok(store)
            }
        }
    }

    /// Register a pre-configured ObjectStore for a given cache key.
    ///
    /// Used for testing (e.g., injecting `InMemory` stores) or manual overrides.
    pub fn register_store(&self, key: &str, store: Arc<dyn ObjectStore>) {
        let mut stores = self.stores.write().unwrap();
        stores.insert(key.to_string(), store);
    }

    /// Build a cloud ObjectStore from config.
    fn build_store(&self, uri: &StorageUri) -> Result<Arc<dyn ObjectStore>, ConnectorError> {
        match uri.scheme {
            StorageScheme::S3 => self.build_s3_store(&uri.bucket),
            StorageScheme::Gcs | StorageScheme::Azure => {
                Err(ConnectorError::UnsupportedOperation(format!(
                    "Cloud storage backend '{}' is not yet implemented.",
                    uri.scheme
                )))
            }
            StorageScheme::Local => unreachable!("local handled in get_store"),
        }
    }

    /// Build an S3 ObjectStore from config.
    ///
    /// Credential resolution: `from_env()` loads AWS env vars as a base,
    /// then config values override when present (config > env > defaults).
    fn build_s3_store(&self, bucket: &str) -> Result<Arc<dyn ObjectStore>, ConnectorError> {
        // Start from env vars (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION, etc.)
        let mut builder = AmazonS3Builder::from_env().with_bucket_name(bucket);

        // Config values override env vars when present
        if let Some(s3_config) = &self.config.s3 {
            if let Some(region) = &s3_config.region {
                builder = builder.with_region(region);
            }
            if let Some(endpoint) = &s3_config.endpoint {
                builder = builder.with_endpoint(endpoint);
            }
            if s3_config.allow_http {
                builder = builder.with_allow_http(true);
            }
            if let Some(key_id) = &s3_config.access_key_id {
                builder = builder.with_access_key_id(key_id);
            }
            if let Some(secret) = &s3_config.secret_access_key {
                builder = builder.with_secret_access_key(secret);
            }
        }

        let store = builder.build().map_err(|e| {
            ConnectorError::ConnectionFailed(format!(
                "Failed to create S3 client for '{bucket}': {e}"
            ))
        })?;

        Ok(Arc::new(store))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_s3_uri() {
        let uri = StorageUri::parse("s3://my-bucket/path/to/file.parquet").unwrap();
        assert_eq!(uri.scheme, StorageScheme::S3);
        assert_eq!(uri.bucket, "my-bucket");
        assert_eq!(uri.path, "path/to/file.parquet");
    }

    #[test]
    fn test_parse_s3a_uri() {
        let uri = StorageUri::parse("s3a://bucket/data/table").unwrap();
        assert_eq!(uri.scheme, StorageScheme::S3);
        assert_eq!(uri.bucket, "bucket");
        assert_eq!(uri.path, "data/table");
    }

    #[test]
    fn test_parse_gs_uri() {
        let uri = StorageUri::parse("gs://analytics-bucket/events.parquet").unwrap();
        assert_eq!(uri.scheme, StorageScheme::Gcs);
        assert_eq!(uri.bucket, "analytics-bucket");
        assert_eq!(uri.path, "events.parquet");
    }

    #[test]
    fn test_parse_abfss_uri() {
        let uri =
            StorageUri::parse("abfss://container@account.dfs.core.windows.net/path/file.parquet")
                .unwrap();
        assert_eq!(uri.scheme, StorageScheme::Azure);
        assert_eq!(uri.bucket, "container");
        assert_eq!(uri.path, "path/file.parquet");
    }

    #[test]
    fn test_parse_az_uri() {
        let uri = StorageUri::parse("az://container/path/file.parquet").unwrap();
        assert_eq!(uri.scheme, StorageScheme::Azure);
        assert_eq!(uri.bucket, "container");
        assert_eq!(uri.path, "path/file.parquet");
    }

    #[test]
    fn test_parse_file_uri() {
        let uri = StorageUri::parse("file:///data/local.parquet").unwrap();
        assert_eq!(uri.scheme, StorageScheme::Local);
        assert_eq!(uri.bucket, "");
        assert_eq!(uri.path, "/data/local.parquet");
    }

    #[test]
    fn test_parse_plain_path() {
        let uri = StorageUri::parse("/data/local.parquet").unwrap();
        assert_eq!(uri.scheme, StorageScheme::Local);
        assert_eq!(uri.bucket, "");
        assert_eq!(uri.path, "/data/local.parquet");
    }

    #[test]
    fn test_parse_relative_path() {
        let uri = StorageUri::parse("data/local.parquet").unwrap();
        assert_eq!(uri.scheme, StorageScheme::Local);
        assert_eq!(uri.path, "data/local.parquet");
    }

    #[test]
    fn test_parse_s3_bucket_only() {
        let uri = StorageUri::parse("s3://my-bucket").unwrap();
        assert_eq!(uri.scheme, StorageScheme::S3);
        assert_eq!(uri.bucket, "my-bucket");
        assert_eq!(uri.path, "");
    }

    #[test]
    fn test_cache_key() {
        let s3 = StorageUri::parse("s3://bucket/path").unwrap();
        assert_eq!(s3.cache_key(), "s3://bucket");

        let local = StorageUri::parse("/data/file").unwrap();
        assert_eq!(local.cache_key(), "local");
    }

    #[test]
    fn test_storage_registry_local() {
        let registry = StorageRegistry::new();
        let uri = StorageUri::parse("/data/file.parquet").unwrap();
        let store = registry.get_store(&uri);
        assert!(store.is_ok());
    }

    #[test]
    fn test_storage_registry_s3_without_config_uses_env() {
        // With from_env(), S3 stores can be built even without explicit config
        // (credentials come from AWS_* env vars or instance profile).
        let registry = StorageRegistry::new();
        let uri = StorageUri::parse("s3://bucket/file.parquet").unwrap();
        let store = registry.get_store(&uri);
        assert!(
            store.is_ok(),
            "S3 store should build via from_env() even without config"
        );
    }

    #[test]
    fn test_storage_registry_manual_caching() {
        let registry = StorageRegistry::new();
        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
        registry.register_store("s3://my-bucket", store);

        let uri = StorageUri::parse("s3://my-bucket/file.parquet").unwrap();
        assert!(registry.get_store(&uri).is_ok());

        // Different bucket is lazily created (not cached yet, but builds via from_env)
        let uri2 = StorageUri::parse("s3://other-bucket/file.parquet").unwrap();
        assert!(registry.get_store(&uri2).is_ok());
    }

    #[test]
    fn test_storage_registry_lazy_creation_with_s3_config() {
        let config = CloudStorageConfig {
            s3: Some(S3StorageConfig {
                region: Some("us-east-1".to_string()),
                endpoint: Some("http://localhost:9000".to_string()),
                allow_http: true,
                ..Default::default()
            }),
        };
        let registry = StorageRegistry::with_config(config);

        // Lazy creation should succeed (builds AmazonS3 client from config)
        let uri = StorageUri::parse("s3://test-bucket/data/file.parquet").unwrap();
        let store = registry.get_store(&uri);
        assert!(store.is_ok(), "lazy S3 store creation should succeed");

        // Second call should hit cache
        let store2 = registry.get_store(&uri);
        assert!(store2.is_ok(), "cached S3 store lookup should succeed");
    }

    #[test]
    fn test_storage_registry_lazy_creation_different_buckets() {
        let config = CloudStorageConfig {
            s3: Some(S3StorageConfig {
                region: Some("us-west-2".to_string()),
                ..Default::default()
            }),
        };
        let registry = StorageRegistry::with_config(config);

        let uri1 = StorageUri::parse("s3://bucket-a/data.parquet").unwrap();
        let uri2 = StorageUri::parse("s3://bucket-b/data.parquet").unwrap();

        assert!(registry.get_store(&uri1).is_ok());
        assert!(registry.get_store(&uri2).is_ok());
    }

    #[test]
    fn test_storage_registry_no_s3_config_still_uses_env() {
        // Even without [storage.s3] config, from_env() can build a client
        // using AWS_* environment variables.
        let config = CloudStorageConfig { s3: None };
        let registry = StorageRegistry::with_config(config);

        let uri = StorageUri::parse("s3://bucket/file.parquet").unwrap();
        let result = registry.get_store(&uri);
        assert!(
            result.is_ok(),
            "S3 store should build via from_env() even without config section"
        );
    }

    #[test]
    fn test_storage_registry_manual_override_takes_precedence() {
        let config = CloudStorageConfig {
            s3: Some(S3StorageConfig {
                region: Some("us-east-1".to_string()),
                endpoint: Some("http://localhost:9000".to_string()),
                allow_http: true,
                ..Default::default()
            }),
        };
        let registry = StorageRegistry::with_config(config);

        // Pre-register an InMemory store for a specific bucket
        let mock_store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        registry.register_store("s3://my-bucket", mock_store);

        // Should return the manually registered store, not build a new one
        let uri = StorageUri::parse("s3://my-bucket/file.parquet").unwrap();
        assert!(registry.get_store(&uri).is_ok());
    }
}
