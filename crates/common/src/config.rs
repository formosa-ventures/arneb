//! Server configuration management for trino-alt.

use std::fmt;
use std::path::Path;

use serde::Deserialize;

use crate::error::ConfigError;

/// Server configuration loaded from TOML file and/or environment variables.
#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    /// Server listen address.
    #[serde(default = "default_bind_address")]
    pub bind_address: String,

    /// Server listen port.
    #[serde(default = "default_port")]
    pub port: u16,

    /// Number of worker threads for query execution.
    #[serde(default = "default_max_worker_threads")]
    pub max_worker_threads: usize,

    /// Maximum memory usage in megabytes.
    #[serde(default = "default_max_memory_mb")]
    pub max_memory_mb: usize,
}

fn default_bind_address() -> String {
    "127.0.0.1".to_string()
}

fn default_port() -> u16 {
    5432
}

fn default_max_worker_threads() -> usize {
    num_cpus::get()
}

fn default_max_memory_mb() -> usize {
    1024
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_address: default_bind_address(),
            port: default_port(),
            max_worker_threads: default_max_worker_threads(),
            max_memory_mb: default_max_memory_mb(),
        }
    }
}

/// Parse an environment variable value, returning a `ConfigError::InvalidValue` on failure.
fn parse_env_var<T: std::str::FromStr>(key: &str, value: &str) -> Result<T, ConfigError> {
    value.parse::<T>().map_err(|_| ConfigError::InvalidValue {
        key: key.to_string(),
        value: value.to_string(),
        reason: format!("expected {}", std::any::type_name::<T>()),
    })
}

impl ServerConfig {
    /// Load config from a TOML file.
    pub fn from_file(path: &Path) -> Result<Self, ConfigError> {
        let content = std::fs::read_to_string(path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                ConfigError::FileNotFound(path.display().to_string())
            } else {
                ConfigError::ParseError(format!("failed to read {}: {e}", path.display()))
            }
        })?;
        toml::from_str(&content).map_err(|e| ConfigError::ParseError(e.to_string()))
    }

    /// Apply environment variable overrides with `TRINO_` prefix.
    /// Env vars take precedence over file/default values.
    pub fn apply_env_overrides(&mut self) -> Result<(), ConfigError> {
        if let Ok(val) = std::env::var("TRINO_BIND_ADDRESS") {
            self.bind_address = val;
        }

        if let Ok(val) = std::env::var("TRINO_PORT") {
            self.port = parse_env_var("port", &val)?;
        }

        if let Ok(val) = std::env::var("TRINO_MAX_WORKER_THREADS") {
            self.max_worker_threads = parse_env_var("max_worker_threads", &val)?;
        }

        if let Ok(val) = std::env::var("TRINO_MAX_MEMORY_MB") {
            self.max_memory_mb = parse_env_var("max_memory_mb", &val)?;
        }

        Ok(())
    }

    /// Validate configuration values.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.port == 0 {
            return Err(ConfigError::InvalidValue {
                key: "port".to_string(),
                value: "0".to_string(),
                reason: "port must be > 0".to_string(),
            });
        }

        if self.max_memory_mb == 0 {
            return Err(ConfigError::InvalidValue {
                key: "max_memory_mb".to_string(),
                value: "0".to_string(),
                reason: "max_memory_mb must be > 0".to_string(),
            });
        }

        Ok(())
    }

    /// Load configuration: file (optional) + env overrides + validation.
    ///
    /// If `path` is `Some`, loads from that file (error if missing).
    /// If `path` is `None`, tries the default path `./trino-alt.toml`,
    /// falling back to defaults if the file doesn't exist.
    pub fn load(path: Option<&Path>) -> Result<Self, ConfigError> {
        let mut config = match path {
            Some(p) => Self::from_file(p)?,
            None => {
                let default_path = Path::new("./trino-alt.toml");
                if default_path.exists() {
                    Self::from_file(default_path)?
                } else {
                    Self::default()
                }
            }
        };

        config.apply_env_overrides()?;
        config.validate()?;
        Ok(config)
    }
}

impl fmt::Display for ServerConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Server Configuration:")?;
        writeln!(f, "  bind_address:       {}", self.bind_address)?;
        writeln!(f, "  port:               {}", self.port)?;
        writeln!(f, "  max_worker_threads: {}", self.max_worker_threads)?;
        write!(f, "  max_memory_mb:      {}", self.max_memory_mb)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::sync::Mutex;

    // Env vars are process-global. Serialize tests that touch them.
    static ENV_MUTEX: Mutex<()> = Mutex::new(());

    fn clear_trino_env_vars() {
        std::env::remove_var("TRINO_BIND_ADDRESS");
        std::env::remove_var("TRINO_PORT");
        std::env::remove_var("TRINO_MAX_WORKER_THREADS");
        std::env::remove_var("TRINO_MAX_MEMORY_MB");
    }

    #[test]
    fn default_values() {
        let config = ServerConfig::default();
        assert_eq!(config.bind_address, "127.0.0.1");
        assert_eq!(config.port, 5432);
        assert!(config.max_worker_threads > 0);
        assert_eq!(config.max_memory_mb, 1024);
    }

    #[test]
    fn from_toml_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.toml");
        {
            let mut f = std::fs::File::create(&path).unwrap();
            writeln!(f, r#"bind_address = "0.0.0.0""#).unwrap();
            writeln!(f, "port = 8080").unwrap();
            writeln!(f, "max_worker_threads = 4").unwrap();
            writeln!(f, "max_memory_mb = 2048").unwrap();
        }

        let config = ServerConfig::from_file(&path).unwrap();
        assert_eq!(config.bind_address, "0.0.0.0");
        assert_eq!(config.port, 8080);
        assert_eq!(config.max_worker_threads, 4);
        assert_eq!(config.max_memory_mb, 2048);
    }

    #[test]
    fn from_toml_partial() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("partial.toml");
        {
            let mut f = std::fs::File::create(&path).unwrap();
            writeln!(f, "port = 9999").unwrap();
        }

        let config = ServerConfig::from_file(&path).unwrap();
        assert_eq!(config.bind_address, "127.0.0.1"); // default
        assert_eq!(config.port, 9999);
    }

    #[test]
    fn file_not_found() {
        let result = ServerConfig::from_file(Path::new("/nonexistent/path.toml"));
        assert!(matches!(result, Err(ConfigError::FileNotFound(_))));
    }

    #[test]
    fn malformed_toml() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.toml");
        std::fs::write(&path, "this is not valid { toml [").unwrap();

        let result = ServerConfig::from_file(&path);
        assert!(matches!(result, Err(ConfigError::ParseError(_))));
    }

    #[test]
    fn env_var_override() {
        let _lock = ENV_MUTEX.lock().unwrap();
        clear_trino_env_vars();

        std::env::set_var("TRINO_BIND_ADDRESS", "10.0.0.1");
        let mut config = ServerConfig::default();
        config.apply_env_overrides().unwrap();
        assert_eq!(config.bind_address, "10.0.0.1");

        clear_trino_env_vars();
    }

    #[test]
    fn env_var_invalid_value() {
        let _lock = ENV_MUTEX.lock().unwrap();
        clear_trino_env_vars();

        std::env::set_var("TRINO_MAX_MEMORY_MB", "not_a_number");
        let mut config = ServerConfig::default();
        let result = config.apply_env_overrides();
        assert!(matches!(result, Err(ConfigError::InvalidValue { .. })));

        clear_trino_env_vars();
    }

    #[test]
    fn validate_port_zero() {
        let config = ServerConfig {
            port: 0,
            ..ServerConfig::default()
        };
        let result = config.validate();
        assert!(matches!(result, Err(ConfigError::InvalidValue { .. })));
    }

    #[test]
    fn validate_memory_zero() {
        let config = ServerConfig {
            max_memory_mb: 0,
            ..ServerConfig::default()
        };
        let result = config.validate();
        assert!(matches!(result, Err(ConfigError::InvalidValue { .. })));
    }

    #[test]
    fn load_missing_default_file_uses_defaults() {
        let _lock = ENV_MUTEX.lock().unwrap();
        clear_trino_env_vars();

        let config = ServerConfig::load(None).unwrap();
        assert_eq!(config.bind_address, "127.0.0.1");
        assert_eq!(config.port, 5432);
    }

    #[test]
    fn display_format() {
        let config = ServerConfig::default();
        let display = config.to_string();
        assert!(display.contains("bind_address"));
        assert!(display.contains("port"));
        assert!(display.contains("max_worker_threads"));
        assert!(display.contains("max_memory_mb"));
    }
}
