use std::path::Path;

use anyhow::{bail, Result};
use serde::Deserialize;
use trino_common::types::DataType;
use trino_common::ServerConfig;

#[derive(Debug, Deserialize)]
pub struct AppConfig {
    #[serde(flatten)]
    pub server: ServerConfig,

    #[serde(default)]
    pub tables: Vec<TableConfig>,
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
                let default_path = Path::new("./trino-alt.toml");
                if default_path.exists() {
                    let content = std::fs::read_to_string(default_path)?;
                    toml::from_str(&content)
                        .map_err(|e| anyhow::anyhow!("failed to parse trino-alt.toml: {e}"))?
                } else {
                    AppConfig {
                        server: ServerConfig::default(),
                        tables: Vec::new(),
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
        })
    }
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
            unit: trino_common::types::TimeUnit::Microsecond,
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
