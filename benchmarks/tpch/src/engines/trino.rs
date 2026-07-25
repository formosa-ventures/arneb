//! Trino adapter — submits SQL to `/v1/statement` and walks the `nextUri`
//! pagination chain, accumulating rows.

use std::time::Instant;

use async_trait::async_trait;
use serde_json::Value;

use crate::canonical::CanonicalValue;

use super::{BenchmarkEngine, EngineError, EngineResult};

pub struct TrinoEngine {
    host: String,
    port: u16,
    catalog: String,
    schema: String,
    base_url: String,
    http: reqwest::Client,
}

impl TrinoEngine {
    pub fn new(host: impl Into<String>, port: u16, catalog: String, schema: String) -> Self {
        let host = host.into();
        let base_url = format!("http://{host}:{port}");
        Self {
            host,
            port,
            catalog,
            schema,
            base_url,
            http: reqwest::Client::new(),
        }
    }
}

#[async_trait]
impl BenchmarkEngine for TrinoEngine {
    fn name(&self) -> &'static str {
        "trino"
    }

    fn host(&self) -> String {
        self.host.clone()
    }

    fn port(&self) -> Option<u16> {
        Some(self.port)
    }

    async fn connect(&mut self) -> Result<(), EngineError> {
        let info_url = format!("{}/v1/info", self.base_url);
        self.http
            .get(&info_url)
            .send()
            .await
            .map_err(|e| EngineError::Connect(format!("trino at {}: {}", self.base_url, e)))?;
        Ok(())
    }

    async fn execute(&mut self, sql: &str) -> Result<EngineResult, EngineError> {
        let url = format!("{}/v1/statement", self.base_url);
        let start = Instant::now();
        let resp = self
            .http
            .post(&url)
            .header("X-Trino-User", "benchmark")
            .header("X-Trino-Catalog", &self.catalog)
            .header("X-Trino-Schema", &self.schema)
            .body(sql.to_string())
            .send()
            .await
            .map_err(|e| EngineError::Query(e.to_string()))?;
        let mut result: Value = resp
            .json()
            .await
            .map_err(|e| EngineError::Query(e.to_string()))?;

        let mut all_rows: Vec<Vec<CanonicalValue>> = Vec::new();
        let mut column_types: Option<Vec<String>> = None;

        loop {
            if let Some(err) = result.get("error") {
                let msg = err
                    .get("message")
                    .and_then(|m| m.as_str())
                    .unwrap_or("unknown error");
                return Err(EngineError::Query(msg.to_string()));
            }
            if column_types.is_none() {
                if let Some(cols) = result.get("columns").and_then(|c| c.as_array()) {
                    let types = cols
                        .iter()
                        .map(|c| {
                            c.get("type")
                                .and_then(|t| t.as_str())
                                .unwrap_or("")
                                .to_string()
                        })
                        .collect();
                    column_types = Some(types);
                }
            }
            if let Some(data) = result.get("data").and_then(|d| d.as_array()) {
                for raw_row in data {
                    if let Some(arr) = raw_row.as_array() {
                        let row = arr
                            .iter()
                            .enumerate()
                            .map(|(i, v)| {
                                let ty = column_types
                                    .as_ref()
                                    .and_then(|cs| cs.get(i))
                                    .map(String::as_str)
                                    .unwrap_or("");
                                json_to_canonical(v, ty)
                            })
                            .collect();
                        all_rows.push(row);
                    }
                }
            }
            let next_uri = match result.get("nextUri").and_then(|u| u.as_str()) {
                Some(uri) => uri.to_string(),
                None => break,
            };
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            let resp = self
                .http
                .get(&next_uri)
                .send()
                .await
                .map_err(|e| EngineError::Query(e.to_string()))?;
            result = resp
                .json()
                .await
                .map_err(|e| EngineError::Query(e.to_string()))?;
        }

        Ok(EngineResult {
            rows: all_rows,
            elapsed: start.elapsed(),
        })
    }
}

/// Map a Trino JSON cell to CanonicalValue using the column's declared type.
fn json_to_canonical(value: &Value, ty: &str) -> CanonicalValue {
    if value.is_null() {
        return CanonicalValue::Null;
    }
    let lower = ty.to_ascii_lowercase();
    if lower == "boolean" {
        if let Some(b) = value.as_bool() {
            return CanonicalValue::Bool(b);
        }
    }
    if lower == "tinyint" || lower == "smallint" || lower == "integer" || lower == "bigint" {
        if let Some(i) = value.as_i64() {
            return CanonicalValue::Int(i);
        }
        if let Some(s) = value.as_str() {
            if let Ok(i) = s.parse::<i64>() {
                return CanonicalValue::Int(i);
            }
        }
    }
    if lower == "real" || lower == "double" || lower.starts_with("decimal") {
        if let Some(f) = value.as_f64() {
            return CanonicalValue::Float(f);
        }
        if let Some(s) = value.as_str() {
            if let Ok(f) = s.parse::<f64>() {
                return CanonicalValue::Float(f);
            }
        }
    }
    if lower == "date" || lower.starts_with("timestamp") {
        if let Some(s) = value.as_str() {
            return CanonicalValue::Timestamp(s.to_string());
        }
    }
    // Fallback — coerce whatever we got into a string.
    if let Some(s) = value.as_str() {
        return CanonicalValue::Str(s.to_string());
    }
    if let Some(b) = value.as_bool() {
        return CanonicalValue::Bool(b);
    }
    if let Some(i) = value.as_i64() {
        return CanonicalValue::Int(i);
    }
    if let Some(f) = value.as_f64() {
        return CanonicalValue::Float(f);
    }
    CanonicalValue::Str(value.to_string())
}
