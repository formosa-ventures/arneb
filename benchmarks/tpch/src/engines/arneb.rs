//! Arneb adapter — talks to a running Arneb server over the PostgreSQL wire
//! protocol via tokio-postgres.

use std::time::Instant;

use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use tokio_postgres::types::Type;
use tokio_postgres::Row;

use crate::canonical::CanonicalValue;

use super::{BenchmarkEngine, EngineError, EngineResult};

pub struct ArnebEngine {
    host: String,
    port: u16,
    user: String,
    dbname: String,
    client: Option<tokio_postgres::Client>,
}

impl ArnebEngine {
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
            user: "test".into(),
            dbname: "test".into(),
            client: None,
        }
    }
}

#[async_trait]
impl BenchmarkEngine for ArnebEngine {
    fn name(&self) -> &'static str {
        "arneb"
    }

    fn host(&self) -> String {
        self.host.clone()
    }

    fn port(&self) -> Option<u16> {
        Some(self.port)
    }

    async fn connect(&mut self) -> Result<(), EngineError> {
        let conn_str = format!(
            "host={} port={} user={} dbname={}",
            self.host, self.port, self.user, self.dbname
        );
        let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
            .await
            .map_err(|e| EngineError::Connect(format!("{}: {}", conn_str, e)))?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("arneb connection task error: {e}");
            }
        });
        self.client = Some(client);
        Ok(())
    }

    async fn execute(&mut self, sql: &str) -> Result<EngineResult, EngineError> {
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| EngineError::Connect("arneb: not connected".into()))?;
        let start = Instant::now();
        let rows = client
            .query(sql, &[])
            .await
            .map_err(|e| EngineError::Query(e.to_string()))?;
        let elapsed = start.elapsed();
        let canonical_rows = rows.iter().map(convert_row).collect::<Vec<_>>();
        Ok(EngineResult {
            rows: canonical_rows,
            elapsed,
        })
    }
}

/// Convert one tokio-postgres row into a vector of CanonicalValues by
/// dispatching on the column's PG type.
fn convert_row(row: &Row) -> Vec<CanonicalValue> {
    (0..row.len()).map(|i| convert_cell(row, i)).collect()
}

fn convert_cell(row: &Row, idx: usize) -> CanonicalValue {
    let col_type = row.columns()[idx].type_().clone();
    match col_type {
        Type::BOOL => row
            .try_get::<_, Option<bool>>(idx)
            .ok()
            .flatten()
            .map(CanonicalValue::Bool)
            .unwrap_or(CanonicalValue::Null),
        Type::INT2 => row
            .try_get::<_, Option<i16>>(idx)
            .ok()
            .flatten()
            .map(|v| CanonicalValue::Int(v as i64))
            .unwrap_or(CanonicalValue::Null),
        Type::INT4 => row
            .try_get::<_, Option<i32>>(idx)
            .ok()
            .flatten()
            .map(|v| CanonicalValue::Int(v as i64))
            .unwrap_or(CanonicalValue::Null),
        Type::INT8 => row
            .try_get::<_, Option<i64>>(idx)
            .ok()
            .flatten()
            .map(CanonicalValue::Int)
            .unwrap_or(CanonicalValue::Null),
        Type::FLOAT4 => row
            .try_get::<_, Option<f32>>(idx)
            .ok()
            .flatten()
            .map(|v| CanonicalValue::Float(v as f64))
            .unwrap_or(CanonicalValue::Null),
        Type::FLOAT8 => row
            .try_get::<_, Option<f64>>(idx)
            .ok()
            .flatten()
            .map(CanonicalValue::Float)
            .unwrap_or(CanonicalValue::Null),
        Type::NUMERIC => {
            // tokio-postgres has no built-in NUMERIC parser without an extra
            // crate. Read it as the wire-format string via the catch-all.
            read_as_string(row, idx)
        }
        Type::DATE => row
            .try_get::<_, Option<NaiveDate>>(idx)
            .ok()
            .flatten()
            .map(|d| CanonicalValue::Timestamp(d.format("%Y-%m-%d").to_string()))
            .unwrap_or(CanonicalValue::Null),
        Type::TIMESTAMP => row
            .try_get::<_, Option<NaiveDateTime>>(idx)
            .ok()
            .flatten()
            .map(|t| CanonicalValue::Timestamp(format_naive_utc(t)))
            .unwrap_or(CanonicalValue::Null),
        Type::TIMESTAMPTZ => row
            .try_get::<_, Option<DateTime<Utc>>>(idx)
            .ok()
            .flatten()
            .map(|t| CanonicalValue::Timestamp(t.to_rfc3339()))
            .unwrap_or(CanonicalValue::Null),
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME => row
            .try_get::<_, Option<String>>(idx)
            .ok()
            .flatten()
            .map(CanonicalValue::Str)
            .unwrap_or(CanonicalValue::Null),
        _ => read_as_string(row, idx),
    }
}

fn format_naive_utc(t: NaiveDateTime) -> String {
    DateTime::<Utc>::from_naive_utc_and_offset(t, Utc).to_rfc3339()
}

fn read_as_string(row: &Row, idx: usize) -> CanonicalValue {
    // Try a few common text-like getters; fall back to Null on failure.
    if let Ok(Some(s)) = row.try_get::<_, Option<String>>(idx) {
        return CanonicalValue::Str(s);
    }
    if let Ok(Some(s)) = row.try_get::<_, Option<&str>>(idx) {
        return CanonicalValue::Str(s.to_string());
    }
    CanonicalValue::Null
}
