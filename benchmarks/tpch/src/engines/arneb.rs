//! Arneb adapter — talks to a running Arneb server over the PostgreSQL wire
//! protocol via tokio-postgres.

use std::time::Instant;

use async_trait::async_trait;
use chrono::{DateTime, NaiveDateTime, Utc};
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

/// Convert one cell, decoding from the **text** wire format.
///
/// Arneb declares `FieldFormat::Text` in its RowDescription, so tokio-postgres'
/// typed getters — which decode the binary format for FLOAT8, INT8, DATE and
/// friends — fail on every one of these columns. The previous implementation
/// funnelled each of those failures through `.ok().flatten().unwrap_or(Null)`,
/// so a decode error became a NULL value and the correctness check spent its
/// time comparing fabricated NULLs against the other engines' real results.
/// `SELECT SUM(...)` from arneb hashed to sha256("\N").
///
/// Reading the raw bytes and parsing per declared type keeps decode failures
/// visible: anything unparseable stays as its text, which shows up as a real
/// divergence instead of a silent NULL.
fn convert_cell(row: &Row, idx: usize) -> CanonicalValue {
    let col_type = row.columns()[idx].type_().clone();
    let raw = match row.try_get::<_, Option<RawWireText>>(idx) {
        Ok(Some(RawWireText(s))) => s,
        // A genuine SQL NULL.
        Ok(None) => return CanonicalValue::Null,
        // Should not happen — RawWireText accepts every type — but do not
        // manufacture a NULL if it ever does.
        Err(e) => return CanonicalValue::Str(format!("<decode error: {e}>")),
    };

    match col_type {
        Type::BOOL => match raw.as_str() {
            "t" | "true" | "TRUE" | "1" => CanonicalValue::Bool(true),
            "f" | "false" | "FALSE" | "0" => CanonicalValue::Bool(false),
            _ => CanonicalValue::Str(raw),
        },
        Type::INT2 | Type::INT4 | Type::INT8 => match raw.parse::<i64>() {
            Ok(v) => CanonicalValue::Int(v),
            Err(_) => CanonicalValue::Str(raw),
        },
        // NUMERIC is grouped with the floats on purpose: arneb types
        // `SUM(double * double)` as NUMERIC where Trino and DataFusion call the
        // same expression a double. Canonicalizing it as text would make every
        // decimal column diverge on formatting alone.
        Type::FLOAT4 | Type::FLOAT8 | Type::NUMERIC => match raw.parse::<f64>() {
            Ok(v) => CanonicalValue::Float(v),
            Err(_) => CanonicalValue::Str(raw),
        },
        Type::DATE => CanonicalValue::Timestamp(raw),
        Type::TIMESTAMP => match NaiveDateTime::parse_from_str(&raw, "%Y-%m-%d %H:%M:%S%.f") {
            Ok(t) => CanonicalValue::Timestamp(format_naive_utc(t)),
            Err(_) => CanonicalValue::Timestamp(raw),
        },
        Type::TIMESTAMPTZ => match DateTime::parse_from_rfc3339(&raw) {
            Ok(t) => CanonicalValue::Timestamp(t.with_timezone(&Utc).to_rfc3339()),
            Err(_) => CanonicalValue::Timestamp(raw),
        },
        _ => CanonicalValue::Str(raw),
    }
}

fn format_naive_utc(t: NaiveDateTime) -> String {
    DateTime::<Utc>::from_naive_utc_and_offset(t, Utc).to_rfc3339()
}

/// Reads any column as its raw wire bytes, whatever the declared type.
///
/// `String`'s `FromSql` only accepts the text-ish OIDs, so asking it for a
/// NUMERIC fails — and the previous fallback turned that failure into
/// `CanonicalValue::Null`. Every NUMERIC arneb returned was therefore recorded
/// as NULL and compared against the other engines' real values, so the
/// correctness check was reporting divergences it had manufactured itself.
///
/// Arneb declares `FieldFormat::Text` in its RowDescription, so these bytes are
/// the value's ASCII text.
struct RawWireText(String);

impl<'a> tokio_postgres::types::FromSql<'a> for RawWireText {
    fn from_sql(
        _ty: &tokio_postgres::types::Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(RawWireText(String::from_utf8_lossy(raw).into_owned()))
    }

    fn accepts(_ty: &tokio_postgres::types::Type) -> bool {
        true
    }
}
