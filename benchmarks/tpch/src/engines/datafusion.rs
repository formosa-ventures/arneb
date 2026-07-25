//! DataFusion adapter — runs in-process via the `datafusion` crate. Reads the
//! same Parquet files Arneb and Trino read by registering an `AmazonS3`
//! object store pointed at MinIO (or real S3) and registering each TPC-H
//! table as a `ListingTable`.

use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use datafusion::arrow::array::{
    Array, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, Int8Array, LargeStringArray, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::prelude::SessionContext;
use object_store::aws::AmazonS3Builder;
use url::Url;

use crate::canonical::CanonicalValue;

use super::{BenchmarkEngine, EngineError, EngineResult};

const TPCH_TABLES: &[&str] = &[
    "lineitem", "orders", "customer", "part", "partsupp", "supplier", "nation", "region",
];

#[derive(Clone, Debug)]
pub struct DataFusionConfig {
    pub endpoint: String,
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub bucket: String,
    pub prefix: String,
    pub allow_http: bool,
}

impl Default for DataFusionConfig {
    fn default() -> Self {
        Self {
            endpoint: "http://127.0.0.1:9000".into(),
            region: "us-east-1".into(),
            access_key_id: "minioadmin".into(),
            secret_access_key: "minioadmin".into(),
            bucket: "warehouse".into(),
            prefix: "tpch".into(),
            allow_http: true,
        }
    }
}

pub struct DataFusionEngine {
    config: DataFusionConfig,
    ctx: Option<SessionContext>,
}

impl DataFusionEngine {
    pub fn new(config: DataFusionConfig) -> Self {
        Self { config, ctx: None }
    }
}

#[async_trait]
impl BenchmarkEngine for DataFusionEngine {
    fn name(&self) -> &'static str {
        "datafusion"
    }

    fn host(&self) -> String {
        "in-process".into()
    }

    fn port(&self) -> Option<u16> {
        None
    }

    async fn connect(&mut self) -> Result<(), EngineError> {
        let ctx = SessionContext::new();

        // Build S3 object store.
        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(&self.config.bucket)
            .with_region(&self.config.region)
            .with_endpoint(&self.config.endpoint)
            .with_access_key_id(&self.config.access_key_id)
            .with_secret_access_key(&self.config.secret_access_key);
        if self.config.allow_http {
            builder = builder.with_allow_http(true);
        }
        let store = builder
            .build()
            .map_err(|e| EngineError::Connect(format!("S3 builder: {e}")))?;

        let s3_url = Url::parse(&format!("s3://{}", self.config.bucket))
            .map_err(|e| EngineError::Connect(format!("bad bucket url: {e}")))?;
        ctx.runtime_env()
            .register_object_store(&s3_url, Arc::new(store));

        // Register each TPC-H table as a ListingTable.
        for table in TPCH_TABLES {
            let url = format!(
                "s3://{}/{}/{}/",
                self.config.bucket, self.config.prefix, table
            );
            let listing_url = ListingTableUrl::parse(&url)
                .map_err(|e| EngineError::Connect(format!("listing url {url}: {e}")))?;
            let format = Arc::new(ParquetFormat::default());
            let opts = ListingOptions::new(format).with_file_extension(".parquet");
            let resolved_schema = opts
                .infer_schema(&ctx.state(), &listing_url)
                .await
                .map_err(|e| EngineError::Connect(format!("infer schema {table}: {e}")))?;
            let cfg = ListingTableConfig::new(listing_url)
                .with_listing_options(opts)
                .with_schema(resolved_schema);
            let listing_table = Arc::new(
                ListingTable::try_new(cfg)
                    .map_err(|e| EngineError::Connect(format!("listing table {table}: {e}")))?,
            );
            ctx.register_table(*table, listing_table)
                .map_err(|e| EngineError::Connect(format!("register {table}: {e}")))?;
        }

        self.ctx = Some(ctx);
        Ok(())
    }

    async fn execute(&mut self, sql: &str) -> Result<EngineResult, EngineError> {
        let ctx = self
            .ctx
            .as_ref()
            .ok_or_else(|| EngineError::Connect("datafusion: not connected".into()))?;
        let start = Instant::now();
        let df = ctx
            .sql(sql)
            .await
            .map_err(|e| EngineError::Query(e.to_string()))?;
        let batches = df
            .collect()
            .await
            .map_err(|e| EngineError::Query(e.to_string()))?;
        let elapsed = start.elapsed();

        let mut rows = Vec::new();
        for batch in &batches {
            for r in 0..batch.num_rows() {
                let mut row = Vec::with_capacity(batch.num_columns());
                for c in 0..batch.num_columns() {
                    row.push(array_to_canonical(batch.column(c).as_ref(), r));
                }
                rows.push(row);
            }
        }
        Ok(EngineResult { rows, elapsed })
    }
}

fn array_to_canonical(array: &dyn Array, row: usize) -> CanonicalValue {
    if array.is_null(row) {
        return CanonicalValue::Null;
    }
    match array.data_type() {
        DataType::Boolean => CanonicalValue::Bool(
            array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(row),
        ),
        DataType::Int8 => CanonicalValue::Int(
            array
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .value(row) as i64,
        ),
        DataType::Int16 => CanonicalValue::Int(
            array
                .as_any()
                .downcast_ref::<Int16Array>()
                .unwrap()
                .value(row) as i64,
        ),
        DataType::Int32 => CanonicalValue::Int(
            array
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(row) as i64,
        ),
        DataType::Int64 => CanonicalValue::Int(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(row),
        ),
        DataType::Float32 => CanonicalValue::Float(
            array
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .value(row) as f64,
        ),
        DataType::Float64 => CanonicalValue::Float(
            array
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(row),
        ),
        DataType::Decimal128(_, scale) => {
            let arr = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
            let raw = arr.value(row);
            let scale = *scale as i32;
            let f = (raw as f64) / 10f64.powi(scale);
            CanonicalValue::Float(f)
        }
        DataType::Utf8 => CanonicalValue::Str(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(row)
                .to_string(),
        ),
        DataType::LargeUtf8 => CanonicalValue::Str(
            array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .unwrap()
                .value(row)
                .to_string(),
        ),
        DataType::Date32 => {
            let arr = array.as_any().downcast_ref::<Date32Array>().unwrap();
            let days = arr.value(row);
            // Days since 1970-01-01.
            let date = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
                .unwrap()
                .checked_add_days(chrono::Days::new(days.max(0) as u64))
                .unwrap_or(chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
            CanonicalValue::Timestamp(date.format("%Y-%m-%d").to_string())
        }
        DataType::Timestamp(unit, _tz) => {
            let raw_micros: i64 = match unit {
                TimeUnit::Second => {
                    array
                        .as_any()
                        .downcast_ref::<TimestampSecondArray>()
                        .unwrap()
                        .value(row)
                        * 1_000_000
                }
                TimeUnit::Millisecond => {
                    array
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .unwrap()
                        .value(row)
                        * 1_000
                }
                TimeUnit::Microsecond => array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap()
                    .value(row),
                TimeUnit::Nanosecond => {
                    array
                        .as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .unwrap()
                        .value(row)
                        / 1_000
                }
            };
            let secs = raw_micros / 1_000_000;
            let nanos = ((raw_micros % 1_000_000) * 1_000) as u32;
            let dt = chrono::DateTime::from_timestamp(secs, nanos)
                .unwrap_or_else(|| chrono::DateTime::from_timestamp(0, 0).unwrap());
            CanonicalValue::Timestamp(dt.to_rfc3339())
        }
        _ => {
            // Fallback: render via Arrow's display.
            let s = datafusion::arrow::util::display::array_value_to_string(array, row)
                .unwrap_or_default();
            CanonicalValue::Str(s)
        }
    }
}
