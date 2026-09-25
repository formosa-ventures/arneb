//! Iceberg table metadata (`*.metadata.json`) parsing.
//!
//! Supports format versions 1 and 2. Only the subset of the spec needed
//! for current-snapshot reads is modelled: schemas, partition specs,
//! snapshots, and table properties. Unknown fields are ignored so newer
//! writers that add fields keep working.

use std::collections::HashMap;
use std::io::Read;

use serde::Deserialize;
use serde_json::Value;

use arneb_common::error::ConnectorError;
use arneb_common::types::{ColumnInfo, DataType, TimeUnit};

/// Table property holding the default name mapping (JSON) used to resolve
/// columns in data files written without Parquet field IDs (e.g. tables
/// migrated from Hive).
pub const NAME_MAPPING_PROPERTY: &str = "schema.name-mapping.default";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// An Iceberg type, reduced to what Arneb can reason about.
#[derive(Debug, Clone, PartialEq)]
pub enum IcebergType {
    /// `boolean`
    Boolean,
    /// `int` (32-bit)
    Int,
    /// `long` (64-bit)
    Long,
    /// `float`
    Float,
    /// `double`
    Double,
    /// `decimal(P,S)`
    Decimal {
        /// Precision.
        precision: u8,
        /// Scale.
        scale: i8,
    },
    /// `date`
    Date,
    /// `time`
    Time,
    /// `timestamp` / `timestamp_ns` (no zone)
    Timestamp {
        /// `true` for `timestamp_ns`.
        nanos: bool,
    },
    /// `timestamptz` / `timestamptz_ns`
    TimestampTz {
        /// `true` for `timestamptz_ns`.
        nanos: bool,
    },
    /// `string`
    String,
    /// `uuid`
    Uuid,
    /// `fixed[L]`
    Fixed(usize),
    /// `binary`
    Binary,
    /// Nested (`struct` / `list` / `map`) or any type Arneb does not model.
    Unsupported(String),
}

impl IcebergType {
    /// Parse an Iceberg JSON type (either a primitive string or a nested
    /// object).
    pub fn from_json(v: &Value) -> Self {
        match v {
            Value::String(s) => Self::parse_primitive(s),
            Value::Object(o) => {
                let kind = o.get("type").and_then(Value::as_str).unwrap_or("unknown");
                IcebergType::Unsupported(kind.to_string())
            }
            other => IcebergType::Unsupported(other.to_string()),
        }
    }

    fn parse_primitive(s: &str) -> Self {
        let s = s.trim();
        match s {
            "boolean" => IcebergType::Boolean,
            "int" => IcebergType::Int,
            "long" => IcebergType::Long,
            "float" => IcebergType::Float,
            "double" => IcebergType::Double,
            "date" => IcebergType::Date,
            "time" => IcebergType::Time,
            "timestamp" => IcebergType::Timestamp { nanos: false },
            "timestamp_ns" => IcebergType::Timestamp { nanos: true },
            "timestamptz" => IcebergType::TimestampTz { nanos: false },
            "timestamptz_ns" => IcebergType::TimestampTz { nanos: true },
            "string" => IcebergType::String,
            "uuid" => IcebergType::Uuid,
            "binary" => IcebergType::Binary,
            _ => {
                if let Some(inner) = s.strip_prefix("decimal(").and_then(|r| r.strip_suffix(')')) {
                    let mut parts = inner.split(',').map(str::trim);
                    let p = parts.next().and_then(|p| p.parse::<u8>().ok());
                    let sc = parts.next().and_then(|p| p.parse::<i8>().ok());
                    if let (Some(precision), Some(scale)) = (p, sc) {
                        return IcebergType::Decimal { precision, scale };
                    }
                }
                if let Some(len) = s
                    .strip_prefix("fixed[")
                    .and_then(|r| r.strip_suffix(']'))
                    .and_then(|l| l.trim().parse::<usize>().ok())
                {
                    return IcebergType::Fixed(len);
                }
                IcebergType::Unsupported(s.to_string())
            }
        }
    }

    /// Map to an Arneb [`DataType`]. The mapping is chosen to equal what
    /// the Arrow Parquet reader produces for spec-compliant files, so the
    /// common path needs no cast. Returns `None` for types Arneb cannot
    /// represent (nested types, `time`).
    pub fn to_arneb(&self) -> Option<DataType> {
        Some(match self {
            IcebergType::Boolean => DataType::Boolean,
            IcebergType::Int => DataType::Int32,
            IcebergType::Long => DataType::Int64,
            IcebergType::Float => DataType::Float32,
            IcebergType::Double => DataType::Float64,
            IcebergType::Decimal { precision, scale } => DataType::Decimal128 {
                precision: *precision,
                scale: *scale,
            },
            IcebergType::Date => DataType::Date32,
            IcebergType::Timestamp { nanos } => DataType::Timestamp {
                unit: if *nanos {
                    TimeUnit::Nanosecond
                } else {
                    TimeUnit::Microsecond
                },
                timezone: None,
            },
            IcebergType::TimestampTz { nanos } => DataType::Timestamp {
                unit: if *nanos {
                    TimeUnit::Nanosecond
                } else {
                    TimeUnit::Microsecond
                },
                timezone: Some("+00:00".to_string()),
            },
            IcebergType::String => DataType::Utf8,
            IcebergType::Uuid | IcebergType::Fixed(_) | IcebergType::Binary => DataType::Binary,
            IcebergType::Time | IcebergType::Unsupported(_) => return None,
        })
    }
}

/// A top-level field of an Iceberg schema.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct IcebergField {
    /// Stable field ID — the identity of the column across renames.
    pub id: i32,
    /// Current column name.
    pub name: String,
    /// `true` when the column is `required` (non-nullable).
    #[serde(default)]
    pub required: bool,
    /// Field type.
    #[serde(rename = "type", deserialize_with = "de_type")]
    pub field_type: IcebergType,
}

fn de_type<'de, D: serde::Deserializer<'de>>(d: D) -> Result<IcebergType, D::Error> {
    Ok(IcebergType::from_json(&Value::deserialize(d)?))
}

/// An Iceberg schema (only top-level fields are modelled).
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct IcebergSchema {
    /// Schema ID (absent in v1 metadata).
    #[serde(rename = "schema-id")]
    schema_id: Option<i32>,
    /// Top-level fields in declaration order.
    pub fields: Vec<IcebergField>,
}

impl IcebergSchema {
    /// The columns Arneb exposes for this schema, paired with their field
    /// IDs. Columns of unsupported types are skipped (and reported via
    /// the second return value) rather than failing the whole table —
    /// the same policy as the Hive connector.
    pub fn to_columns(&self) -> (Vec<(i32, ColumnInfo)>, Vec<String>) {
        let mut cols = Vec::with_capacity(self.fields.len());
        let mut skipped = Vec::new();
        for f in &self.fields {
            match f.field_type.to_arneb() {
                Some(data_type) => cols.push((
                    f.id,
                    ColumnInfo {
                        name: f.name.clone(),
                        data_type,
                        nullable: !f.required,
                    },
                )),
                None => skipped.push(format!("{} ({:?})", f.name, f.field_type)),
            }
        }
        (cols, skipped)
    }
}

/// A table snapshot.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Snapshot {
    /// Snapshot ID.
    #[serde(rename = "snapshot-id")]
    pub snapshot_id: i64,
    /// Manifest list location. Legacy v1 snapshots that list manifests
    /// inline instead are not supported.
    #[serde(rename = "manifest-list")]
    pub manifest_list: Option<String>,
    /// Snapshot summary (`total-records`, `total-files-size`, ...).
    #[serde(default)]
    pub summary: HashMap<String, String>,
}

impl Snapshot {
    fn summary_u64(&self, key: &str) -> Option<u64> {
        self.summary.get(key).and_then(|v| v.parse().ok())
    }

    /// Total live rows, from the snapshot summary.
    pub fn total_records(&self) -> Option<u64> {
        self.summary_u64("total-records")
    }

    /// Total live data bytes, from the snapshot summary.
    pub fn total_files_size(&self) -> Option<u64> {
        self.summary_u64("total-files-size")
    }
}

/// Parsed Iceberg table metadata (format v1 or v2; v3 best-effort).
#[derive(Debug, Clone)]
pub struct TableMetadata {
    /// The current schema.
    pub current_schema: IcebergSchema,
    /// All snapshots still referenced by the metadata.
    pub snapshots: Vec<Snapshot>,
    /// Current snapshot ID, `None` for a table with no data yet.
    pub current_snapshot_id: Option<i64>,
    /// Table properties.
    pub properties: HashMap<String, String>,
}

/// The metadata JSON as written; unknown fields are ignored.
#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
struct RawMetadata {
    format_version: Option<u64>,
    /// v2: all schemas plus `current-schema-id`.
    #[serde(default)]
    schemas: Vec<IcebergSchema>,
    current_schema_id: Option<i32>,
    /// v1: the single schema.
    schema: Option<IcebergSchema>,
    #[serde(default)]
    snapshots: Vec<Snapshot>,
    current_snapshot_id: Option<i64>,
    #[serde(default)]
    properties: HashMap<String, String>,
}

/// One entry of the `schema.name-mapping.default` property.
#[derive(Deserialize)]
struct NameMappingEntry {
    #[serde(rename = "field-id")]
    field_id: Option<i32>,
    #[serde(default)]
    names: Vec<String>,
}

impl TableMetadata {
    /// Parse metadata JSON bytes. Transparently gunzips
    /// `*.gz.metadata.json` files (detected by the gzip magic bytes).
    pub fn parse(bytes: &[u8]) -> Result<Self, ConnectorError> {
        let owned;
        let json_bytes = if bytes.starts_with(&[0x1f, 0x8b]) {
            let mut out = Vec::new();
            flate2::read::GzDecoder::new(bytes)
                .read_to_end(&mut out)
                .map_err(|e| bad(&format!("gunzip metadata: {e}")))?;
            owned = out;
            &owned[..]
        } else {
            bytes
        };
        let raw: RawMetadata =
            serde_json::from_slice(json_bytes).map_err(|e| bad(&format!("invalid JSON: {e}")))?;

        match raw.format_version {
            None => return Err(bad("missing 'format-version'")),
            Some(v @ (0 | 4..)) => {
                return Err(ConnectorError::UnsupportedOperation(format!(
                    "Iceberg format-version {v} is not supported"
                )))
            }
            Some(_) => {}
        }

        // v2 uses `schemas` + `current-schema-id`; v1 has `schema` (and
        // optionally the v2 fields as well).
        let current_schema = match raw.current_schema_id {
            Some(cur) if !raw.schemas.is_empty() => raw
                .schemas
                .into_iter()
                .find(|s| s.schema_id == Some(cur))
                .ok_or_else(|| bad(&format!("current-schema-id {cur} not in 'schemas'")))?,
            _ => raw
                .schema
                .ok_or_else(|| bad("missing 'schema' / 'schemas'"))?,
        };

        Ok(Self {
            current_schema,
            snapshots: raw.snapshots,
            // `-1` (v1 convention) and `null` both mean "no current snapshot".
            current_snapshot_id: raw.current_snapshot_id.filter(|id| *id != -1),
            properties: raw.properties,
        })
    }

    /// The snapshot with the given ID.
    pub fn snapshot(&self, id: i64) -> Option<&Snapshot> {
        self.snapshots.iter().find(|s| s.snapshot_id == id)
    }

    /// The current snapshot, `None` for an empty table.
    pub fn current_snapshot(&self) -> Option<&Snapshot> {
        self.current_snapshot_id.and_then(|id| self.snapshot(id))
    }

    /// Parse the table's default name mapping, if one is set:
    /// `name -> field id` for top-level fields.
    pub fn name_mapping(&self) -> HashMap<String, i32> {
        let Some(raw) = self.properties.get(NAME_MAPPING_PROPERTY) else {
            return HashMap::new();
        };
        let entries: Vec<NameMappingEntry> = serde_json::from_str(raw).unwrap_or_default();
        entries
            .into_iter()
            .filter_map(|e| Some((e.field_id?, e.names)))
            .flat_map(|(id, names)| names.into_iter().map(move |n| (n, id)))
            .collect()
    }
}

fn bad(msg: &str) -> ConnectorError {
    ConnectorError::ReadError(format!("invalid Iceberg metadata: {msg}"))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// A v2 metadata document shaped like the ones Trino writes, with a
    /// renamed column (field 2 was `n_name`, now `nation_name`) and a
    /// column added after the first snapshot (field 5).
    pub(crate) const V2_METADATA: &str = r#"{
      "format-version": 2,
      "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
      "location": "s3://warehouse/tpch/nation_ice",
      "last-sequence-number": 2,
      "last-updated-ms": 1700000000000,
      "last-column-id": 5,
      "current-schema-id": 1,
      "schemas": [
        {"type": "struct", "schema-id": 0, "fields": [
          {"id": 1, "name": "n_nationkey", "required": false, "type": "long"},
          {"id": 2, "name": "n_name", "required": false, "type": "string"},
          {"id": 3, "name": "n_regionkey", "required": false, "type": "long"},
          {"id": 4, "name": "n_comment", "required": false, "type": "string"}
        ]},
        {"type": "struct", "schema-id": 1, "fields": [
          {"id": 1, "name": "n_nationkey", "required": true, "type": "long"},
          {"id": 2, "name": "nation_name", "required": false, "type": "string"},
          {"id": 3, "name": "n_regionkey", "required": false, "type": "long"},
          {"id": 4, "name": "n_comment", "required": false, "type": "string"},
          {"id": 5, "name": "added_at", "required": false, "type": "timestamptz"},
          {"id": 6, "name": "price", "required": false, "type": "decimal(12, 2)"},
          {"id": 7, "name": "tags", "required": false,
           "type": {"type": "list", "element-id": 8, "element": "string", "element-required": false}}
        ]}
      ],
      "default-spec-id": 1,
      "partition-specs": [
        {"spec-id": 0, "fields": []},
        {"spec-id": 1, "fields": [
          {"name": "n_regionkey", "transform": "identity", "source-id": 3, "field-id": 1000}
        ]}
      ],
      "last-partition-id": 1000,
      "default-sort-order-id": 0,
      "sort-orders": [{"order-id": 0, "fields": []}],
      "properties": {"write.format.default": "PARQUET"},
      "current-snapshot-id": 222,
      "snapshots": [
        {"snapshot-id": 111, "sequence-number": 1, "timestamp-ms": 1,
         "manifest-list": "s3://warehouse/tpch/nation_ice/metadata/snap-111.avro",
         "summary": {"operation": "append", "total-records": "20"}, "schema-id": 0},
        {"snapshot-id": 222, "parent-snapshot-id": 111, "sequence-number": 2, "timestamp-ms": 2,
         "manifest-list": "s3://warehouse/tpch/nation_ice/metadata/snap-222.avro",
         "summary": {"operation": "append", "total-records": "25", "total-files-size": "4096",
                     "total-delete-files": "0"}, "schema-id": 1}
      ]
    }"#;

    #[test]
    fn parse_v2_metadata() {
        let md = TableMetadata::parse(V2_METADATA.as_bytes()).unwrap();
        assert_eq!(md.current_schema.schema_id, Some(1));
        assert_eq!(md.current_schema.fields.len(), 7);
        assert_eq!(md.current_schema.fields[1].name, "nation_name");
        assert_eq!(md.current_schema.fields[1].id, 2);
        assert_eq!(
            md.current_schema.fields[5].field_type,
            IcebergType::Decimal {
                precision: 12,
                scale: 2
            }
        );
        let snap = md.current_snapshot().unwrap();
        assert_eq!(snap.snapshot_id, 222);
        assert_eq!(
            snap.manifest_list.as_deref(),
            Some("s3://warehouse/tpch/nation_ice/metadata/snap-222.avro")
        );
        assert_eq!(snap.total_records(), Some(25));
        assert_eq!(snap.total_files_size(), Some(4096));
    }

    #[test]
    fn schema_to_columns_maps_types_and_skips_nested() {
        let md = TableMetadata::parse(V2_METADATA.as_bytes()).unwrap();
        let (cols, skipped) = md.current_schema.to_columns();
        let names: Vec<_> = cols.iter().map(|(_, c)| c.name.as_str()).collect();
        assert_eq!(
            names,
            [
                "n_nationkey",
                "nation_name",
                "n_regionkey",
                "n_comment",
                "added_at",
                "price"
            ]
        );
        assert_eq!(cols[0].0, 1);
        assert_eq!(cols[0].1.data_type, DataType::Int64);
        assert!(!cols[0].1.nullable, "required field is non-nullable");
        assert_eq!(cols[1].1.data_type, DataType::Utf8);
        assert_eq!(
            cols[4].1.data_type,
            DataType::Timestamp {
                unit: TimeUnit::Microsecond,
                timezone: Some("+00:00".into())
            }
        );
        assert_eq!(
            cols[5].1.data_type,
            DataType::Decimal128 {
                precision: 12,
                scale: 2
            }
        );
        assert_eq!(skipped.len(), 1);
        assert!(skipped[0].starts_with("tags"));
    }

    #[test]
    fn parse_v1_metadata_without_snapshot() {
        let v1 = r#"{
          "format-version": 1,
          "table-uuid": "d20125c8-7284-442c-9aea-15fee620737c",
          "location": "/tmp/warehouse/t",
          "last-updated-ms": 1,
          "last-column-id": 2,
          "schema": {"type": "struct", "fields": [
            {"id": 1, "name": "id", "required": true, "type": "int"},
            {"id": 2, "name": "d", "required": false, "type": "date"}
          ]},
          "partition-spec": [{"name": "d", "transform": "identity", "source-id": 2, "field-id": 1000}],
          "properties": {},
          "current-snapshot-id": -1,
          "snapshots": []
        }"#;
        let md = TableMetadata::parse(v1.as_bytes()).unwrap();
        assert!(md.current_snapshot().is_none());
        assert_eq!(md.current_schema.fields.len(), 2);
        let (cols, _) = md.current_schema.to_columns();
        assert_eq!(cols[1].1.data_type, DataType::Date32);
    }

    #[test]
    fn parse_gzipped_metadata() {
        use std::io::Write;
        let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        enc.write_all(V2_METADATA.as_bytes()).unwrap();
        let gz = enc.finish().unwrap();
        let md = TableMetadata::parse(&gz).unwrap();
        assert_eq!(md.current_snapshot_id, Some(222));
    }

    #[test]
    fn reject_unknown_format_version() {
        let err = TableMetadata::parse(br#"{"format-version": 9}"#).unwrap_err();
        assert!(err.to_string().contains("format-version 9"));
    }

    #[test]
    fn primitive_type_parsing() {
        use IcebergType as T;
        let cases = [
            ("boolean", T::Boolean),
            ("int", T::Int),
            ("long", T::Long),
            ("float", T::Float),
            ("double", T::Double),
            ("date", T::Date),
            ("time", T::Time),
            ("timestamp", T::Timestamp { nanos: false }),
            ("timestamptz_ns", T::TimestampTz { nanos: true }),
            ("string", T::String),
            ("uuid", T::Uuid),
            ("binary", T::Binary),
            ("fixed[16]", T::Fixed(16)),
            (
                "decimal(38,10)",
                T::Decimal {
                    precision: 38,
                    scale: 10,
                },
            ),
        ];
        for (s, expected) in cases {
            assert_eq!(IcebergType::from_json(&Value::String(s.into())), expected);
        }
        assert!(IcebergType::Time.to_arneb().is_none());
        assert_eq!(IcebergType::Uuid.to_arneb(), Some(DataType::Binary));
    }

    #[test]
    fn name_mapping_parsed() {
        let mut md = TableMetadata::parse(V2_METADATA.as_bytes()).unwrap();
        md.properties.insert(
            NAME_MAPPING_PROPERTY.into(),
            r#"[{"field-id": 1, "names": ["id", "ID"]}, {"field-id": 2, "names": ["name"]}]"#
                .into(),
        );
        let m = md.name_mapping();
        assert_eq!(m.get("id"), Some(&1));
        assert_eq!(m.get("ID"), Some(&1));
        assert_eq!(m.get("name"), Some(&2));
    }
}
