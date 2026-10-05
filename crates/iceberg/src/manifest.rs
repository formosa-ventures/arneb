//! Iceberg manifest list and manifest (Avro) decoding.
//!
//! Values are read with Avro's *writer* schema into generic
//! [`apache_avro::types::Value`]s and fields are extracted by name, which
//! keeps the reader tolerant of v1 vs v2 layout differences and of
//! optional fields that some writers omit.

use apache_avro::types::Value;
use apache_avro::Reader;

use arneb_common::error::ConnectorError;

/// Whether a manifest tracks data files or delete files.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManifestContent {
    /// Data files (`content = 0`, and every v1 manifest).
    Data,
    /// Position / equality delete files (`content = 1`).
    Deletes,
}

/// One entry of a manifest list.
#[derive(Debug, Clone, PartialEq)]
pub struct ManifestFile {
    /// Location of the manifest file.
    pub manifest_path: String,
    /// Data or deletes.
    pub content: ManifestContent,
    /// Number of `ADDED` entries, when recorded.
    pub added_files_count: Option<i64>,
    /// Number of `EXISTING` entries, when recorded.
    pub existing_files_count: Option<i64>,
}

impl ManifestFile {
    /// `true` when the manifest provably has no live (added/existing)
    /// entries, based on the counts in the manifest list.
    pub fn provably_empty(&self) -> bool {
        matches!(
            (self.added_files_count, self.existing_files_count),
            (Some(0), Some(0))
        )
    }
}

/// `data_file.content` values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataFileContent {
    /// Regular data file.
    Data,
    /// Position delete file.
    PositionDeletes,
    /// Equality delete file.
    EqualityDeletes,
}

/// Manifest entry status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryStatus {
    /// Unchanged file carried over from an earlier snapshot.
    Existing,
    /// File added by the snapshot that wrote the manifest.
    Added,
    /// File removed by the snapshot that wrote the manifest (tombstone).
    Deleted,
}

/// A data (or delete) file described by a manifest entry.
#[derive(Debug, Clone, PartialEq)]
pub struct DataFile {
    /// Data or delete content.
    pub content: DataFileContent,
    /// Full URI of the file.
    pub file_path: String,
    /// File format (`PARQUET`, `ORC`, `AVRO`), upper-cased.
    pub file_format: String,
}

/// One manifest entry.
#[derive(Debug, Clone, PartialEq)]
pub struct ManifestEntry {
    /// Entry status.
    pub status: EntryStatus,
    /// The file.
    pub data_file: DataFile,
}

// ---------------------------------------------------------------------------
// Decoding
// ---------------------------------------------------------------------------

fn bad(what: &str, msg: impl std::fmt::Display) -> ConnectorError {
    ConnectorError::ReadError(format!("invalid Iceberg {what}: {msg}"))
}

/// Strip an Avro `union` wrapper (optional fields are `["null", T]`).
fn unwrap_union(v: &Value) -> &Value {
    match v {
        Value::Union(_, inner) => unwrap_union(inner),
        other => other,
    }
}

fn field<'a>(record: &'a [(String, Value)], name: &str) -> Option<&'a Value> {
    record
        .iter()
        .find(|(k, _)| k == name)
        .map(|(_, v)| unwrap_union(v))
        .filter(|v| !matches!(v, Value::Null))
}

fn as_i64(v: &Value) -> Option<i64> {
    match v {
        Value::Int(i) => Some(*i as i64),
        Value::Long(l) => Some(*l),
        _ => None,
    }
}

fn as_string(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Bytes(b) => String::from_utf8(b.clone()).ok(),
        Value::Enum(_, s) => Some(s.clone()),
        _ => None,
    }
}

fn read_records(bytes: &[u8], what: &str) -> Result<Vec<Vec<(String, Value)>>, ConnectorError> {
    let reader = Reader::new(bytes).map_err(|e| bad(what, e))?;
    let mut out = Vec::new();
    for value in reader {
        match value.map_err(|e| bad(what, e))? {
            Value::Record(fields) => out.push(fields),
            other => return Err(bad(what, format!("expected record, got {other:?}"))),
        }
    }
    Ok(out)
}

/// Decode a manifest list (Avro object container file).
pub fn read_manifest_list(bytes: &[u8]) -> Result<Vec<ManifestFile>, ConnectorError> {
    const WHAT: &str = "manifest list";
    read_records(bytes, WHAT)?
        .into_iter()
        .map(|r| {
            let manifest_path = field(&r, "manifest_path")
                .and_then(as_string)
                .ok_or_else(|| bad(WHAT, "entry missing manifest_path"))?;
            // v1 manifest lists have no `content` column: always data.
            let content = match field(&r, "content").and_then(as_i64) {
                None | Some(0) => ManifestContent::Data,
                Some(1) => ManifestContent::Deletes,
                Some(other) => return Err(bad(WHAT, format!("unknown manifest content {other}"))),
            };
            // v2 names first, then the v1 spellings.
            let added_files_count = field(&r, "added_files_count")
                .or_else(|| field(&r, "added_data_files_count"))
                .and_then(as_i64);
            let existing_files_count = field(&r, "existing_files_count")
                .or_else(|| field(&r, "existing_data_files_count"))
                .and_then(as_i64);
            Ok(ManifestFile {
                manifest_path,
                content,
                added_files_count,
                existing_files_count,
            })
        })
        .collect()
}

/// Decode a manifest file (Avro object container file).
pub fn read_manifest(bytes: &[u8]) -> Result<Vec<ManifestEntry>, ConnectorError> {
    const WHAT: &str = "manifest";
    read_records(bytes, WHAT)?
        .into_iter()
        .map(|r| {
            let status = match field(&r, "status").and_then(as_i64) {
                Some(0) => EntryStatus::Existing,
                Some(1) => EntryStatus::Added,
                Some(2) => EntryStatus::Deleted,
                other => return Err(bad(WHAT, format!("bad entry status {other:?}"))),
            };
            let Some(Value::Record(df)) = field(&r, "data_file") else {
                return Err(bad(WHAT, "entry missing data_file"));
            };
            let content = match field(df, "content").and_then(as_i64) {
                None | Some(0) => DataFileContent::Data,
                Some(1) => DataFileContent::PositionDeletes,
                Some(2) => DataFileContent::EqualityDeletes,
                Some(other) => return Err(bad(WHAT, format!("unknown file content {other}"))),
            };
            let file_path = field(df, "file_path")
                .and_then(as_string)
                .ok_or_else(|| bad(WHAT, "data_file missing file_path"))?;
            let file_format = field(df, "file_format")
                .and_then(as_string)
                .unwrap_or_else(|| "PARQUET".to_string())
                .to_ascii_uppercase();
            Ok(ManifestEntry {
                status,
                data_file: DataFile {
                    content,
                    file_path,
                    file_format,
                },
            })
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Test fixtures (shared with other modules' tests)
// ---------------------------------------------------------------------------

#[cfg(test)]
pub(crate) mod fixtures {
    use apache_avro::types::Value;
    use apache_avro::{Codec, DeflateSettings, Schema, Writer};

    /// v2 manifest list schema (trimmed to the fields Iceberg marks
    /// required plus the counts).
    pub const MANIFEST_LIST_SCHEMA: &str = r#"{
      "type": "record", "name": "manifest_file", "fields": [
        {"name": "manifest_path", "type": "string", "field-id": 500},
        {"name": "manifest_length", "type": "long", "field-id": 501},
        {"name": "partition_spec_id", "type": "int", "field-id": 502},
        {"name": "content", "type": "int", "field-id": 517},
        {"name": "sequence_number", "type": "long", "field-id": 515},
        {"name": "min_sequence_number", "type": "long", "field-id": 516},
        {"name": "added_snapshot_id", "type": "long", "field-id": 503},
        {"name": "added_files_count", "type": "int", "field-id": 504},
        {"name": "existing_files_count", "type": "int", "field-id": 505},
        {"name": "deleted_files_count", "type": "int", "field-id": 506}
      ]}"#;

    /// v2 manifest entry schema (trimmed to the fields the reader uses).
    pub const MANIFEST_SCHEMA: &str = r#"{
      "type": "record", "name": "manifest_entry", "fields": [
        {"name": "status", "type": "int", "field-id": 0},
        {"name": "snapshot_id", "type": ["null", "long"], "default": null, "field-id": 1},
        {"name": "data_file", "field-id": 2, "type": {
          "type": "record", "name": "r2", "fields": [
            {"name": "content", "type": "int", "field-id": 134},
            {"name": "file_path", "type": "string", "field-id": 100},
            {"name": "file_format", "type": "string", "field-id": 101},
            {"name": "record_count", "type": "long", "field-id": 103}
          ]}}
      ]}"#;

    /// A manifest-list row.
    pub fn manifest_list_row(path: &str, content: i32, added: i32, existing: i32) -> Value {
        Value::Record(vec![
            ("manifest_path".into(), Value::String(path.into())),
            ("manifest_length".into(), Value::Long(100)),
            ("partition_spec_id".into(), Value::Int(0)),
            ("content".into(), Value::Int(content)),
            ("sequence_number".into(), Value::Long(1)),
            ("min_sequence_number".into(), Value::Long(1)),
            ("added_snapshot_id".into(), Value::Long(1)),
            ("added_files_count".into(), Value::Int(added)),
            ("existing_files_count".into(), Value::Int(existing)),
            ("deleted_files_count".into(), Value::Int(0)),
        ])
    }

    /// Description of one manifest entry for [`manifest_bytes`].
    pub struct Entry<'a> {
        /// 0 existing, 1 added, 2 deleted.
        pub status: i32,
        /// 0 data, 1 position deletes, 2 equality deletes.
        pub content: i32,
        /// File URI.
        pub path: &'a str,
    }

    /// A manifest-entry row.
    pub fn manifest_row(e: &Entry<'_>) -> Value {
        Value::Record(vec![
            ("status".into(), Value::Int(e.status)),
            (
                "snapshot_id".into(),
                Value::Union(1, Box::new(Value::Long(1))),
            ),
            (
                "data_file".into(),
                Value::Record(vec![
                    ("content".into(), Value::Int(e.content)),
                    ("file_path".into(), Value::String(e.path.into())),
                    ("file_format".into(), Value::String("PARQUET".into())),
                    ("record_count".into(), Value::Long(1)),
                ]),
            ),
        ])
    }

    /// Serialize rows into an Avro object container file (deflate, as
    /// Iceberg writers do by default).
    pub fn avro_file(schema: &str, rows: Vec<Value>) -> Vec<u8> {
        let schema = Schema::parse_str(schema).unwrap();
        let mut w = Writer::builder()
            .schema(&schema)
            .writer(Vec::new())
            .codec(Codec::Deflate(DeflateSettings::default()))
            .build()
            .unwrap();
        for r in rows {
            w.append_value(r).unwrap();
        }
        w.into_inner().unwrap()
    }

    /// Convenience: a manifest file from entries.
    pub fn manifest_bytes(entries: &[Entry<'_>]) -> Vec<u8> {
        avro_file(MANIFEST_SCHEMA, entries.iter().map(manifest_row).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::fixtures::*;
    use super::*;

    #[test]
    fn decode_manifest_list() {
        let bytes = avro_file(
            MANIFEST_LIST_SCHEMA,
            vec![
                manifest_list_row("s3://b/t/metadata/m1.avro", 0, 2, 1),
                manifest_list_row("s3://b/t/metadata/m2.avro", 1, 1, 0),
            ],
        );
        let list = read_manifest_list(&bytes).unwrap();
        assert_eq!(list.len(), 2);
        assert_eq!(list[0].manifest_path, "s3://b/t/metadata/m1.avro");
        assert_eq!(list[0].content, ManifestContent::Data);
        assert_eq!(list[0].added_files_count, Some(2));
        assert!(!list[0].provably_empty());
        assert_eq!(list[1].content, ManifestContent::Deletes);
    }

    #[test]
    fn decode_v1_manifest_list_without_content() {
        let schema = r#"{"type": "record", "name": "manifest_file", "fields": [
            {"name": "manifest_path", "type": "string"},
            {"name": "manifest_length", "type": "long"},
            {"name": "partition_spec_id", "type": "int"},
            {"name": "added_snapshot_id", "type": ["null", "long"], "default": null},
            {"name": "added_data_files_count", "type": ["null", "int"], "default": null},
            {"name": "existing_data_files_count", "type": ["null", "int"], "default": null}
        ]}"#;
        let row = Value::Record(vec![
            ("manifest_path".into(), Value::String("/t/m.avro".into())),
            ("manifest_length".into(), Value::Long(1)),
            ("partition_spec_id".into(), Value::Int(0)),
            (
                "added_snapshot_id".into(),
                Value::Union(0, Box::new(Value::Null)),
            ),
            (
                "added_data_files_count".into(),
                Value::Union(1, Box::new(Value::Int(0))),
            ),
            (
                "existing_data_files_count".into(),
                Value::Union(1, Box::new(Value::Int(0))),
            ),
        ]);
        let list = read_manifest_list(&avro_file(schema, vec![row])).unwrap();
        assert_eq!(list[0].content, ManifestContent::Data);
        assert!(list[0].provably_empty());
    }

    #[test]
    fn decode_manifest_entries() {
        let bytes = manifest_bytes(&[
            Entry {
                status: 1,
                content: 0,
                path: "s3://b/t/data/a.parquet",
            },
            Entry {
                status: 2,
                content: 1,
                path: "s3://b/t/data/old.parquet",
            },
        ]);
        let entries = read_manifest(&bytes).unwrap();
        assert_eq!(entries.len(), 2);
        let e = &entries[0];
        assert_eq!(e.status, EntryStatus::Added);
        assert_eq!(e.data_file.content, DataFileContent::Data);
        assert_eq!(e.data_file.file_path, "s3://b/t/data/a.parquet");
        assert_eq!(e.data_file.file_format, "PARQUET");
        assert_eq!(entries[1].status, EntryStatus::Deleted);
        assert_eq!(
            entries[1].data_file.content,
            DataFileContent::PositionDeletes
        );
    }

    #[test]
    fn corrupt_avro_is_an_error() {
        let err = read_manifest(b"not avro").unwrap_err();
        assert!(err.to_string().contains("invalid Iceberg manifest"));
    }
}
