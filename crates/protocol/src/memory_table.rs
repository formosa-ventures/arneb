//! Column-typed row buffers for the virtual metadata tables (Trino's
//! `information_schema` / `system.*` and pgwire's `pg_catalog`), turned into
//! Arrow batches or in-memory tables.

use std::sync::Arc;

use arneb_common::types::{ColumnInfo, DataType};
use arneb_connectors::memory::MemoryTable;
use arrow::array::{ArrayRef, BooleanArray, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{Field, Schema};

/// A cell of a metadata row.
#[derive(Debug, Clone)]
pub(crate) enum Cell {
    Str(Option<String>),
    Int(Option<i64>),
    Bool(Option<bool>),
}

pub(crate) fn s(v: impl Into<String>) -> Cell {
    Cell::Str(Some(v.into()))
}

#[derive(Debug, Clone, Copy)]
enum Kind {
    Str,
    Int,
    Bool,
}

impl Kind {
    fn data_type(self) -> DataType {
        match self {
            Kind::Str => DataType::Utf8,
            Kind::Int => DataType::Int64,
            Kind::Bool => DataType::Boolean,
        }
    }
}

/// Column-typed row buffer that turns into an Arrow batch / memory table.
pub(crate) struct TableBuilder {
    columns: Vec<(&'static str, Kind)>,
    rows: Vec<Vec<Cell>>,
}

impl TableBuilder {
    /// `columns`: names; a trailing `#` marks a bigint column, `?` a boolean
    /// one, and the rest are varchar.
    pub(crate) fn new(columns: &[&'static str]) -> Self {
        Self {
            columns: columns
                .iter()
                .map(|c| {
                    if let Some(name) = c.strip_suffix('#') {
                        (name, Kind::Int)
                    } else if let Some(name) = c.strip_suffix('?') {
                        (name, Kind::Bool)
                    } else {
                        (*c, Kind::Str)
                    }
                })
                .collect(),
            rows: Vec::new(),
        }
    }

    pub(crate) fn push(&mut self, row: Vec<Cell>) {
        debug_assert_eq!(row.len(), self.columns.len());
        self.rows.push(row);
    }

    pub(crate) fn column_infos(&self) -> Vec<ColumnInfo> {
        self.columns
            .iter()
            .map(|(name, kind)| ColumnInfo {
                name: name.to_string(),
                data_type: kind.data_type(),
                nullable: true,
            })
            .collect()
    }

    pub(crate) fn batch(&self) -> RecordBatch {
        let fields: Vec<Field> = self
            .columns
            .iter()
            .map(|(name, kind)| Field::new(*name, kind.data_type().into(), true))
            .collect();
        let arrays: Vec<ArrayRef> = (0..self.columns.len())
            .map(|i| -> ArrayRef {
                let cells = self.rows.iter().map(move |r| &r[i]);
                match self.columns[i].1 {
                    // A number in a varchar column is shown as text.
                    Kind::Str => Arc::new(StringArray::from_iter(cells.map(|c| match c {
                        Cell::Str(v) => v.clone(),
                        Cell::Int(v) => v.map(|n| n.to_string()),
                        Cell::Bool(v) => v.map(|b| b.to_string()),
                    }))),
                    Kind::Int => Arc::new(Int64Array::from_iter(cells.map(|c| match c {
                        Cell::Int(v) => *v,
                        _ => None,
                    }))),
                    Kind::Bool => Arc::new(BooleanArray::from_iter(cells.map(|c| match c {
                        Cell::Bool(v) => *v,
                        _ => None,
                    }))),
                }
            })
            .collect();
        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
            .expect("metadata batch columns match schema")
    }

    pub(crate) fn into_memory_table(self) -> Arc<MemoryTable> {
        let batch = self.batch();
        Arc::new(MemoryTable::new(self.column_infos(), vec![batch]))
    }
}
