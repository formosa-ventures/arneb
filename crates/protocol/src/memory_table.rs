//! Column-typed row buffers for the virtual metadata tables (Trino's
//! `information_schema` / `system.*` and pgwire's `pg_catalog`), turned into
//! Arrow batches or in-memory tables.

use std::sync::Arc;

use arneb_common::types::{ColumnInfo, DataType};
use arneb_connectors::memory::MemoryTable;
use arrow::array::{
    ArrayRef, BooleanArray, Int64Array, Int64Builder, ListBuilder, RecordBatch, StringArray,
    StringBuilder,
};
use arrow::datatypes::{Field, Schema};

/// A cell of a metadata row.
#[derive(Debug, Clone)]
pub(crate) enum Cell {
    Str(Option<String>),
    Int(Option<i64>),
    Bool(Option<bool>),
}

/// A NULL cell, for a column of any kind (an array column holds only NULLs).
pub(crate) const NULL: Cell = Cell::Str(None);

pub(crate) fn s(v: impl Into<String>) -> Cell {
    Cell::Str(Some(v.into()))
}

#[derive(Debug, Clone, Copy)]
enum Kind {
    Str,
    Int,
    Bool,
    StrList,
    IntList,
}

impl Kind {
    fn data_type(self) -> DataType {
        match self {
            Kind::Str => DataType::Utf8,
            Kind::Int => DataType::Int64,
            Kind::Bool => DataType::Boolean,
            Kind::StrList => DataType::List(Box::new(DataType::Utf8)),
            Kind::IntList => DataType::List(Box::new(DataType::Int64)),
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
    /// one, and the rest are varchar. A `[]` before that marker (`indkey[]#`,
    /// `reloptions[]`) makes the column an array of that type; booleans have
    /// no array form here.
    pub(crate) fn new(columns: &[&'static str]) -> Self {
        Self {
            columns: columns
                .iter()
                .map(|c| {
                    let (rest, marker) = match c.as_bytes().last() {
                        Some(b'#') => (&c[..c.len() - 1], Some('#')),
                        Some(b'?') => (&c[..c.len() - 1], Some('?')),
                        _ => (*c, None),
                    };
                    match (rest.strip_suffix("[]"), marker) {
                        (Some(name), Some('#')) => (name, Kind::IntList),
                        (Some(name), None) => (name, Kind::StrList),
                        (Some(_), Some(_)) => panic!("no array form for column spec {c}"),
                        (None, Some('#')) => (rest, Kind::Int),
                        (None, Some(_)) => (rest, Kind::Bool),
                        (None, None) => (rest, Kind::Str),
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
                    // Array columns hold NULL (no catalog row has an array
                    // value yet), so the batch carries the right type.
                    Kind::StrList => {
                        let mut builder = ListBuilder::new(StringBuilder::new());
                        cells.for_each(|_| builder.append(false));
                        Arc::new(builder.finish())
                    }
                    Kind::IntList => {
                        let mut builder = ListBuilder::new(Int64Builder::new());
                        cells.for_each(|_| builder.append(false));
                        Arc::new(builder.finish())
                    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn column_specs_combine_array_and_type_markers() {
        let mut t = TableBuilder::new(&["a", "b#", "c?", "d[]", "e[]#"]);
        t.push(vec![NULL, NULL, NULL, NULL, NULL]);
        let types: Vec<DataType> = t.column_infos().into_iter().map(|c| c.data_type).collect();
        assert_eq!(
            types,
            [
                DataType::Utf8,
                DataType::Int64,
                DataType::Boolean,
                DataType::List(Box::new(DataType::Utf8)),
                DataType::List(Box::new(DataType::Int64)),
            ]
        );
        let batch = t.batch();
        assert_eq!(batch.num_rows(), 1);
        assert!(batch.columns().iter().all(|c| c.is_null(0)));
    }
}
