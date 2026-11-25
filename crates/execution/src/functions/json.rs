//! `json_build_object`, as PostgreSQL clients (SQLAlchemy's identity and
//! collation reflection) call it. Arneb has no JSON type, so the object
//! comes back as text in PostgreSQL's `json` output format
//! (`{"a" : 1, "b" : "x"}`); a nested object is therefore a string.

use std::fmt::Write;
use std::sync::Arc;

use arneb_common::error::ExecutionError;
use arneb_common::types::DataType;
use arrow::array::{Array, ArrayRef, StringArray};
use arrow::datatypes::DataType as ArrowDataType;

use super::args::{invalid, utf8};
use super::registry::ScalarFunction;

pub(crate) fn all_json_functions() -> Vec<Arc<dyn ScalarFunction>> {
    vec![Arc::new(JsonBuildObject)]
}

/// `json_build_object(key, value, ...)`: the pairs as a JSON object in
/// text. An odd argument count is refused (at plan time by the planner,
/// again here); a NULL key is an error; a NULL value is `null`.
#[derive(Debug)]
struct JsonBuildObject;

impl ScalarFunction for JsonBuildObject {
    fn name(&self) -> &str {
        "JSON_BUILD_OBJECT"
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(DataType::Utf8)
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        // Nullary calls have no array to take the row count from.
        self.invoke(args, 0)
    }
    fn invoke(&self, args: &[ArrayRef], num_rows: usize) -> Result<ArrayRef, ExecutionError> {
        if !args.len().is_multiple_of(2) {
            return Err(invalid(
                "json_build_object requires an even number of arguments",
            ));
        }
        let rows = args.first().map_or(num_rows, |a| a.len());
        let keys: Vec<StringArray> = args
            .iter()
            .step_by(2)
            .map(|a| utf8(a, "JSON_BUILD_OBJECT"))
            .collect::<Result<_, _>>()?;
        let values: Vec<JsonColumn> = args
            .iter()
            .skip(1)
            .step_by(2)
            .map(JsonColumn::new)
            .collect::<Result<_, _>>()?;
        let mut out = Vec::with_capacity(rows);
        for row in 0..rows {
            let mut text = String::from("{");
            for (n, (key, value)) in keys.iter().zip(&values).enumerate() {
                if key.is_null(row) {
                    return Err(invalid("null value not allowed for object key"));
                }
                if n > 0 {
                    text.push_str(", ");
                }
                write_json_string(&mut text, key.value(row));
                text.push_str(" : ");
                value.write(&mut text, row);
            }
            text.push('}');
            out.push(text);
        }
        Ok(Arc::new(StringArray::from(out)))
    }
}

/// One value column, as the text of each row and whether it is quoted.
struct JsonColumn {
    text: StringArray,
    kind: Kind,
}

enum Kind {
    /// Always `null`.
    Null,
    /// Numbers and booleans: the text, unquoted (NaN and infinities are
    /// quoted, as PostgreSQL does).
    Bare,
    /// Everything else: a quoted, escaped string.
    Quoted,
}

impl JsonColumn {
    fn new(array: &ArrayRef) -> Result<Self, ExecutionError> {
        use ArrowDataType as T;
        let kind = match array.data_type() {
            T::Null => Kind::Null,
            T::Boolean
            | T::Int8
            | T::Int16
            | T::Int32
            | T::Int64
            | T::UInt8
            | T::UInt16
            | T::UInt32
            | T::UInt64
            | T::Float32
            | T::Float64
            | T::Decimal128(..) => Kind::Bare,
            _ => Kind::Quoted,
        };
        let text = utf8(array, "JSON_BUILD_OBJECT")?;
        Ok(Self { text, kind })
    }

    fn write(&self, out: &mut String, row: usize) {
        if self.text.is_null(row) || matches!(self.kind, Kind::Null) {
            out.push_str("null");
            return;
        }
        let text = self.text.value(row);
        let finite = !matches!(text, "NaN" | "inf" | "-inf" | "Infinity" | "-Infinity");
        if matches!(self.kind, Kind::Bare) && finite {
            out.push_str(text);
        } else {
            write_json_string(out, text);
        }
    }
}

/// `s` as a JSON string literal, escaped as PostgreSQL's `json` does.
fn write_json_string(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{BooleanArray, Float64Array, Int64Array, NullArray};

    fn build(args: Vec<ArrayRef>) -> Vec<String> {
        let out = JsonBuildObject.invoke(&args, 2).unwrap();
        let out = out.as_any().downcast_ref::<StringArray>().unwrap();
        (0..out.len()).map(|i| out.value(i).to_string()).collect()
    }

    #[test]
    fn encodes_each_type_with_postgres_spacing() {
        let out = build(vec![
            Arc::new(StringArray::from(vec!["a", "a"])),
            Arc::new(Int64Array::from(vec![Some(1), None])),
            Arc::new(StringArray::from(vec!["b", "b"])),
            Arc::new(BooleanArray::from(vec![true, false])),
            Arc::new(StringArray::from(vec!["c", "c"])),
            Arc::new(NullArray::new(2)),
            Arc::new(StringArray::from(vec!["d", "d"])),
            Arc::new(StringArray::from(vec!["x\"y\\z\n", "plain"])),
            Arc::new(StringArray::from(vec!["f", "f"])),
            Arc::new(Float64Array::from(vec![1.5, f64::NAN])),
        ]);
        assert_eq!(
            out[0],
            r#"{"a" : 1, "b" : true, "c" : null, "d" : "x\"y\\z\n", "f" : 1.5}"#
        );
        assert_eq!(
            out[1],
            r#"{"a" : null, "b" : false, "c" : null, "d" : "plain", "f" : "NaN"}"#
        );
    }

    #[test]
    fn empty_object_and_bad_arguments() {
        assert_eq!(build(vec![]), ["{}", "{}"]);
        let odd = JsonBuildObject.invoke(&[Arc::new(StringArray::from(vec!["a"]))], 1);
        assert!(odd.unwrap_err().to_string().contains("even number"));
        let null_key = JsonBuildObject.invoke(
            &[
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(Int64Array::from(vec![1])),
            ],
            1,
        );
        assert!(null_key.unwrap_err().to_string().contains("object key"));
    }
}
