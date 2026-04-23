//! Rustc-style error diagnostic rendering.
//!
//! This module wraps `codespan-reporting` to produce the compiler-style
//! diagnostic output that users see at the pgwire/CLI boundary:
//!
//! ```text
//! error: cannot apply operator '<=' to Utf8 and Date32
//!   ┌─ query.sql:3:19
//!   │
//! 3 │   WHERE l_shipdate <= DATE '1998-12-01'
//!   │                    ^^ here
//! ```
//!
//! # When to call [`render_plan_error`]
//!
//! Call this when you have both an error and access to the original SQL
//! text (i.e. at the boundary that received the query). Inside library
//! code, prefer [`Display`] / `err.to_string()` — it emits the
//! position-free message and never touches source text. The renderer is
//! a presentation concern for the code path that talks to the user.

use std::ops::Range;

use codespan_reporting::diagnostic::{Diagnostic, Label};
use codespan_reporting::files::SimpleFile;
use codespan_reporting::term::{self, termcolor::Buffer, Config};

use crate::error::{Location, PlanError};

/// A named chunk of SQL source that diagnostics can point into.
///
/// `name` is shown in the `file:line:col` marker. Use `"<query>"` (or
/// similar placeholder) for ad-hoc client-submitted SQL; use a file path
/// for queries loaded from disk.
pub struct SourceFile {
    /// Logical name of the source (e.g. `"<query>"` or a file path).
    pub name: String,
    /// The SQL text itself.
    pub text: String,
}

impl SourceFile {
    /// Convenience constructor.
    pub fn new(name: impl Into<String>, text: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            text: text.into(),
        }
    }
}

/// Render a [`PlanError`] as a multi-line diagnostic with source
/// context, matching the rustc/clang style.
///
/// If the error has no location (e.g. `PlanError::TableNotFound`) the
/// output is a short `error: <message>` header with no snippet. If the
/// location is present but lies outside the source text, the renderer
/// falls back to a location-prefixed message.
pub fn render_plan_error(err: &PlanError, source: &SourceFile) -> String {
    let file = SimpleFile::new(source.name.as_str(), source.text.as_str());
    let message = err.to_string();

    let diag = match err.location() {
        Some(loc) => {
            match location_to_byte_range(&source.text, loc) {
                Some(range) => Diagnostic::error()
                    .with_message(&message)
                    .with_labels(vec![Label::primary((), range).with_message("here")]),
                None => {
                    // Location points outside the text — still emit the
                    // line:col prefix the user expects, without a snippet.
                    return format!("error: line {}:{}: {message}", loc.line, loc.column);
                }
            }
        }
        None => Diagnostic::error().with_message(&message),
    };

    let mut buffer = Buffer::no_color();
    let config = Config::default();
    if term::emit(&mut buffer, &config, &file, &diag).is_err() {
        // Fall back to plain display on any rendering error.
        return format!("error: {message}");
    }
    String::from_utf8_lossy(buffer.as_slice()).into_owned()
}

/// Convert a 1-based `Location` into a byte range suitable for
/// `codespan-reporting`. Returns `None` if the location points past the
/// end of the source. The returned range is a zero-width span; the
/// renderer draws the caret at that exact column.
fn location_to_byte_range(text: &str, loc: Location) -> Option<Range<usize>> {
    if loc.line == 0 || loc.column == 0 {
        return None;
    }
    let mut current_line: u64 = 1;
    let mut line_start = 0usize;
    for (i, ch) in text.char_indices() {
        if current_line == loc.line {
            // Count columns by characters within the current line.
            let col_byte_offset = line_byte_for_column(&text[line_start..], loc.column)?;
            let pos = line_start + col_byte_offset;
            return Some(pos..pos);
        }
        if ch == '\n' {
            current_line += 1;
            line_start = i + ch.len_utf8();
        }
    }
    if current_line == loc.line {
        let col_byte_offset = line_byte_for_column(&text[line_start..], loc.column)?;
        let pos = line_start + col_byte_offset;
        return Some(pos..pos);
    }
    None
}

/// Walk `column-1` characters into `line_text` and return the byte
/// offset. `None` if the line is too short.
fn line_byte_for_column(line_text: &str, column: u64) -> Option<usize> {
    let target = (column - 1) as usize;
    let mut consumed = 0usize;
    for (i, ch) in line_text.char_indices() {
        if consumed == target {
            return Some(i);
        }
        if ch == '\n' {
            return None;
        }
        consumed += 1;
    }
    if consumed == target {
        Some(line_text.len())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Location;
    use crate::types::DataType;

    #[test]
    fn render_type_mismatch_with_caret() {
        let source = SourceFile::new(
            "query.sql",
            "SELECT *\nFROM t\nWHERE l_shipdate <= DATE '1998-12-01'",
        );
        let err = PlanError::TypeMismatch {
            expected: DataType::Date32,
            found: DataType::Utf8,
            location: Some(Location {
                line: 3,
                column: 19,
            }),
        };
        let out = render_plan_error(&err, &source);
        assert!(out.contains("query.sql:3:19"), "got: {out}");
        assert!(
            out.contains("WHERE l_shipdate <= DATE '1998-12-01'"),
            "got: {out}"
        );
        // codespan draws a caret `^` under the labelled position.
        assert!(out.contains('^'), "got: {out}");
    }

    #[test]
    fn render_column_not_found_with_caret() {
        let source = SourceFile::new("query.sql", "SELECT unknown_col FROM users");
        let err = PlanError::ColumnNotFound {
            name: "unknown_col".to_string(),
            location: Some(Location { line: 1, column: 8 }),
        };
        let out = render_plan_error(&err, &source);
        assert!(out.contains("query.sql:1:8"), "got: {out}");
        assert!(out.contains("column not found: unknown_col"), "got: {out}");
    }

    #[test]
    fn render_no_location_still_includes_message() {
        let source = SourceFile::new("query.sql", "SELECT * FROM missing");
        let err = PlanError::TableNotFound("missing".to_string());
        let out = render_plan_error(&err, &source);
        assert!(out.contains("table not found: missing"), "got: {out}");
    }

    #[test]
    fn render_out_of_range_location_falls_back_to_prefix() {
        let source = SourceFile::new("query.sql", "SELECT 1");
        let err = PlanError::ColumnNotFound {
            name: "x".to_string(),
            location: Some(Location { line: 5, column: 1 }),
        };
        let out = render_plan_error(&err, &source);
        assert!(out.contains("line 5:1"), "got: {out}");
        assert!(out.contains("column not found: x"), "got: {out}");
    }
}
