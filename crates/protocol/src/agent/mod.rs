//! The agent query API service: an HTTP query API for AI agents on the
//! coordinator's Web port. Statements are submitted, polled and cancelled
//! through [`AgentQueryService`], which runs them on the Trino-protocol
//! engine; the configurable [`AgentLimits`] bound every request.

pub mod discovery;
pub mod error;
pub mod explain;
pub mod limits;
pub mod results;
pub mod service;
pub mod statement;

pub use discovery::{ColumnDescription, ListRequest, Listing, Named, TableDescription, TableStats};
pub use error::{AgentError, AgentErrorCode, SqlLocation};
pub use explain::{ExplainRequest, Explanation, ScanEstimate};
pub use limits::AgentLimits;
pub use results::{ResultPage, TruncatedBy};
pub use service::AgentQueryService;
pub use statement::{Column, StatementState, StatementStatus, SubmitRequest};

/// Cuts `text` to at most `max_bytes` bytes, backing off to the previous
/// character boundary so the result stays valid UTF-8. Returns whether it
/// was cut.
pub fn truncate_on_char_boundary(text: &mut String, max_bytes: usize) -> bool {
    if text.len() <= max_bytes {
        return false;
    }
    let mut end = max_bytes;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text.truncate(end);
    true
}

#[cfg(test)]
mod tests {
    use super::truncate_on_char_boundary;

    #[test]
    fn truncation_backs_off_to_a_char_boundary() {
        let mut s = "ab測".to_owned(); // 2 + 3 bytes
        assert!(!truncate_on_char_boundary(&mut s, 5));
        assert_eq!(s, "ab測");
        for (max, expected) in [(4, "ab"), (3, "ab"), (2, "ab"), (1, "a"), (0, "")] {
            let mut s = "ab測".to_owned();
            assert!(truncate_on_char_boundary(&mut s, max), "{max}");
            assert_eq!(s, expected, "{max}");
        }
    }
}
