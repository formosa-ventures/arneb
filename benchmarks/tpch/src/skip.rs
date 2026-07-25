//! Per-engine skip list — queries an engine cannot run today.
//!
//! Recording a query as skipped (rather than letting it fail) keeps the
//! report's failure count meaningful and makes the gap visible as a
//! follow-up TODO.
//!
//! Entries must be verified by actually running the query against the engine.
//! An unverified entry is worse than no entry: it silently removes a query from
//! the comparison and publishes a false reason for doing so.

#[derive(Debug, Clone)]
pub struct SkipEntry {
    pub engine: &'static str,
    pub query_id: &'static str,
    pub reason: &'static str,
}

#[derive(Debug, Clone, Default)]
pub struct SkipList {
    entries: Vec<SkipEntry>,
}

impl SkipList {
    pub fn new(entries: Vec<SkipEntry>) -> Self {
        Self { entries }
    }

    pub fn lookup(&self, engine: &str, query_id: &str) -> Option<&SkipEntry> {
        self.entries
            .iter()
            .find(|e| e.engine == engine && e.query_id == query_id)
    }
}

/// Skip list shipped with the harness. Empty: every TPC-H query runs on every
/// supported engine.
///
/// This previously declared arneb unable to run q15, q17, q20, q21 and q22,
/// each citing unsupported correlated subqueries. None of it was true. The
/// entries were derived from reading the SQL rather than executing it, and the
/// task that would have checked them was deferred and never done — so the
/// harness excluded five queries from every run and reported a fabricated
/// reason for each. All five execute successfully against a stock arneb build.
///
/// Add an entry only after observing the failure, and quote the error.
pub fn default_skip_list() -> SkipList {
    SkipList::new(vec![])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_query_is_skipped_without_a_verified_reason() {
        let list = default_skip_list();
        for q in [
            "q01", "q02", "q03", "q04", "q05", "q06", "q07", "q08", "q09", "q10", "q11", "q12",
            "q13", "q14", "q15", "q16", "q17", "q18", "q19", "q20", "q21", "q22",
        ] {
            assert!(
                list.lookup("arneb", q).is_none(),
                "{q} is skipped for arneb; every TPC-H query is known to run, so this \
                 entry silently drops a query from the comparison"
            );
        }
    }
}
