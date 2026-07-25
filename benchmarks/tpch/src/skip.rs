//! Per-engine skip list — queries an engine cannot run today.
//!
//! Recording a query as skipped (rather than letting it fail) keeps the
//! report's failure count meaningful and makes the gap visible as a
//! follow-up TODO.

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

/// Skip list shipped with the harness. Updated as Arneb's SQL coverage grows.
pub fn default_skip_list() -> SkipList {
    SkipList::new(vec![
        SkipEntry {
            engine: "arneb",
            query_id: "q15",
            reason: "uses CREATE VIEW + LIMIT in scalar subquery (view DDL not supported in benchmark flow)",
        },
        SkipEntry {
            engine: "arneb",
            query_id: "q17",
            reason: "uses correlated scalar subquery in WHERE clause",
        },
        SkipEntry {
            engine: "arneb",
            query_id: "q20",
            reason: "uses correlated EXISTS subquery",
        },
        SkipEntry {
            engine: "arneb",
            query_id: "q21",
            reason: "uses correlated EXISTS / NOT EXISTS subqueries",
        },
        SkipEntry {
            engine: "arneb",
            query_id: "q22",
            reason: "uses correlated NOT EXISTS subquery in WHERE clause",
        },
    ])
}
