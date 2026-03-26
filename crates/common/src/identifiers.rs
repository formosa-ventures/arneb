//! Identifier types for distributed query execution.

use std::fmt;

/// Uniquely identifies a query.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct QueryId(pub uuid::Uuid);

impl QueryId {
    /// Creates a new random QueryId.
    pub fn new() -> Self {
        Self(uuid::Uuid::new_v4())
    }
}

impl Default for QueryId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for QueryId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Identifies a stage (fragment instance) within a query.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StageId(pub u32);

impl fmt::Display for StageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Identifies a task (partition of a stage) within a query.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TaskId {
    /// The stage this task belongs to.
    pub stage_id: StageId,
    /// The partition index within the stage.
    pub partition_id: u32,
}

impl fmt::Display for TaskId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.stage_id, self.partition_id)
    }
}

/// Identifies a data split (unit of work for a source scan).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SplitId(pub String);

impl fmt::Display for SplitId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn stage_id_display() {
        assert_eq!(StageId(42).to_string(), "42");
    }

    #[test]
    fn stage_id_equality_and_hash() {
        let mut map = HashMap::new();
        map.insert(StageId(1), "first");
        map.insert(StageId(2), "second");
        assert_eq!(map[&StageId(1)], "first");
        assert_eq!(StageId(1), StageId(1));
        assert_ne!(StageId(1), StageId(2));
    }

    #[test]
    fn task_id_display() {
        let tid = TaskId {
            stage_id: StageId(3),
            partition_id: 7,
        };
        assert_eq!(tid.to_string(), "3.7");
    }

    #[test]
    fn task_id_as_hashmap_key() {
        let mut map = HashMap::new();
        let t1 = TaskId {
            stage_id: StageId(1),
            partition_id: 0,
        };
        let t2 = TaskId {
            stage_id: StageId(1),
            partition_id: 1,
        };
        map.insert(t1, "task-0");
        map.insert(t2, "task-1");
        assert_eq!(map.len(), 2);
    }

    #[test]
    fn split_id_display() {
        let sid = SplitId("file://data/part-0.parquet".into());
        assert_eq!(sid.to_string(), "file://data/part-0.parquet");
    }
}
