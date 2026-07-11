//! Dynamic filter identifiers shared between planner, execution, and RPC layers.
//!
//! A [`DynamicFilterId`] is allocated at plan time for each cross-fragment dynamic
//! filter (one per INNER/SEMI join equi-key where the build side is small enough
//! to summarise). It travels with the logical plan through fragmentation and is
//! used as the join key between build-side producers and probe-side consumers.

use std::fmt;

/// Identifies one cross-fragment dynamic filter within a query.
///
/// IDs are dense u32 counters allocated by `DynamicFilterIdAllocator` (in the
/// planner crate); they are unique within a single query plan.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct DynamicFilterId(pub u32);

impl fmt::Display for DynamicFilterId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "df_{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn dynamic_filter_id_display() {
        assert_eq!(DynamicFilterId(0).to_string(), "df_0");
        assert_eq!(DynamicFilterId(42).to_string(), "df_42");
    }

    #[test]
    fn dynamic_filter_id_equality_and_hash() {
        let mut map = HashMap::new();
        map.insert(DynamicFilterId(1), "build-on-nation");
        map.insert(DynamicFilterId(2), "build-on-supplier");
        assert_eq!(map[&DynamicFilterId(1)], "build-on-nation");
        assert_eq!(DynamicFilterId(1), DynamicFilterId(1));
        assert_ne!(DynamicFilterId(1), DynamicFilterId(2));
    }

    #[test]
    fn dynamic_filter_id_ordering() {
        let mut ids = vec![DynamicFilterId(3), DynamicFilterId(1), DynamicFilterId(2)];
        ids.sort();
        assert_eq!(
            ids,
            vec![DynamicFilterId(1), DynamicFilterId(2), DynamicFilterId(3)]
        );
    }
}
