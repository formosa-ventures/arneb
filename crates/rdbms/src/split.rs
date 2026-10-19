use std::collections::HashMap;

/// `TableScan.properties` key: the split column's remote name.
const SPLIT_COLUMN_PROPERTY: &str = "rdbms.split_column";
/// `TableScan.properties` key: comma-separated inner boundaries (i64).
const SPLIT_BOUNDS_PROPERTY: &str = "rdbms.split_bounds";

/// How a table is divided into scan partitions.
///
/// `bounds` are the inner boundaries `b1 < b2 < … < b(n-1)`; partition 0
/// reads `key < b1`, partition k reads `b_k <= key < b(k+1)`, and the last
/// reads `key >= b(n-1)`. The outer ranges are open so rows written after
/// planning are still read exactly once.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SplitPlan {
    /// Split column (remote name) and its inner boundaries. `None` means a
    /// single, unsplit partition.
    split: Option<(String, Vec<i64>)>,
}

/// The key range one partition reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SplitRange {
    /// Remote name of the split column.
    pub column: String,
    /// Inclusive lower bound; `None` = unbounded.
    pub low: Option<i64>,
    /// Exclusive upper bound; `None` = unbounded.
    pub high: Option<i64>,
}

impl SplitPlan {
    /// A single-partition plan.
    pub fn single() -> Self {
        Self::default()
    }

    /// A plan over `column` with the given inner boundaries (strictly
    /// increasing). An empty `bounds` is a single partition.
    pub fn new(column: impl Into<String>, bounds: Vec<i64>) -> Self {
        debug_assert!(bounds.windows(2).all(|w| w[0] < w[1]));
        if bounds.is_empty() {
            return Self::single();
        }
        Self {
            split: Some((column.into(), bounds)),
        }
    }

    /// Number of partitions.
    pub fn partition_count(&self) -> usize {
        self.split.as_ref().map_or(1, |(_, b)| b.len() + 1)
    }

    /// The key range of `partition`, or `None` for an unsplit table.
    pub fn range(&self, partition: usize) -> Option<SplitRange> {
        let (column, bounds) = self.split.as_ref()?;
        Some(SplitRange {
            column: column.clone(),
            low: partition.checked_sub(1).map(|i| bounds[i]),
            high: bounds.get(partition).copied(),
        })
    }

    /// Store the plan in a `TableScan` properties map.
    pub fn write_properties(&self, properties: &mut HashMap<String, String>) {
        if let Some((column, bounds)) = &self.split {
            properties.insert(SPLIT_COLUMN_PROPERTY.to_string(), column.clone());
            let joined: Vec<String> = bounds.iter().map(i64::to_string).collect();
            properties.insert(SPLIT_BOUNDS_PROPERTY.to_string(), joined.join(","));
        }
    }

    /// Read a plan stored by [`write_properties`](Self::write_properties).
    pub fn from_properties(properties: &HashMap<String, String>) -> Result<Self, String> {
        let (Some(column), Some(bounds)) = (
            properties.get(SPLIT_COLUMN_PROPERTY),
            properties.get(SPLIT_BOUNDS_PROPERTY),
        ) else {
            return Ok(Self::single());
        };
        let bounds = bounds
            .split(',')
            .map(|b| b.parse::<i64>())
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("invalid '{SPLIT_BOUNDS_PROPERTY}': {e}"))?;
        if !bounds.windows(2).all(|w| w[0] < w[1]) {
            return Err(format!(
                "invalid '{SPLIT_BOUNDS_PROPERTY}': bounds are not increasing"
            ));
        }
        Ok(Self::new(column.clone(), bounds))
    }
}

/// Inner boundaries cutting `[min, max]` into equal-width key ranges.
///
/// The partition count is `min(max_splits, ceil(estimated_rows /
/// split_min_rows))`, capped by the number of distinct keys in the range.
/// Returns no boundaries (one partition) for small tables.
pub fn plan_splits(
    min: i64,
    max: i64,
    estimated_rows: u64,
    split_min_rows: u64,
    max_splits: usize,
) -> Vec<i64> {
    if max <= min || split_min_rows == 0 || estimated_rows < split_min_rows {
        return Vec::new();
    }
    let wanted = estimated_rows
        .div_ceil(split_min_rows)
        .min(max_splits as u64) as i128;
    let span = max as i128 - min as i128 + 1;
    let n = wanted.min(span);
    if n <= 1 {
        return Vec::new();
    }
    let mut bounds: Vec<i64> = (1..n)
        .map(|k| (min as i128 + span * k / n) as i64)
        .collect();
    bounds.dedup();
    bounds
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Which partition a key falls in, by the range predicates.
    fn owners(plan: &SplitPlan, key: i64) -> Vec<usize> {
        (0..plan.partition_count())
            .filter(|&p| {
                let r = plan.range(p).unwrap();
                r.low.is_none_or(|l| key >= l) && r.high.is_none_or(|h| key < h)
            })
            .collect()
    }

    #[test]
    fn large_table_gets_max_splits() {
        let b = plan_splits(1, 1_000_000, 1_000_000, 100_000, 8);
        assert_eq!(b.len(), 7);
        assert!(b.windows(2).all(|w| w[0] < w[1]));
    }

    #[test]
    fn row_estimate_limits_split_count() {
        assert_eq!(plan_splits(1, 1_000_000, 250_000, 100_000, 16).len(), 2);
    }

    #[test]
    fn small_or_degenerate_tables_are_not_split() {
        assert!(plan_splits(1, 1_000, 999, 1_000, 8).is_empty());
        assert!(plan_splits(5, 5, 1_000_000, 1, 8).is_empty());
        assert!(plan_splits(9, 3, 1_000_000, 1, 8).is_empty());
        assert!(plan_splits(1, 100, 1_000, 0, 8).is_empty());
    }

    #[test]
    fn narrow_key_range_caps_partitions() {
        // 3 distinct keys cannot make 8 non-empty ranges.
        let b = plan_splits(10, 12, 1_000_000, 1, 8);
        assert_eq!(b, vec![11, 12]);
    }

    #[test]
    fn extreme_keys_do_not_overflow() {
        let b = plan_splits(i64::MIN, i64::MAX, u64::MAX, 1, 4);
        assert_eq!(b.len(), 3);
        assert!(b.windows(2).all(|w| w[0] < w[1]));
    }

    #[test]
    fn every_key_is_owned_exactly_once() {
        let plan = SplitPlan::new("id", plan_splits(1, 1_000, 1_000, 100, 7));
        assert_eq!(plan.partition_count(), 7);
        // Including keys outside the sampled [min, max].
        for key in [-5, 0, 1, 2, 142, 143, 500, 999, 1_000, 1_001, 10_000] {
            assert_eq!(owners(&plan, key).len(), 1, "key {key}");
        }
    }

    #[test]
    fn unsplit_plan_has_one_unbounded_partition() {
        let plan = SplitPlan::single();
        assert_eq!(plan.partition_count(), 1);
        assert!(plan.range(0).is_none());
        assert_eq!(SplitPlan::new("id", vec![]), plan);
    }

    #[test]
    fn properties_round_trip() {
        let plan = SplitPlan::new("o_orderkey", vec![-3, 10, 2_000]);
        let mut props = HashMap::new();
        plan.write_properties(&mut props);
        assert_eq!(SplitPlan::from_properties(&props).unwrap(), plan);

        let mut empty = HashMap::new();
        SplitPlan::single().write_properties(&mut empty);
        assert!(empty.is_empty());
        assert_eq!(
            SplitPlan::from_properties(&empty).unwrap(),
            SplitPlan::single()
        );
    }

    #[test]
    fn bad_bounds_are_rejected() {
        let mut props = HashMap::new();
        props.insert(SPLIT_COLUMN_PROPERTY.into(), "id".into());
        props.insert(SPLIT_BOUNDS_PROPERTY.into(), "5,3".into());
        assert!(SplitPlan::from_properties(&props).is_err());
        props.insert(SPLIT_BOUNDS_PROPERTY.into(), "x".into());
        assert!(SplitPlan::from_properties(&props).is_err());
    }
}
