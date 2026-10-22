use crate::{Dialect, RemoteTable, SplitRange, Translation};

/// The remote `SELECT` for one scan partition.
///
/// - `projection`: Arneb column indices to fetch, in output order (`None` =
///   all columns in table order; empty = no columns, rows only).
/// - `translation`: the pushed filter conjuncts.
/// - `split`: this partition's key range, if the table is split.
/// - `limit`: appended only when `translation.complete` (otherwise the
///   remote would cut rows before Arneb's local filter runs).
pub fn build_select(
    table: &RemoteTable,
    projection: Option<&[usize]>,
    translation: &Translation,
    split: Option<&SplitRange>,
    limit: Option<usize>,
    dialect: &dyn Dialect,
) -> Result<String, String> {
    let select_list = match projection {
        Some([]) => dialect.empty_select_list().to_string(),
        Some(indices) => indices
            .iter()
            .map(|&i| {
                table
                    .columns
                    .get(i)
                    .map(|c| dialect.quote_identifier(&c.name))
                    .ok_or_else(|| format!("projection index {i} out of range"))
            })
            .collect::<Result<Vec<_>, _>>()?
            .join(", "),
        None => table
            .columns
            .iter()
            .map(|c| dialect.quote_identifier(&c.name))
            .collect::<Vec<_>>()
            .join(", "),
    };
    let mut sql = format!(
        "SELECT {select_list} FROM {}.{}",
        dialect.quote_identifier(&table.schema),
        dialect.quote_identifier(&table.name)
    );

    let mut conditions = translation.conjuncts.clone();
    if let Some(range) = split {
        let column = dialect.quote_identifier(&range.column);
        if let Some(low) = range.low {
            conditions.push(format!("{column} >= {low}"));
        }
        if let Some(high) = range.high {
            conditions.push(format!("{column} < {high}"));
        }
    }
    if !conditions.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&conditions.join(" AND "));
    }
    if let (Some(n), true) = (limit, translation.complete) {
        sql.push_str(&format!(" LIMIT {n}"));
    }
    Ok(sql)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::{nation, TestDialect};
    use crate::SplitPlan;

    fn sql(
        projection: Option<&[usize]>,
        translation: &Translation,
        split: Option<&SplitRange>,
        limit: Option<usize>,
    ) -> String {
        let (_, table) = nation();
        build_select(&table, projection, translation, split, limit, &TestDialect).unwrap()
    }

    fn complete() -> Translation {
        Translation {
            conjuncts: vec![],
            complete: true,
            ..Default::default()
        }
    }

    #[test]
    fn full_scan_selects_all_columns() {
        assert_eq!(
            sql(None, &complete(), None, None),
            "SELECT \"n_nationkey\", \"n_name\", \"n_regionkey\", \"n_comment\", \"n_balance\", \
             \"n_flag\", \"n_ratio\" FROM \"public\".\"nation\""
        );
    }

    #[test]
    fn projection_order_is_preserved() {
        assert_eq!(
            sql(Some(&[2, 0]), &complete(), None, None),
            "SELECT \"n_regionkey\", \"n_nationkey\" FROM \"public\".\"nation\""
        );
    }

    #[test]
    fn empty_projection_uses_dialect_list() {
        assert_eq!(
            sql(Some(&[]), &complete(), None, None),
            "SELECT  FROM \"public\".\"nation\""
        );
    }

    #[test]
    fn out_of_range_projection_is_an_error() {
        let (_, table) = nation();
        assert!(build_select(&table, Some(&[9]), &complete(), None, None, &TestDialect).is_err());
    }

    #[test]
    fn filters_split_and_limit_combine() {
        let t = Translation {
            conjuncts: vec!["(\"n_regionkey\" = 1)".into()],
            complete: true,
            ..Default::default()
        };
        let plan = SplitPlan::new("n_nationkey", vec![10, 20]);
        assert_eq!(
            sql(Some(&[1]), &t, plan.range(1).as_ref(), Some(5)),
            "SELECT \"n_name\" FROM \"public\".\"nation\" \
             WHERE (\"n_regionkey\" = 1) AND \"n_nationkey\" >= 10 AND \"n_nationkey\" < 20 LIMIT 5"
        );
        assert!(sql(None, &t, plan.range(0).as_ref(), None)
            .ends_with("WHERE (\"n_regionkey\" = 1) AND \"n_nationkey\" < 10"));
        assert!(sql(None, &t, plan.range(2).as_ref(), None)
            .ends_with("WHERE (\"n_regionkey\" = 1) AND \"n_nationkey\" >= 20"));
    }

    #[test]
    fn limit_is_dropped_when_a_filter_stayed_local() {
        let t = Translation {
            conjuncts: vec![],
            complete: false,
            ..Default::default()
        };
        assert_eq!(
            sql(None, &t, None, Some(5)),
            sql(None, &complete(), None, None)
        );
        assert!(sql(None, &complete(), None, Some(5)).ends_with(" LIMIT 5"));
    }
}
