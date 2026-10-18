//! Shared building blocks for remote-SQL connectors (PostgreSQL today,
//! MySQL later).
//!
//! A connector for one database implements [`Dialect`] and a client; this
//! crate owns everything else that is database-neutral:
//!
//! - [`translate_filters`]: which pushed-down [`PlanExpr`] conjuncts become
//!   remote SQL. Anything not translated stays in Arneb's `FilterExec`,
//!   which always runs above the scan, so a missed translation can cost
//!   performance but never correctness.
//! - [`build_select`]: the remote `SELECT` for one scan partition.
//! - [`SplitPlan`] / [`plan_splits`]: primary-key range splits, planned once
//!   on the coordinator and carried to every node in the table-scan
//!   properties so all nodes agree on the partitions.
//!
//! See `openspec/changes/rdbms-postgresql-connector/design.md`.

mod select;
mod split;
mod table;
mod translate;

pub use select::build_select;
pub use split::{plan_splits, SplitPlan, SplitRange};
pub use table::{RemoteColumn, RemoteTable, TABLE_META_PROPERTY};
pub use translate::{translate_filters, Translation};

use std::fmt::Debug;

use arneb_common::types::ScalarValue;

/// The database-specific part of remote SQL generation.
pub trait Dialect: Send + Sync + Debug {
    /// Quote one identifier (schema, table or column name), escaping any
    /// embedded quote character.
    fn quote_identifier(&self, ident: &str) -> String;

    /// Render a literal as remote SQL, or `None` when this dialect cannot
    /// render it with exactly Arneb's value (the conjunct then stays local).
    /// Never called with [`ScalarValue::Null`].
    fn render_literal(&self, value: &ScalarValue) -> Option<String>;

    /// Whether filters on this column may be pushed at all. A dialect
    /// refuses columns whose remote comparison semantics differ from
    /// Arneb's (e.g. PostgreSQL `char(n)` ignores trailing spaces).
    fn pushable_column(&self, column: &RemoteColumn) -> bool {
        let _ = column;
        true
    }

    /// The `SELECT` list to use when no column is needed (e.g. `count(*)`).
    /// The result must produce zero columns per row.
    fn empty_select_list(&self) -> &'static str;
}

#[cfg(test)]
pub(crate) mod test_util {
    use arneb_common::types::{ColumnInfo, DataType, ScalarValue};
    use arneb_planner::PlanExpr;

    use crate::{Dialect, RemoteColumn, RemoteTable};

    /// A minimal ANSI-ish dialect for framework tests.
    #[derive(Debug)]
    pub struct TestDialect;

    impl Dialect for TestDialect {
        fn quote_identifier(&self, ident: &str) -> String {
            format!("\"{}\"", ident.replace('"', "\"\""))
        }

        fn render_literal(&self, value: &ScalarValue) -> Option<String> {
            Some(match value {
                ScalarValue::Int32(v) => v.to_string(),
                ScalarValue::Int64(v) => v.to_string(),
                ScalarValue::Utf8(s) if !s.contains('\\') => format!("'{}'", s.replace('\'', "''")),
                ScalarValue::Decimal128 { value, scale, .. } => {
                    let s = *scale as u32;
                    let p = 10i128.pow(s);
                    format!(
                        "{}.{:0width$}",
                        value / p,
                        (value % p).abs(),
                        width = s as usize
                    )
                }
                _ => return None,
            })
        }

        fn pushable_column(&self, column: &RemoteColumn) -> bool {
            column.remote_type != "bpchar"
        }

        fn empty_select_list(&self) -> &'static str {
            ""
        }
    }

    pub fn col(index: usize) -> PlanExpr {
        PlanExpr::Column {
            index,
            name: format!("c{index}"),
            span: None,
        }
    }

    pub fn lit(value: ScalarValue) -> PlanExpr {
        PlanExpr::Literal { value, span: None }
    }

    /// `nation` plus a decimal column, as (Arneb schema, remote table).
    pub fn nation() -> (Vec<ColumnInfo>, RemoteTable) {
        let cols = [
            ("n_nationkey", DataType::Int32, "int4"),
            ("n_name", DataType::Utf8, "varchar"),
            ("n_regionkey", DataType::Int32, "int4"),
            ("n_comment", DataType::Utf8, "varchar"),
            (
                "n_balance",
                DataType::Decimal128 {
                    precision: 15,
                    scale: 2,
                },
                "numeric",
            ),
            ("n_flag", DataType::Utf8, "bpchar"),
            ("n_ratio", DataType::Float64, "float8"),
        ];
        let schema = cols
            .iter()
            .map(|(n, t, _)| ColumnInfo {
                name: n.to_string(),
                data_type: t.clone(),
                nullable: true,
            })
            .collect();
        let table = RemoteTable {
            schema: "public".into(),
            name: "nation".into(),
            columns: cols
                .iter()
                .map(|(n, _, r)| RemoteColumn {
                    name: n.to_string(),
                    remote_type: r.to_string(),
                })
                .collect(),
        };
        (schema, table)
    }
}
