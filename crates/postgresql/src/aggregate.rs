use arneb_common::types::{ColumnInfo, DataType, ScalarValue};
use arneb_execution::AggregatePushdown;
use arneb_planner::PlanExpr;
use arneb_rdbms::{
    translate_filters, Dialect, RemoteColumn, RemoteTable, TranslateOptions, Translation,
};

use crate::{copy::Kind, PostgresDialect};

/// Validated PostgreSQL expressions and their binary COPY decoders.
pub(crate) struct AggregateSql {
    /// Select expressions in the requested output order.
    pub(crate) select: Vec<String>,
    /// Expressions used as grouping keys.
    pub(crate) group_by: Vec<String>,
    /// Column names and decoders in select-list order.
    pub(crate) kinds: Vec<(String, Kind)>,
    /// Exact predicates to apply before aggregation.
    pub(crate) filters: Translation,
}

/// Plan an exact aggregate whose results fit the requested schema.
pub(crate) fn plan_aggregate_pushdown(
    table: &RemoteTable,
    schema: &[ColumnInfo],
    request: &AggregatePushdown<'_>,
) -> Option<AggregateSql> {
    if request.aggregates.is_empty()
        || request.output_schema.len() != request.group_by.len() + request.aggregates.len()
    {
        return None;
    }
    let filters = translate_filters(
        request.filters,
        schema,
        table,
        &AggregateFilterDialect,
        TranslateOptions::default(),
    );
    if !filters.complete {
        return None;
    }

    let mut group_by = Vec::with_capacity(request.group_by.len());
    let mut kinds = Vec::with_capacity(request.output_schema.len());
    for (expr, output) in request.group_by.iter().zip(request.output_schema) {
        let (remote, input, sql) = resolve_column(expr, table, schema)?;
        if input.data_type != output.data_type {
            return None;
        }
        group_by.push(render_ordered_column(remote, input, sql)?);
        kinds.push((
            output.name.clone(),
            Kind::for_column(&output.name, &remote.remote_type, &output.data_type).ok()?,
        ));
    }

    let mut select = group_by.clone();
    for (expr, output) in request
        .aggregates
        .iter()
        .zip(&request.output_schema[group_by.len()..])
    {
        let (sql, remote_type) = render_aggregate(expr, table, schema, output, request.partial)?;
        select.push(sql);
        kinds.push((
            output.name.clone(),
            Kind::for_column(&output.name, remote_type, &output.data_type).ok()?,
        ));
    }
    Some(AggregateSql {
        select,
        group_by,
        kinds,
        filters,
    })
}

fn resolve_column<'a, 'b>(
    expr: &PlanExpr,
    table: &'a RemoteTable,
    schema: &'b [ColumnInfo],
) -> Option<(&'a RemoteColumn, &'b ColumnInfo, String)> {
    let PlanExpr::Column { index, .. } = expr else {
        return None;
    };
    let remote = table.columns.get(*index)?;
    Some((
        remote,
        schema.get(*index)?,
        PostgresDialect.quote_identifier(&remote.name),
    ))
}

fn render_ordered_column(remote: &RemoteColumn, input: &ColumnInfo, sql: String) -> Option<String> {
    match (remote.remote_type.as_str(), &input.data_type) {
        ("text" | "varchar", DataType::Utf8) => Some(format!(r#"({sql} COLLATE pg_catalog."C")"#)),
        ("int2" | "int4" | "int8" | "bool" | "date" | "timestamp" | "timestamptz" | "bytea", _) => {
            Some(sql)
        }
        ("numeric", DataType::Decimal128 { precision, .. }) if *precision < 38 => Some(sql),
        _ => None,
    }
}

fn render_aggregate<'a>(
    expr: &PlanExpr,
    table: &'a RemoteTable,
    schema: &[ColumnInfo],
    output: &ColumnInfo,
    partial: bool,
) -> Option<(String, &'a str)> {
    let PlanExpr::Function {
        name,
        args,
        distinct: false,
        order_by: None,
        ..
    } = expr
    else {
        return None;
    };
    let name = name.to_ascii_uppercase();
    if name == "COUNT" && output.data_type == DataType::Int64 {
        let arg = match args.as_slice() {
            [] | [PlanExpr::Wildcard] => "*".into(),
            [PlanExpr::Literal {
                value: ScalarValue::Null,
                ..
            }] => "NULL".into(),
            [PlanExpr::Literal { .. }] => "*".into(),
            [expr] => {
                let (remote, input, sql) = resolve_column(expr, table, schema)?;
                Kind::for_column(&input.name, &remote.remote_type, &input.data_type).ok()?;
                sql
            }
            _ => return None,
        };
        return Some((format!("pg_catalog.count({arg})"), "int8"));
    }
    let [arg] = args.as_slice() else { return None };
    let (remote, input, sql) = resolve_column(arg, table, schema)?;
    let remote_type = remote.remote_type.as_str();
    let narrow_int = matches!(
        (remote_type, &input.data_type),
        ("int2", DataType::Int16) | ("int4", DataType::Int32)
    );
    match (name.as_str(), &output.data_type) {
        // SUM(bigint) produces numeric and a split can exceed i64 before
        // it is combined with sums of the other splits.
        ("SUM", DataType::Int64) if narrow_int => Some((format!("pg_catalog.sum({sql})"), "int8")),
        (
            "SUM",
            DataType::Decimal128 {
                precision: 38,
                scale,
            },
        ) => match &input.data_type {
            DataType::Decimal128 {
                precision,
                scale: input_scale,
            } if remote_type == "numeric" && *precision < 38 && input_scale == scale => {
                Some((format!("pg_catalog.sum({sql})"), "numeric"))
            }
            _ => None,
        },
        ("AVG", DataType::Float64) if !partial && narrow_int => {
            // Match the engine's floating-point accumulation and division.
            Some((format!("pg_catalog.avg({sql}::double precision)"), "float8"))
        }
        ("MIN" | "MAX", data_type)
            if data_type == &input.data_type && !matches!(remote_type, "bool" | "bytea") =>
        {
            let sql = render_ordered_column(remote, input, sql)?;
            Some((
                format!("pg_catalog.{}({sql})", name.to_ascii_lowercase()),
                if remote_type == "varchar" {
                    "text"
                } else {
                    remote_type
                },
            ))
        }
        _ => None,
    }
}

/// Row scans can recheck a pushed predicate locally. Aggregates need the
/// narrower set whose comparisons are independent of the remote collation
/// and whose values have the same representation on both sides.
#[derive(Debug)]
struct AggregateFilterDialect;

impl Dialect for AggregateFilterDialect {
    fn quote_identifier(&self, ident: &str) -> String {
        PostgresDialect.quote_identifier(ident)
    }

    fn render_literal(&self, value: &ScalarValue) -> Option<String> {
        PostgresDialect.render_literal(value)
    }

    fn pushable_column(&self, column: &RemoteColumn) -> bool {
        matches!(
            column.remote_type.as_str(),
            "int2" | "int4" | "int8" | "bool" | "date" | "timestamp" | "timestamptz"
        )
    }

    fn empty_select_list(&self) -> &'static str {
        PostgresDialect.empty_select_list()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arneb_catalog::CatalogProvider;
    use arneb_common::{stream::collect_stream, types::TableReference};
    use arneb_connectors::ConnectorFactory;
    use arneb_execution::ScanContext;
    use arneb_sql_parser::ast::BinaryOp;
    use arrow::{array::AsArray, datatypes::Int64Type};

    use crate::{PostgresCatalog, PostgresConfig};

    fn col(index: usize) -> PlanExpr {
        PlanExpr::Column {
            index,
            name: format!("col{index}"),
            span: None,
        }
    }

    fn lit(value: ScalarValue) -> PlanExpr {
        PlanExpr::Literal { value, span: None }
    }

    fn bin(left: PlanExpr, op: BinaryOp, right: PlanExpr) -> PlanExpr {
        PlanExpr::BinaryOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
            span: None,
        }
    }

    #[test]
    fn aggregate_result_types_match_postgres_wire_types() {
        let cases = [
            (
                "SUM",
                "int2",
                DataType::Int16,
                DataType::Int64,
                Some(("pg_catalog.sum(\"x\")", "int8")),
            ),
            ("SUM", "int8", DataType::Int64, DataType::Int64, None),
            (
                "AVG",
                "int4",
                DataType::Int32,
                DataType::Float64,
                Some(("pg_catalog.avg(\"x\"::double precision)", "float8")),
            ),
            (
                "MIN",
                "varchar",
                DataType::Utf8,
                DataType::Utf8,
                Some((r#"pg_catalog.min(("x" COLLATE pg_catalog."C"))"#, "text")),
            ),
        ];
        for (name, remote_type, input_type, output_type, expected) in cases {
            let table = RemoteTable {
                schema: "public".into(),
                name: "fixture".into(),
                columns: vec![RemoteColumn {
                    name: "x".into(),
                    remote_type: remote_type.into(),
                }],
            };
            let schema = [ColumnInfo {
                name: "x".into(),
                data_type: input_type,
                nullable: true,
            }];
            let output = ColumnInfo {
                name: name.into(),
                data_type: output_type,
                nullable: true,
            };
            let expr = PlanExpr::Function {
                name: name.into(),
                args: vec![col(0)],
                distinct: false,
                order_by: None,
                span: None,
            };
            let actual = render_aggregate(&expr, &table, &schema, &output, false);
            assert_eq!(
                actual.as_ref().map(|(sql, ty)| (sql.as_str(), *ty)),
                expected
            );
            if name == "AVG" || remote_type == "int8" {
                assert!(render_aggregate(&expr, &table, &schema, &output, true).is_none());
            }
        }
    }

    #[tokio::test]
    async fn exact_aggregate_pushdown_respects_filters_and_splits() {
        let Ok(url) = std::env::var("ARNEB_TEST_POSTGRES_URL") else {
            return;
        };
        let mut config = PostgresConfig::new(&url);
        config.split_min_rows = 50_000;
        config.max_splits = 4;
        let cat = PostgresCatalog::connect("pg", config).await.unwrap();
        let table = cat
            .schema("arneb_test")
            .await
            .unwrap()
            .table("keyed")
            .await
            .unwrap();
        let ds = cat
            .connector_factory()
            .create_data_source(
                &TableReference {
                    catalog: Some("pg".into()),
                    schema: Some("arneb_test".into()),
                    table: "keyed".into(),
                },
                &table.schema(),
                &table.properties(),
            )
            .await
            .unwrap();
        let aggregates = [PlanExpr::Function {
            name: "COUNT".into(),
            args: vec![PlanExpr::Wildcard],
            distinct: false,
            order_by: None,
            span: None,
        }];
        let output_schema = [ColumnInfo {
            name: "n".into(),
            data_type: DataType::Int64,
            nullable: false,
        }];
        let filters = [bin(col(1), BinaryOp::Eq, lit(ScalarValue::Int32(3)))];
        let mut request = AggregatePushdown {
            group_by: &[],
            aggregates: &aggregates,
            filters: &filters,
            output_schema: &output_schema,
            partial: false,
        };
        let (client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(connection);
        let expected: i64 = client
            .query_one("SELECT count(*) FROM arneb_test.keyed WHERE grp = 3", &[])
            .await
            .unwrap()
            .get(0);
        for (partial, partitions) in [(false, 1), (true, 4)] {
            request.partial = partial;
            let aggregate = ds.pushdown_aggregate(&request).unwrap();
            assert_eq!(aggregate.partition_count(), partitions);
            let mut batches = Vec::new();
            for partition in 0..partitions {
                let stream = aggregate
                    .scan(&ScanContext::default(), partition)
                    .await
                    .unwrap();
                batches.extend(collect_stream(stream).await.unwrap());
            }
            assert_eq!(
                batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
                partitions
            );
            let total: i64 = batches
                .iter()
                .map(|b| b.column(0).as_primitive::<Int64Type>().value(0))
                .sum();
            assert_eq!(total, expected);
        }
        let residual = [bin(
            col(2),
            BinaryOp::Eq,
            lit(ScalarValue::Utf8("label-1".into())),
        )];
        request.filters = &residual;
        assert!(ds.pushdown_aggregate(&request).is_none());
    }
}
