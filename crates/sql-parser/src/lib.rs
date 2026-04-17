#![warn(missing_docs)]
#![warn(unreachable_pub)]
#![deny(unsafe_code)]

//! SQL parser for arneb.
//!
//! Parses SQL strings into a arneb-specific AST using `sqlparser-rs`
//! as the underlying parser. Only the SQL subset required for the MVP is
//! supported; unsupported constructs produce [`arneb_common::error::ParseError`].

pub mod ast;
mod convert;

use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use arneb_common::error::ParseError;

pub use ast::*;

/// Parse a SQL string into a arneb [`Statement`].
///
/// Uses `sqlparser-rs` with the [`GenericDialect`] for lexing and parsing,
/// then converts to arneb's AST representation.
///
/// # Errors
///
/// Returns [`ParseError::InvalidSyntax`] for syntax errors and
/// [`ParseError::UnsupportedFeature`] for valid SQL that arneb
/// does not yet support.
pub fn parse(sql: &str) -> Result<Statement, ParseError> {
    let dialect = GenericDialect {};
    let statements =
        Parser::parse_sql(&dialect, sql).map_err(|e| ParseError::InvalidSyntax(e.to_string()))?;

    if statements.is_empty() {
        return Err(ParseError::InvalidSyntax("empty SQL statement".to_string()));
    }

    if statements.len() > 1 {
        return Err(ParseError::UnsupportedFeature(
            "multiple statements".to_string(),
        ));
    }

    convert::convert_statement(statements.into_iter().next().unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arneb_common::types::{DataType, ScalarValue};

    /// Helper to extract the SelectBody from a query (panics if it's a set operation).
    fn select_body(query: &Query) -> &SelectBody {
        match &query.body {
            QueryBody::Select(body) => body,
            other => panic!("expected Select body, got {other:?}"),
        }
    }

    // -- Basic SELECT tests --

    #[test]
    fn parse_select_literal() {
        let stmt = parse("SELECT 1").unwrap();
        let Statement::Query(query) = stmt else {
            panic!("expected Query");
        };
        let body = select_body(&query);
        assert_eq!(body.projection.len(), 1);
        match &body.projection[0] {
            SelectItem::UnnamedExpr(Expr::Literal(ScalarValue::Int64(1))) => {}
            other => panic!("expected literal 1, got {other:?}"),
        }
    }

    #[test]
    fn parse_select_columns_from_table() {
        let stmt = parse("SELECT a, b FROM t").unwrap();
        let Statement::Query(query) = stmt else {
            panic!("expected Query");
        };
        assert_eq!(select_body(&query).projection.len(), 2);
        assert_eq!(select_body(&query).from.len(), 1);
        match &select_body(&query).from[0].relation {
            TableFactor::Table { name, alias } => {
                assert_eq!(name.table, "t");
                assert!(alias.is_none());
            }
            other => panic!("expected table, got {other:?}"),
        }
    }

    #[test]
    fn parse_select_wildcard() {
        let stmt = parse("SELECT * FROM t").unwrap();
        let Statement::Query(query) = stmt else {
            panic!("expected Query");
        };
        assert!(matches!(
            &select_body(&query).projection[0],
            SelectItem::Wildcard
        ));
    }

    #[test]
    fn parse_select_qualified_wildcard() {
        let stmt = parse("SELECT t.* FROM t").unwrap();
        let Statement::Query(query) = stmt else {
            panic!("expected Query");
        };
        match &select_body(&query).projection[0] {
            SelectItem::QualifiedWildcard(tr) => {
                assert_eq!(tr.table, "t");
            }
            other => panic!("expected qualified wildcard, got {other:?}"),
        }
    }

    // -- WHERE clause tests --

    #[test]
    fn parse_where_comparison() {
        let stmt = parse("SELECT a FROM t WHERE x > 1").unwrap();
        let Statement::Query(query) = stmt else {
            panic!("expected Query");
        };
        assert!(select_body(&query).selection.is_some());
        match select_body(&query).selection.as_deref() {
            Some(Expr::BinaryOp { op, .. }) => {
                assert_eq!(*op, BinaryOp::Gt);
            }
            other => panic!("expected BinaryOp Gt, got {other:?}"),
        }
    }

    #[test]
    fn parse_where_and_or() {
        let stmt = parse("SELECT a FROM t WHERE x > 1 AND y < 2 OR z = 3").unwrap();
        let Statement::Query(query) = stmt else {
            panic!("expected Query");
        };
        // Should parse successfully; exact tree depends on precedence
        assert!(select_body(&query).selection.is_some());
    }

    #[test]
    fn parse_where_is_null() {
        let stmt = parse("SELECT a FROM t WHERE x IS NULL").unwrap();
        let Statement::Query(query) = stmt else {
            panic!("expected Query");
        };
        assert!(matches!(
            select_body(&query).selection.as_deref(),
            Some(Expr::IsNull(_))
        ));
    }

    #[test]
    fn parse_where_between() {
        let stmt = parse("SELECT a FROM t WHERE x BETWEEN 1 AND 10").unwrap();
        let Statement::Query(query) = stmt else {
            panic!("expected Query");
        };
        match select_body(&query).selection.as_deref() {
            Some(Expr::Between { negated, .. }) => {
                assert!(!negated);
            }
            other => panic!("expected Between, got {other:?}"),
        }
    }

    #[test]
    fn parse_where_in_list() {
        let stmt = parse("SELECT a FROM t WHERE x IN (1, 2, 3)").unwrap();
        let Statement::Query(query) = stmt else {
            panic!("expected Query");
        };
        match select_body(&query).selection.as_deref() {
            Some(Expr::InList { list, negated, .. }) => {
                assert_eq!(list.len(), 3);
                assert!(!negated);
            }
            other => panic!("expected InList, got {other:?}"),
        }
    }

    // -- JOIN tests --

    #[test]
    fn parse_inner_join() {
        let stmt = parse("SELECT * FROM t1 JOIN t2 ON t1.id = t2.id").unwrap();
        let Statement::Query(query) = stmt else {
            panic!("expected Query");
        };
        assert_eq!(select_body(&query).from.len(), 1);
        assert_eq!(select_body(&query).from[0].joins.len(), 1);
        assert_eq!(
            select_body(&query).from[0].joins[0].join_type,
            JoinType::Inner
        );
        assert!(matches!(
            &select_body(&query).from[0].joins[0].condition,
            JoinCondition::On(_)
        ));
    }

    #[test]
    fn parse_left_join() {
        let stmt = parse("SELECT * FROM t1 LEFT JOIN t2 ON t1.id = t2.id").unwrap();
        let Statement::Query(query) = stmt else {
            panic!("expected Query");
        };
        assert_eq!(
            select_body(&query).from[0].joins[0].join_type,
            JoinType::Left
        );
    }

    #[test]
    fn parse_right_join() {
        let stmt = parse("SELECT * FROM t1 RIGHT JOIN t2 ON t1.id = t2.id").unwrap();
        let Statement::Query(query) = stmt else {
            panic!("expected Query");
        };
        assert_eq!(
            select_body(&query).from[0].joins[0].join_type,
            JoinType::Right
        );
    }

    #[test]
    fn parse_cross_join() {
        let stmt = parse("SELECT * FROM t1 CROSS JOIN t2").unwrap();
        let Statement::Query(query) = stmt else {
            panic!("expected Query");
        };
        assert_eq!(
            select_body(&query).from[0].joins[0].join_type,
            JoinType::Cross
        );
        assert!(matches!(
            &select_body(&query).from[0].joins[0].condition,
            JoinCondition::None
        ));
    }

    #[test]
    fn parse_join_using() {
        let stmt = parse("SELECT * FROM t1 LEFT JOIN t2 USING (id)").unwrap();
        let Statement::Query(query) = stmt else {
            panic!("expected Query");
        };
        match &select_body(&query).from[0].joins[0].condition {
            JoinCondition::Using(cols) => {
                assert_eq!(cols, &["id"]);
            }
            other => panic!("expected Using, got {other:?}"),
        }
    }

    #[test]
    fn parse_multiple_joins() {
        let stmt = parse("SELECT * FROM t1 JOIN t2 ON t1.id = t2.id LEFT JOIN t3 ON t2.id = t3.id")
            .unwrap();
        let Statement::Query(query) = stmt else {
            panic!("expected Query");
        };
        assert_eq!(select_body(&query).from[0].joins.len(), 2);
    }

    // -- Expression tests --

    #[test]
    fn parse_arithmetic_expr() {
        let stmt = parse("SELECT a + b * 2").unwrap();
        let Statement::Query(query) = stmt else {
            panic!("expected Query");
        };
        // Should parse without error; precedence handled by sqlparser
        assert_eq!(select_body(&query).projection.len(), 1);
    }

    #[test]
    fn parse_function_call() {
        let stmt = parse("SELECT COUNT(*) FROM t").unwrap();
        let Statement::Query(query) = stmt else {
            panic!("expected Query");
        };
        match &select_body(&query).projection[0] {
            SelectItem::UnnamedExpr(Expr::Function { name, args, .. }) => {
                assert_eq!(name, "COUNT");
                assert_eq!(args.len(), 1);
                assert!(matches!(&args[0], FunctionArg::Wildcard));
            }
            other => panic!("expected function, got {other:?}"),
        }
    }

    #[test]
    fn parse_function_distinct() {
        let stmt = parse("SELECT COUNT(DISTINCT x) FROM t").unwrap();
        let Statement::Query(query) = stmt else {
            panic!("expected Query");
        };
        match &select_body(&query).projection[0] {
            SelectItem::UnnamedExpr(Expr::Function { distinct, .. }) => {
                assert!(distinct);
            }
            other => panic!("expected function, got {other:?}"),
        }
    }

    #[test]
    fn parse_cast_expr() {
        let stmt = parse("SELECT CAST(x AS INTEGER) FROM t").unwrap();
        let Statement::Query(query) = stmt else {
            panic!("expected Query");
        };
        match &select_body(&query).projection[0] {
            SelectItem::UnnamedExpr(Expr::Cast { data_type, .. }) => {
                assert_eq!(*data_type, DataType::Int32);
            }
            other => panic!("expected cast, got {other:?}"),
        }
    }

    #[test]
    fn parse_date_typed_string_literal() {
        // `DATE 'YYYY-MM-DD'` is lowered to `CAST('YYYY-MM-DD' AS DATE)` so
        // downstream passes (physical planner, expression evaluator) can
        // reuse the Cast path without needing a separate typed-string node.
        let stmt = parse("SELECT DATE '1998-12-01'").unwrap();
        let Statement::Query(query) = stmt else {
            panic!("expected Query");
        };
        match &select_body(&query).projection[0] {
            SelectItem::UnnamedExpr(Expr::Cast { expr, data_type }) => {
                assert_eq!(*data_type, DataType::Date32);
                match expr.as_ref() {
                    Expr::Literal(ScalarValue::Utf8(s)) => assert_eq!(s, "1998-12-01"),
                    other => panic!("expected Utf8 literal, got {other:?}"),
                }
            }
            other => panic!("expected Cast(Utf8 AS Date32), got {other:?}"),
        }
    }

    #[test]
    fn parse_date_literal_in_where_clause() {
        // Regression for the TPC-H Q1 shape: `WHERE l_shipdate <= DATE '1998-12-01'`.
        let stmt = parse("SELECT * FROM t WHERE d <= DATE '1998-12-01'").unwrap();
        let Statement::Query(query) = stmt else {
            panic!("expected Query");
        };
        let body = select_body(&query);
        let predicate = body.selection.as_ref().expect("WHERE clause");
        match predicate.as_ref() {
            Expr::BinaryOp { right, .. } => match right.as_ref() {
                Expr::Cast { data_type, expr } => {
                    assert_eq!(*data_type, DataType::Date32);
                    assert!(matches!(expr.as_ref(), Expr::Literal(ScalarValue::Utf8(_))));
                }
                other => panic!("expected Cast on RHS, got {other:?}"),
            },
            other => panic!("expected BinaryOp predicate, got {other:?}"),
        }
    }

    #[test]
    fn parse_timestamp_typed_string_literal() {
        let stmt = parse("SELECT TIMESTAMP '2025-01-02 03:04:05'").unwrap();
        let Statement::Query(query) = stmt else {
            panic!("expected Query");
        };
        match &select_body(&query).projection[0] {
            SelectItem::UnnamedExpr(Expr::Cast { expr, data_type }) => {
                assert!(matches!(data_type, DataType::Timestamp { .. }));
                assert!(matches!(expr.as_ref(), Expr::Literal(ScalarValue::Utf8(_))));
            }
            other => panic!("expected Cast(Utf8 AS Timestamp), got {other:?}"),
        }
    }

    #[test]
    fn parse_nested_expr() {
        let stmt = parse("SELECT (a + b) * c FROM t").unwrap();
        let Statement::Query(query) = stmt else {
            panic!("expected Query");
        };
        // Should parse without error
        assert_eq!(select_body(&query).projection.len(), 1);
    }

    // -- GROUP BY, HAVING, ORDER BY, LIMIT, OFFSET tests --

    #[test]
    fn parse_group_by() {
        let stmt = parse("SELECT a, COUNT(*) FROM t GROUP BY a").unwrap();
        let Statement::Query(query) = stmt else {
            panic!("expected Query");
        };
        assert_eq!(select_body(&query).group_by.len(), 1);
    }

    #[test]
    fn parse_having() {
        let stmt = parse("SELECT a, COUNT(*) FROM t GROUP BY a HAVING COUNT(*) > 1").unwrap();
        let Statement::Query(query) = stmt else {
            panic!("expected Query");
        };
        assert!(select_body(&query).having.is_some());
    }

    #[test]
    fn parse_order_by() {
        let stmt = parse("SELECT a FROM t ORDER BY a DESC").unwrap();
        let Statement::Query(query) = stmt else {
            panic!("expected Query");
        };
        assert_eq!(query.order_by.len(), 1);
        assert_eq!(query.order_by[0].asc, Some(false));
    }

    #[test]
    fn parse_limit_offset() {
        let stmt = parse("SELECT a FROM t LIMIT 10 OFFSET 5").unwrap();
        let Statement::Query(query) = stmt else {
            panic!("expected Query");
        };
        assert!(query.limit.is_some());
        assert!(query.offset.is_some());
    }

    // -- Subquery tests --

    #[test]
    fn parse_subquery_in_from() {
        let stmt = parse("SELECT * FROM (SELECT a FROM t) AS sub").unwrap();
        let Statement::Query(query) = stmt else {
            panic!("expected Query");
        };
        match &select_body(&query).from[0].relation {
            TableFactor::Subquery { alias, .. } => {
                assert_eq!(alias, "sub");
            }
            other => panic!("expected subquery, got {other:?}"),
        }
    }

    #[test]
    fn parse_subquery_in_where() {
        let stmt = parse("SELECT * FROM t WHERE x IN (SELECT id FROM t2)").unwrap();
        let Statement::Query(query) = stmt else {
            panic!("expected Query");
        };
        assert!(select_body(&query).selection.is_some());
    }

    // -- EXPLAIN test --

    #[test]
    fn parse_explain() {
        let stmt = parse("EXPLAIN SELECT 1").unwrap();
        assert!(matches!(stmt, Statement::Explain(_)));
    }

    // -- Error case tests --

    #[test]
    fn parse_syntax_error() {
        let result = parse("SELCT 1");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, ParseError::InvalidSyntax(_)));
    }

    #[test]
    fn parse_empty_string() {
        let result = parse("");
        assert!(result.is_err());
    }

    #[test]
    fn parse_create_table() {
        let stmt = parse("CREATE TABLE t (a INT, b VARCHAR)").unwrap();
        assert!(matches!(stmt, Statement::CreateTable { .. }));
    }

    #[test]
    fn parse_drop_table() {
        let stmt = parse("DROP TABLE IF EXISTS t").unwrap();
        match stmt {
            Statement::DropTable { if_exists, .. } => assert!(if_exists),
            other => panic!("expected DropTable, got {other:?}"),
        }
    }

    #[test]
    fn parse_insert_values() {
        let stmt = parse("INSERT INTO t VALUES (1, 'hello')").unwrap();
        assert!(matches!(stmt, Statement::InsertInto { .. }));
    }

    #[test]
    fn parse_delete_from() {
        let stmt = parse("DELETE FROM t WHERE id = 1").unwrap();
        assert!(matches!(
            stmt,
            Statement::DeleteFrom {
                predicate: Some(_),
                ..
            }
        ));
    }

    #[test]
    fn parse_create_view() {
        let stmt = parse("CREATE VIEW v AS SELECT 1").unwrap();
        assert!(matches!(stmt, Statement::CreateView { .. }));
    }

    #[test]
    fn parse_drop_view() {
        let stmt = parse("DROP VIEW IF EXISTS v").unwrap();
        match stmt {
            Statement::DropView { if_exists, .. } => assert!(if_exists),
            other => panic!("expected DropView, got {other:?}"),
        }
    }

    #[test]
    fn parse_unsupported_update() {
        let result = parse("UPDATE t SET a = 1");
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ParseError::UnsupportedFeature(_)
        ));
    }

    // -- Literal conversion tests --

    #[test]
    fn parse_integer_literal() {
        let stmt = parse("SELECT 42").unwrap();
        let Statement::Query(query) = stmt else {
            panic!("expected Query");
        };
        match &select_body(&query).projection[0] {
            SelectItem::UnnamedExpr(Expr::Literal(ScalarValue::Int64(42))) => {}
            other => panic!("expected int 42, got {other:?}"),
        }
    }

    #[test]
    #[allow(clippy::approx_constant)]
    fn parse_float_literal() {
        let stmt = parse("SELECT 3.14").unwrap();
        let Statement::Query(query) = stmt else {
            panic!("expected Query");
        };
        match &select_body(&query).projection[0] {
            SelectItem::UnnamedExpr(Expr::Literal(ScalarValue::Float64(v))) => {
                assert!((v - 3.14).abs() < f64::EPSILON);
            }
            other => panic!("expected float 3.14, got {other:?}"),
        }
    }

    #[test]
    fn parse_string_literal() {
        let stmt = parse("SELECT 'hello'").unwrap();
        let Statement::Query(query) = stmt else {
            panic!("expected Query");
        };
        match &select_body(&query).projection[0] {
            SelectItem::UnnamedExpr(Expr::Literal(ScalarValue::Utf8(s))) => {
                assert_eq!(s, "hello");
            }
            other => panic!("expected string, got {other:?}"),
        }
    }

    #[test]
    fn parse_boolean_literal() {
        let stmt = parse("SELECT TRUE").unwrap();
        let Statement::Query(query) = stmt else {
            panic!("expected Query");
        };
        match &select_body(&query).projection[0] {
            SelectItem::UnnamedExpr(Expr::Literal(ScalarValue::Boolean(true))) => {}
            other => panic!("expected true, got {other:?}"),
        }
    }

    #[test]
    fn parse_null_literal() {
        let stmt = parse("SELECT NULL").unwrap();
        let Statement::Query(query) = stmt else {
            panic!("expected Query");
        };
        match &select_body(&query).projection[0] {
            SelectItem::UnnamedExpr(Expr::Literal(ScalarValue::Null)) => {}
            other => panic!("expected NULL, got {other:?}"),
        }
    }

    // -- Data type conversion tests --

    #[test]
    fn parse_cast_integer_types() {
        for (sql_type, expected) in [
            ("TINYINT", DataType::Int8),
            ("SMALLINT", DataType::Int16),
            ("INTEGER", DataType::Int32),
            ("INT", DataType::Int32),
            ("BIGINT", DataType::Int64),
        ] {
            let stmt = parse(&format!("SELECT CAST(x AS {sql_type}) FROM t")).unwrap();
            let Statement::Query(query) = stmt else {
                panic!("expected Query for {sql_type}");
            };
            match &select_body(&query).projection[0] {
                SelectItem::UnnamedExpr(Expr::Cast { data_type, .. }) => {
                    assert_eq!(*data_type, expected, "failed for {sql_type}");
                }
                other => panic!("expected cast for {sql_type}, got {other:?}"),
            }
        }
    }

    #[test]
    fn parse_cast_float_types() {
        for (sql_type, expected) in [
            ("FLOAT", DataType::Float32),
            ("REAL", DataType::Float32),
            ("DOUBLE", DataType::Float64),
        ] {
            let stmt = parse(&format!("SELECT CAST(x AS {sql_type}) FROM t")).unwrap();
            let Statement::Query(query) = stmt else {
                panic!("expected Query for {sql_type}");
            };
            match &select_body(&query).projection[0] {
                SelectItem::UnnamedExpr(Expr::Cast { data_type, .. }) => {
                    assert_eq!(*data_type, expected, "failed for {sql_type}");
                }
                other => panic!("expected cast for {sql_type}, got {other:?}"),
            }
        }
    }

    #[test]
    fn parse_cast_string_types() {
        for sql_type in ["VARCHAR", "TEXT", "VARCHAR(255)"] {
            let stmt = parse(&format!("SELECT CAST(x AS {sql_type}) FROM t")).unwrap();
            let Statement::Query(query) = stmt else {
                panic!("expected Query for {sql_type}");
            };
            match &select_body(&query).projection[0] {
                SelectItem::UnnamedExpr(Expr::Cast { data_type, .. }) => {
                    assert_eq!(*data_type, DataType::Utf8, "failed for {sql_type}");
                }
                other => panic!("expected cast for {sql_type}, got {other:?}"),
            }
        }
    }

    #[test]
    fn parse_cast_decimal() {
        let stmt = parse("SELECT CAST(x AS DECIMAL(10, 2)) FROM t").unwrap();
        let Statement::Query(query) = stmt else {
            panic!("expected Query");
        };
        match &select_body(&query).projection[0] {
            SelectItem::UnnamedExpr(Expr::Cast { data_type, .. }) => {
                assert_eq!(
                    *data_type,
                    DataType::Decimal128 {
                        precision: 10,
                        scale: 2
                    }
                );
            }
            other => panic!("expected cast, got {other:?}"),
        }
    }

    #[test]
    fn parse_cast_boolean() {
        let stmt = parse("SELECT CAST(x AS BOOLEAN) FROM t").unwrap();
        let Statement::Query(query) = stmt else {
            panic!("expected Query");
        };
        match &select_body(&query).projection[0] {
            SelectItem::UnnamedExpr(Expr::Cast { data_type, .. }) => {
                assert_eq!(*data_type, DataType::Boolean);
            }
            other => panic!("expected cast, got {other:?}"),
        }
    }

    #[test]
    fn parse_cast_date() {
        let stmt = parse("SELECT CAST(x AS DATE) FROM t").unwrap();
        let Statement::Query(query) = stmt else {
            panic!("expected Query");
        };
        match &select_body(&query).projection[0] {
            SelectItem::UnnamedExpr(Expr::Cast { data_type, .. }) => {
                assert_eq!(*data_type, DataType::Date32);
            }
            other => panic!("expected cast, got {other:?}"),
        }
    }

    #[test]
    fn parse_cast_timestamp() {
        let stmt = parse("SELECT CAST(x AS TIMESTAMP) FROM t").unwrap();
        let Statement::Query(query) = stmt else {
            panic!("expected Query");
        };
        match &select_body(&query).projection[0] {
            SelectItem::UnnamedExpr(Expr::Cast { data_type, .. }) => {
                assert_eq!(
                    *data_type,
                    DataType::Timestamp {
                        unit: arneb_common::types::TimeUnit::Microsecond,
                        timezone: None
                    }
                );
            }
            other => panic!("expected cast, got {other:?}"),
        }
    }

    // -- Display tests --

    #[test]
    fn binary_op_display() {
        assert_eq!(BinaryOp::Plus.to_string(), "+");
        assert_eq!(BinaryOp::And.to_string(), "AND");
        assert_eq!(BinaryOp::NotEq.to_string(), "!=");
    }

    #[test]
    fn unary_op_display() {
        assert_eq!(UnaryOp::Not.to_string(), "NOT");
        assert_eq!(UnaryOp::Minus.to_string(), "-");
    }

    #[test]
    fn expr_display() {
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column(ColumnRef {
                name: "a".to_string(),
                table: None,
            })),
            op: BinaryOp::Plus,
            right: Box::new(Expr::Literal(ScalarValue::Int64(1))),
        };
        assert_eq!(expr.to_string(), "a + 1");
    }

    // -- SELECT with alias --

    #[test]
    fn parse_select_with_alias() {
        let stmt = parse("SELECT a AS col_a FROM t").unwrap();
        let Statement::Query(query) = stmt else {
            panic!("expected Query");
        };
        match &select_body(&query).projection[0] {
            SelectItem::ExprWithAlias { alias, .. } => {
                assert_eq!(alias, "col_a");
            }
            other => panic!("expected ExprWithAlias, got {other:?}"),
        }
    }

    // -- DISTINCT --

    #[test]
    fn parse_select_distinct() {
        let stmt = parse("SELECT DISTINCT a FROM t").unwrap();
        let Statement::Query(query) = stmt else {
            panic!("expected Query");
        };
        assert!(select_body(&query).distinct);
    }

    // -- Table with alias --

    #[test]
    fn parse_table_alias() {
        let stmt = parse("SELECT * FROM my_table AS t").unwrap();
        let Statement::Query(query) = stmt else {
            panic!("expected Query");
        };
        match &select_body(&query).from[0].relation {
            TableFactor::Table { alias, .. } => {
                assert_eq!(alias.as_deref(), Some("t"));
            }
            other => panic!("expected table, got {other:?}"),
        }
    }

    // -- Multi-part table name --

    #[test]
    fn parse_qualified_table_name() {
        let stmt = parse("SELECT * FROM catalog.schema.table_name").unwrap();
        let Statement::Query(query) = stmt else {
            panic!("expected Query");
        };
        match &select_body(&query).from[0].relation {
            TableFactor::Table { name, .. } => {
                assert_eq!(name.catalog, Some("catalog".to_string()));
                assert_eq!(name.schema, Some("schema".to_string()));
                assert_eq!(name.table, "table_name");
            }
            other => panic!("expected table, got {other:?}"),
        }
    }

    // -- LIKE --

    #[test]
    fn parse_like_expr() {
        let stmt = parse("SELECT * FROM t WHERE name LIKE '%foo%'").unwrap();
        let Statement::Query(query) = stmt else {
            panic!("expected Query");
        };
        match select_body(&query).selection.as_deref() {
            Some(Expr::BinaryOp {
                op: BinaryOp::Like, ..
            }) => {}
            other => panic!("expected LIKE, got {other:?}"),
        }
    }

    // -- NOT LIKE --

    #[test]
    fn parse_not_like_expr() {
        let stmt = parse("SELECT * FROM t WHERE name NOT LIKE '%foo%'").unwrap();
        let Statement::Query(query) = stmt else {
            panic!("expected Query");
        };
        match select_body(&query).selection.as_deref() {
            Some(Expr::BinaryOp {
                op: BinaryOp::NotLike,
                ..
            }) => {}
            other => panic!("expected NOT LIKE, got {other:?}"),
        }
    }

    // -- IS NOT NULL --

    #[test]
    fn parse_is_not_null() {
        let stmt = parse("SELECT * FROM t WHERE x IS NOT NULL").unwrap();
        let Statement::Query(query) = stmt else {
            panic!("expected Query");
        };
        assert!(matches!(
            select_body(&query).selection.as_deref(),
            Some(Expr::IsNotNull(_))
        ));
    }

    // -- CASE / COALESCE / NULLIF tests --

    #[test]
    fn parse_searched_case() {
        let stmt = parse("SELECT CASE WHEN x > 1 THEN 'a' WHEN x > 2 THEN 'b' ELSE 'c' END FROM t")
            .unwrap();
        let Statement::Query(query) = stmt else {
            panic!("expected Query");
        };
        match &select_body(&query).projection[0] {
            SelectItem::UnnamedExpr(Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            }) => {
                assert!(operand.is_none());
                assert_eq!(conditions.len(), 2);
                assert_eq!(results.len(), 2);
                assert!(else_result.is_some());
            }
            other => panic!("expected Case, got {other:?}"),
        }
    }

    #[test]
    fn parse_simple_case() {
        let stmt = parse("SELECT CASE x WHEN 1 THEN 'one' WHEN 2 THEN 'two' END FROM t").unwrap();
        let Statement::Query(query) = stmt else {
            panic!("expected Query");
        };
        match &select_body(&query).projection[0] {
            SelectItem::UnnamedExpr(Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            }) => {
                assert!(operand.is_some());
                assert_eq!(conditions.len(), 2);
                assert_eq!(results.len(), 2);
                assert!(else_result.is_none());
            }
            other => panic!("expected Case, got {other:?}"),
        }
    }

    #[test]
    fn parse_coalesce() {
        let stmt = parse("SELECT COALESCE(a, b, c) FROM t").unwrap();
        let Statement::Query(query) = stmt else {
            panic!("expected Query");
        };
        match &select_body(&query).projection[0] {
            SelectItem::UnnamedExpr(Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            }) => {
                assert!(operand.is_none());
                // COALESCE(a, b, c) → WHEN a IS NOT NULL THEN a WHEN b IS NOT NULL THEN b ELSE c
                assert_eq!(conditions.len(), 2);
                assert_eq!(results.len(), 2);
                assert!(matches!(conditions[0], Expr::IsNotNull(_)));
                assert!(matches!(conditions[1], Expr::IsNotNull(_)));
                assert!(else_result.is_some());
            }
            other => panic!("expected Case (desugared COALESCE), got {other:?}"),
        }
    }

    #[test]
    fn parse_nullif() {
        let stmt = parse("SELECT NULLIF(a, b) FROM t").unwrap();
        let Statement::Query(query) = stmt else {
            panic!("expected Query");
        };
        match &select_body(&query).projection[0] {
            SelectItem::UnnamedExpr(Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            }) => {
                assert!(operand.is_none());
                // NULLIF(a, b) → WHEN a = b THEN NULL ELSE a
                assert_eq!(conditions.len(), 1);
                assert!(matches!(
                    &conditions[0],
                    Expr::BinaryOp {
                        op: BinaryOp::Eq,
                        ..
                    }
                ));
                assert_eq!(results.len(), 1);
                assert!(matches!(&results[0], Expr::Literal(ScalarValue::Null)));
                assert!(else_result.is_some());
            }
            other => panic!("expected Case (desugared NULLIF), got {other:?}"),
        }
    }

    #[test]
    fn parse_case_display() {
        let expr = Expr::Case {
            operand: None,
            conditions: vec![Expr::BinaryOp {
                left: Box::new(Expr::Column(ColumnRef {
                    name: "x".to_string(),
                    table: None,
                })),
                op: BinaryOp::Gt,
                right: Box::new(Expr::Literal(ScalarValue::Int64(1))),
            }],
            results: vec![Expr::Literal(ScalarValue::Utf8("yes".to_string()))],
            else_result: Some(Box::new(Expr::Literal(ScalarValue::Utf8("no".to_string())))),
        };
        assert_eq!(expr.to_string(), "CASE WHEN x > 1 THEN 'yes' ELSE 'no' END");
    }

    // -- Subquery expression tests --

    #[test]
    fn parse_in_subquery() {
        let stmt = parse("SELECT * FROM t WHERE id IN (SELECT id FROM u)").unwrap();
        let Statement::Query(query) = stmt else {
            panic!("expected Query");
        };
        assert!(matches!(
            select_body(&query).selection.as_deref(),
            Some(Expr::InSubquery { negated: false, .. })
        ));
    }

    #[test]
    fn parse_not_in_subquery() {
        let stmt = parse("SELECT * FROM t WHERE id NOT IN (SELECT id FROM u)").unwrap();
        let Statement::Query(query) = stmt else {
            panic!("expected Query");
        };
        assert!(matches!(
            select_body(&query).selection.as_deref(),
            Some(Expr::InSubquery { negated: true, .. })
        ));
    }

    #[test]
    fn parse_exists() {
        let stmt =
            parse("SELECT * FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.id = t.id)").unwrap();
        let Statement::Query(query) = stmt else {
            panic!("expected Query");
        };
        assert!(matches!(
            select_body(&query).selection.as_deref(),
            Some(Expr::Exists { negated: false, .. })
        ));
    }

    #[test]
    fn parse_not_exists() {
        let stmt = parse("SELECT * FROM t WHERE NOT EXISTS (SELECT 1 FROM u)").unwrap();
        let Statement::Query(query) = stmt else {
            panic!("expected Query");
        };
        assert!(matches!(
            select_body(&query).selection.as_deref(),
            Some(Expr::Exists { negated: true, .. })
        ));
    }

    #[test]
    fn parse_scalar_subquery() {
        let stmt = parse("SELECT (SELECT MAX(x) FROM u) FROM t").unwrap();
        let Statement::Query(query) = stmt else {
            panic!("expected Query");
        };
        match &select_body(&query).projection[0] {
            SelectItem::UnnamedExpr(Expr::Subquery(_)) => {}
            other => panic!("expected Subquery, got {other:?}"),
        }
    }
}
