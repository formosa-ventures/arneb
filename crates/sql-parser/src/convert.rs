//! Conversion from `sqlparser-rs` AST to arneb AST.
//!
//! This module provides functions to convert `sqlparser::ast` types into
//! the arneb-specific AST types defined in [`crate::ast`]. Unsupported
//! SQL constructs are rejected with [`ParseError::UnsupportedFeature`].

use arneb_common::error::ParseError;
use arneb_common::types::{DataType, ScalarValue, TableReference, TimeUnit};
use sqlparser::ast as sp;

use crate::ast;

/// Convert a `sqlparser` [`sp::Statement`] into a arneb [`ast::Statement`].
pub(crate) fn convert_statement(stmt: sp::Statement) -> Result<ast::Statement, ParseError> {
    match stmt {
        sp::Statement::Query(query) => {
            let q = convert_query(*query)?;
            Ok(ast::Statement::Query(Box::new(q)))
        }
        sp::Statement::Explain { statement, .. } => {
            let inner = convert_statement(*statement)?;
            Ok(ast::Statement::Explain(Box::new(inner)))
        }
        sp::Statement::CreateTable(ct) => {
            let name = object_name_to_table_reference(&ct.name)?;
            let columns = ct
                .columns
                .into_iter()
                .map(|col| {
                    let dt = convert_data_type(col.data_type)?;
                    Ok(ast::ColumnDef {
                        name: col.name.value,
                        data_type: dt,
                        nullable: true,
                    })
                })
                .collect::<Result<Vec<_>, ParseError>>()?;

            // Check if it's a CTAS
            if let Some(query) = ct.query {
                let q = convert_query(*query)?;
                return Ok(ast::Statement::CreateTableAsSelect {
                    name,
                    query: Box::new(q),
                });
            }

            Ok(ast::Statement::CreateTable {
                name,
                columns,
                if_not_exists: ct.if_not_exists,
            })
        }
        sp::Statement::Drop {
            object_type,
            if_exists,
            names,
            ..
        } => {
            if names.len() != 1 {
                return Err(ParseError::UnsupportedFeature(
                    "DROP with multiple names".to_string(),
                ));
            }
            let name = object_name_to_table_reference(&names[0])?;
            match object_type {
                sp::ObjectType::Table => Ok(ast::Statement::DropTable { name, if_exists }),
                sp::ObjectType::View => Ok(ast::Statement::DropView { name, if_exists }),
                _ => Err(ParseError::UnsupportedFeature(format!(
                    "DROP {object_type}"
                ))),
            }
        }
        sp::Statement::Insert(insert) => {
            let table = match insert.table {
                sp::TableObject::TableName(name) => object_name_to_table_reference(&name)?,
                _ => {
                    return Err(ParseError::UnsupportedFeature(
                        "table function insert".to_string(),
                    ))
                }
            };
            let columns: Vec<String> = insert.columns.iter().map(|c| c.value.clone()).collect();
            let source = if let Some(src) = insert.source {
                match *src.body {
                    sp::SetExpr::Values(values) => {
                        let rows = values
                            .rows
                            .into_iter()
                            .map(|row| {
                                row.into_iter()
                                    .map(convert_expr)
                                    .collect::<Result<Vec<_>, _>>()
                            })
                            .collect::<Result<Vec<_>, _>>()?;
                        ast::InsertSource::Values(rows)
                    }
                    other => {
                        // Reconstruct a query for non-VALUES sources
                        let rebuilt = sp::Query {
                            body: Box::new(other),
                            with: src.with,
                            order_by: src.order_by,
                            limit_clause: src.limit_clause,
                            fetch: src.fetch,
                            locks: src.locks,
                            for_clause: src.for_clause,
                            settings: src.settings,
                            format_clause: src.format_clause,
                            pipe_operators: src.pipe_operators,
                        };
                        let q = convert_query(rebuilt)?;
                        ast::InsertSource::Query(Box::new(q))
                    }
                }
            } else {
                return Err(ParseError::InvalidSyntax(
                    "INSERT requires a source".to_string(),
                ));
            };
            Ok(ast::Statement::InsertInto {
                table,
                columns,
                source,
            })
        }
        sp::Statement::Delete(delete) => {
            let from_tables = match delete.from {
                sp::FromTable::WithFromKeyword(t) | sp::FromTable::WithoutKeyword(t) => t,
            };
            let table_ref = match from_tables.into_iter().next() {
                Some(twj) => match twj.relation {
                    sp::TableFactor::Table { name, .. } => object_name_to_table_reference(&name)?,
                    _ => {
                        return Err(ParseError::UnsupportedFeature(
                            "DELETE from non-table".to_string(),
                        ))
                    }
                },
                None => {
                    return Err(ParseError::InvalidSyntax(
                        "DELETE requires a table".to_string(),
                    ))
                }
            };
            let predicate = match delete.selection {
                Some(expr) => Some(Box::new(convert_expr(expr)?)),
                None => None,
            };
            Ok(ast::Statement::DeleteFrom {
                table: table_ref,
                predicate,
            })
        }
        sp::Statement::CreateView(cv) => {
            let view_name = object_name_to_table_reference(&cv.name)?;
            let q = convert_query(*cv.query)?;
            Ok(ast::Statement::CreateView {
                name: view_name,
                query: Box::new(q),
                or_replace: cv.or_replace,
            })
        }
        other => Err(ParseError::UnsupportedFeature(
            statement_name(&other).to_string(),
        )),
    }
}

/// Convert a `sqlparser` [`sp::Query`] into a arneb [`ast::Query`].
pub(crate) fn convert_query(query: sp::Query) -> Result<ast::Query, ParseError> {
    // Convert CTEs
    let ctes = if let Some(with) = query.with {
        with.cte_tables
            .into_iter()
            .map(|cte| {
                let name = cte.alias.name.value;
                let column_aliases: Vec<String> =
                    cte.alias.columns.iter().map(|c| c.to_string()).collect();
                let q = convert_query(*cte.query)?;
                Ok(ast::CTEDefinition {
                    name,
                    column_aliases,
                    query: Box::new(q),
                })
            })
            .collect::<Result<Vec<_>, ParseError>>()?
    } else {
        vec![]
    };

    // Convert body (may be a set operation)
    let body = convert_set_expr(*query.body)?;

    // Convert ORDER BY
    let order_by = match query.order_by {
        Some(ob) => convert_order_by_clause(ob)?,
        None => vec![],
    };

    // Convert LIMIT and OFFSET from limit_clause
    let (limit, offset) = match query.limit_clause {
        Some(sp::LimitClause::LimitOffset {
            limit: limit_expr,
            offset: offset_expr,
            ..
        }) => {
            let l = match limit_expr {
                Some(expr) => Some(Box::new(convert_expr(expr)?)),
                None => None,
            };
            let o = match offset_expr {
                Some(offset) => Some(Box::new(convert_expr(offset.value)?)),
                None => None,
            };
            (l, o)
        }
        _ => (None, None),
    };

    Ok(ast::Query {
        ctes,
        body,
        order_by,
        limit,
        offset,
    })
}

/// Convert a set expression (SELECT, UNION, INTERSECT, EXCEPT).
fn convert_set_expr(expr: sp::SetExpr) -> Result<ast::QueryBody, ParseError> {
    match expr {
        sp::SetExpr::Select(select) => {
            let body = convert_select(*select)?;
            Ok(ast::QueryBody::Select(body))
        }
        sp::SetExpr::SetOperation {
            op,
            set_quantifier,
            left,
            right,
        } => {
            let ast_op = match (op, set_quantifier) {
                (sp::SetOperator::Union, sp::SetQuantifier::All) => ast::SetOperator::UnionAll,
                (sp::SetOperator::Union, _) => ast::SetOperator::Union,
                (sp::SetOperator::Intersect, _) => ast::SetOperator::Intersect,
                (sp::SetOperator::Except, _) => ast::SetOperator::Except,
                (sp::SetOperator::Minus, _) => ast::SetOperator::Except,
            };
            let l = convert_set_expr(*left)?;
            let r = convert_set_expr(*right)?;
            Ok(ast::QueryBody::SetOperation {
                op: ast_op,
                left: Box::new(l),
                right: Box::new(r),
            })
        }
        other => Err(ParseError::UnsupportedFeature(format!(
            "query body: {other}"
        ))),
    }
}

/// Convert a `sqlparser` [`sp::Select`] into a arneb [`ast::SelectBody`].
fn convert_select(select: sp::Select) -> Result<ast::SelectBody, ParseError> {
    let distinct = match &select.distinct {
        Some(sp::Distinct::Distinct) => true,
        Some(sp::Distinct::On(_)) => {
            return Err(ParseError::UnsupportedFeature("DISTINCT ON".to_string()));
        }
        Some(sp::Distinct::All) => {
            return Err(ParseError::UnsupportedFeature("DISTINCT ALL".to_string()));
        }
        None => false,
    };

    let projection = select
        .projection
        .into_iter()
        .map(convert_select_item)
        .collect::<Result<Vec<_>, _>>()?;

    let from = select
        .from
        .into_iter()
        .map(convert_table_with_joins)
        .collect::<Result<Vec<_>, _>>()?;

    let selection = match select.selection {
        Some(expr) => Some(Box::new(convert_expr(expr)?)),
        None => None,
    };

    let group_by = match select.group_by {
        sp::GroupByExpr::Expressions(exprs, _) => exprs
            .into_iter()
            .map(convert_expr)
            .collect::<Result<Vec<_>, _>>()?,
        sp::GroupByExpr::All(_) => {
            return Err(ParseError::UnsupportedFeature("GROUP BY ALL".to_string()));
        }
    };

    let having = match select.having {
        Some(expr) => Some(Box::new(convert_expr(expr)?)),
        None => None,
    };

    Ok(ast::SelectBody {
        distinct,
        projection,
        from,
        selection,
        group_by,
        having,
    })
}

/// Convert a `sqlparser` [`sp::SelectItem`] into a arneb [`ast::SelectItem`].
fn convert_select_item(item: sp::SelectItem) -> Result<ast::SelectItem, ParseError> {
    match item {
        sp::SelectItem::UnnamedExpr(expr) => {
            let e = convert_expr(expr)?;
            Ok(ast::SelectItem::UnnamedExpr(e))
        }
        sp::SelectItem::ExprWithAlias { expr, alias } => {
            let e = convert_expr(expr)?;
            Ok(ast::SelectItem::ExprWithAlias {
                expr: e,
                alias: alias.value,
            })
        }
        sp::SelectItem::Wildcard(_) => Ok(ast::SelectItem::Wildcard),
        sp::SelectItem::QualifiedWildcard(kind, _) => {
            let table_ref = qualified_wildcard_to_table_reference(kind)?;
            Ok(ast::SelectItem::QualifiedWildcard(table_ref))
        }
    }
}

/// Convert a qualified wildcard kind to a table reference.
fn qualified_wildcard_to_table_reference(
    kind: sp::SelectItemQualifiedWildcardKind,
) -> Result<TableReference, ParseError> {
    match kind {
        sp::SelectItemQualifiedWildcardKind::ObjectName(name) => {
            object_name_to_table_reference(&name)
        }
        sp::SelectItemQualifiedWildcardKind::Expr(_) => Err(ParseError::UnsupportedFeature(
            "expression qualified wildcard".to_string(),
        )),
    }
}

/// Convert a `sqlparser` [`sp::Expr`] into a arneb [`ast::Expr`].
pub(crate) fn convert_expr(expr: sp::Expr) -> Result<ast::Expr, ParseError> {
    match expr {
        sp::Expr::Identifier(ident) => Ok(ast::Expr::Column(ast::ColumnRef {
            name: ident.value,
            table: None,
        })),
        sp::Expr::CompoundIdentifier(idents) => match idents.len() {
            2 => Ok(ast::Expr::Column(ast::ColumnRef {
                table: Some(idents[0].value.clone()),
                name: idents[1].value.clone(),
            })),
            1 => Ok(ast::Expr::Column(ast::ColumnRef {
                name: idents[0].value.clone(),
                table: None,
            })),
            _ => Err(ParseError::UnsupportedFeature(format!(
                "compound identifier with {} parts",
                idents.len()
            ))),
        },
        sp::Expr::Value(val_with_span) => {
            let scalar = convert_value(val_with_span.value)?;
            Ok(ast::Expr::Literal(scalar))
        }
        sp::Expr::BinaryOp { left, op, right } => {
            let l = convert_expr(*left)?;
            let r = convert_expr(*right)?;
            let bin_op = convert_binary_op(op)?;
            Ok(ast::Expr::BinaryOp {
                left: Box::new(l),
                op: bin_op,
                right: Box::new(r),
            })
        }
        sp::Expr::UnaryOp { op, expr } => {
            let e = convert_expr(*expr)?;
            let un_op = convert_unary_op(op)?;
            Ok(ast::Expr::UnaryOp {
                op: un_op,
                expr: Box::new(e),
            })
        }
        sp::Expr::Like {
            negated,
            expr,
            pattern,
            ..
        } => {
            let l = convert_expr(*expr)?;
            let r = convert_expr(*pattern)?;
            let op = if negated {
                ast::BinaryOp::NotLike
            } else {
                ast::BinaryOp::Like
            };
            Ok(ast::Expr::BinaryOp {
                left: Box::new(l),
                op,
                right: Box::new(r),
            })
        }
        sp::Expr::IsNull(expr) => {
            let e = convert_expr(*expr)?;
            Ok(ast::Expr::IsNull(Box::new(e)))
        }
        sp::Expr::IsNotNull(expr) => {
            let e = convert_expr(*expr)?;
            Ok(ast::Expr::IsNotNull(Box::new(e)))
        }
        sp::Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let e = convert_expr(*expr)?;
            let lo = convert_expr(*low)?;
            let hi = convert_expr(*high)?;
            Ok(ast::Expr::Between {
                expr: Box::new(e),
                negated,
                low: Box::new(lo),
                high: Box::new(hi),
            })
        }
        sp::Expr::InList {
            expr,
            list,
            negated,
        } => {
            let e = convert_expr(*expr)?;
            let items = list
                .into_iter()
                .map(convert_expr)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(ast::Expr::InList {
                expr: Box::new(e),
                list: items,
                negated,
            })
        }
        sp::Expr::Cast {
            expr, data_type, ..
        } => {
            let e = convert_expr(*expr)?;
            let dt = convert_data_type(data_type)?;
            Ok(ast::Expr::Cast {
                expr: Box::new(e),
                data_type: dt,
            })
        }
        sp::Expr::Nested(expr) => {
            let e = convert_expr(*expr)?;
            Ok(ast::Expr::Nested(Box::new(e)))
        }
        sp::Expr::Subquery(query) => {
            let q = convert_query(*query)?;
            Ok(ast::Expr::Subquery(Box::new(q)))
        }
        sp::Expr::Function(func) => convert_function(func),
        sp::Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => {
            let e = convert_expr(*expr)?;
            let q = convert_query(*subquery)?;
            Ok(ast::Expr::InSubquery {
                expr: Box::new(e),
                subquery: Box::new(q),
                negated,
            })
        }
        sp::Expr::Exists { subquery, negated } => {
            let q = convert_query(*subquery)?;
            Ok(ast::Expr::Exists {
                subquery: Box::new(q),
                negated,
            })
        }
        sp::Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            let op = match operand {
                Some(expr) => Some(Box::new(convert_expr(*expr)?)),
                None => None,
            };
            let mut conds = Vec::with_capacity(conditions.len());
            let mut results = Vec::with_capacity(conditions.len());
            for cw in conditions {
                conds.push(convert_expr(cw.condition)?);
                results.push(convert_expr(cw.result)?);
            }
            let el = match else_result {
                Some(expr) => Some(Box::new(convert_expr(*expr)?)),
                None => None,
            };
            Ok(ast::Expr::Case {
                operand: op,
                conditions: conds,
                results,
                else_result: el,
            })
        }
        sp::Expr::IsFalse(expr) => {
            let e = convert_expr(*expr)?;
            Ok(ast::Expr::UnaryOp {
                op: ast::UnaryOp::Not,
                expr: Box::new(e),
            })
        }
        sp::Expr::IsTrue(expr) => convert_expr(*expr),
        other => Err(ParseError::UnsupportedFeature(format!(
            "expression: {other}"
        ))),
    }
}

/// Convert a `sqlparser` [`sp::Function`] into a arneb [`ast::Expr::Function`].
fn convert_function(func: sp::Function) -> Result<ast::Expr, ParseError> {
    let name = func.name.to_string();
    let name_upper = name.to_uppercase();

    // Desugar COALESCE(a, b, ...) → CASE WHEN a IS NOT NULL THEN a WHEN b IS NOT NULL THEN b ... ELSE last END
    if name_upper == "COALESCE" {
        let raw_args = match func.args {
            sp::FunctionArguments::List(arg_list) => arg_list
                .args
                .into_iter()
                .map(|a| match a {
                    sp::FunctionArg::Unnamed(sp::FunctionArgExpr::Expr(e)) => convert_expr(e),
                    _ => Err(ParseError::UnsupportedFeature(
                        "non-expression COALESCE argument".to_string(),
                    )),
                })
                .collect::<Result<Vec<_>, _>>()?,
            _ => {
                return Err(ParseError::InvalidSyntax(
                    "COALESCE requires arguments".to_string(),
                ))
            }
        };
        if raw_args.is_empty() {
            return Err(ParseError::InvalidSyntax(
                "COALESCE requires at least one argument".to_string(),
            ));
        }
        if raw_args.len() == 1 {
            return Ok(raw_args.into_iter().next().unwrap());
        }
        let last = raw_args.len() - 1;
        let mut conditions = Vec::new();
        let mut results = Vec::new();
        for arg in &raw_args[..last] {
            conditions.push(ast::Expr::IsNotNull(Box::new(arg.clone())));
            results.push(arg.clone());
        }
        return Ok(ast::Expr::Case {
            operand: None,
            conditions,
            results,
            else_result: Some(Box::new(raw_args.into_iter().last().unwrap())),
        });
    }

    // Desugar NULLIF(a, b) → CASE WHEN a = b THEN NULL ELSE a END
    if name_upper == "NULLIF" {
        let raw_args = match func.args {
            sp::FunctionArguments::List(arg_list) => arg_list
                .args
                .into_iter()
                .map(|a| match a {
                    sp::FunctionArg::Unnamed(sp::FunctionArgExpr::Expr(e)) => convert_expr(e),
                    _ => Err(ParseError::UnsupportedFeature(
                        "non-expression NULLIF argument".to_string(),
                    )),
                })
                .collect::<Result<Vec<_>, _>>()?,
            _ => {
                return Err(ParseError::InvalidSyntax(
                    "NULLIF requires arguments".to_string(),
                ))
            }
        };
        if raw_args.len() != 2 {
            return Err(ParseError::InvalidSyntax(
                "NULLIF requires exactly two arguments".to_string(),
            ));
        }
        let mut args_iter = raw_args.into_iter();
        let a = args_iter.next().unwrap();
        let b = args_iter.next().unwrap();
        return Ok(ast::Expr::Case {
            operand: None,
            conditions: vec![ast::Expr::BinaryOp {
                left: Box::new(a.clone()),
                op: ast::BinaryOp::Eq,
                right: Box::new(b),
            }],
            results: vec![ast::Expr::Literal(ScalarValue::Null)],
            else_result: Some(Box::new(a)),
        });
    }

    let (args, is_distinct) = match func.args {
        sp::FunctionArguments::None => (vec![], false),
        sp::FunctionArguments::Subquery(_) => {
            return Err(ParseError::UnsupportedFeature(
                "subquery as function argument".to_string(),
            ));
        }
        sp::FunctionArguments::List(arg_list) => {
            let distinct_flag =
                arg_list.duplicate_treatment == Some(sp::DuplicateTreatment::Distinct);
            let args = arg_list
                .args
                .into_iter()
                .map(convert_function_arg)
                .collect::<Result<Vec<_>, _>>()?;
            (args, distinct_flag)
        }
    };

    // Check for window function (OVER clause)
    if let Some(over) = func.over {
        match over {
            sp::WindowType::WindowSpec(spec) => {
                let partition_by = spec
                    .partition_by
                    .into_iter()
                    .map(convert_expr)
                    .collect::<Result<Vec<_>, _>>()?;
                let order_by = spec
                    .order_by
                    .into_iter()
                    .map(convert_order_by_expr)
                    .collect::<Result<Vec<_>, _>>()?;
                let plain_args = args
                    .into_iter()
                    .filter_map(|a| match a {
                        ast::FunctionArg::Unnamed(e) => Some(e),
                        ast::FunctionArg::Wildcard => None,
                    })
                    .collect();
                return Ok(ast::Expr::WindowFunction {
                    name,
                    args: plain_args,
                    partition_by,
                    order_by,
                });
            }
            sp::WindowType::NamedWindow(_) => {
                return Err(ParseError::UnsupportedFeature(
                    "named window references".to_string(),
                ));
            }
        }
    }

    Ok(ast::Expr::Function {
        name,
        args,
        distinct: is_distinct,
    })
}

/// Convert a `sqlparser` [`sp::FunctionArg`] into a arneb [`ast::FunctionArg`].
fn convert_function_arg(arg: sp::FunctionArg) -> Result<ast::FunctionArg, ParseError> {
    match arg {
        sp::FunctionArg::Unnamed(arg_expr) => match arg_expr {
            sp::FunctionArgExpr::Expr(expr) => {
                let e = convert_expr(expr)?;
                Ok(ast::FunctionArg::Unnamed(e))
            }
            sp::FunctionArgExpr::Wildcard => Ok(ast::FunctionArg::Wildcard),
            sp::FunctionArgExpr::QualifiedWildcard(_) => Err(ParseError::UnsupportedFeature(
                "qualified wildcard in function argument".to_string(),
            )),
        },
        sp::FunctionArg::Named { .. } | sp::FunctionArg::ExprNamed { .. } => Err(
            ParseError::UnsupportedFeature("named function arguments".to_string()),
        ),
    }
}

/// Convert a `sqlparser` [`sp::TableWithJoins`] into a arneb [`ast::TableWithJoins`].
fn convert_table_with_joins(twj: sp::TableWithJoins) -> Result<ast::TableWithJoins, ParseError> {
    let relation = convert_table_factor(twj.relation)?;
    let joins = twj
        .joins
        .into_iter()
        .map(convert_join)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ast::TableWithJoins { relation, joins })
}

/// Convert a `sqlparser` [`sp::TableFactor`] into a arneb [`ast::TableFactor`].
fn convert_table_factor(factor: sp::TableFactor) -> Result<ast::TableFactor, ParseError> {
    match factor {
        sp::TableFactor::Table { name, alias, .. } => {
            let table_ref = object_name_to_table_reference(&name)?;
            let alias_name = alias.map(|a| a.name.value);
            Ok(ast::TableFactor::Table {
                name: table_ref,
                alias: alias_name,
            })
        }
        sp::TableFactor::Derived {
            subquery, alias, ..
        } => {
            let q = convert_query(*subquery)?;
            let alias_name = alias
                .map(|a| a.name.value)
                .unwrap_or_else(|| "subquery".to_string());
            Ok(ast::TableFactor::Subquery {
                query: Box::new(q),
                alias: alias_name,
            })
        }
        sp::TableFactor::NestedJoin {
            table_with_joins,
            alias,
        } => {
            let inner = convert_table_with_joins(*table_with_joins)?;
            if alias.is_some() {
                return Err(ParseError::UnsupportedFeature(
                    "aliased nested join".to_string(),
                ));
            }
            Ok(inner.relation)
        }
        _ => Err(ParseError::UnsupportedFeature(
            "unsupported table factor".to_string(),
        )),
    }
}

/// Convert a `sqlparser` [`sp::Join`] into a arneb [`ast::Join`].
fn convert_join(join: sp::Join) -> Result<ast::Join, ParseError> {
    let relation = convert_table_factor(join.relation)?;
    let (join_type, condition) = convert_join_operator(join.join_operator)?;
    Ok(ast::Join {
        relation,
        join_type,
        condition,
    })
}

/// Convert a `sqlparser` [`sp::JoinOperator`] into join type and condition.
fn convert_join_operator(
    op: sp::JoinOperator,
) -> Result<(ast::JoinType, ast::JoinCondition), ParseError> {
    match op {
        sp::JoinOperator::Join(constraint) | sp::JoinOperator::Inner(constraint) => {
            let cond = convert_join_constraint(constraint)?;
            Ok((ast::JoinType::Inner, cond))
        }
        sp::JoinOperator::Left(constraint) | sp::JoinOperator::LeftOuter(constraint) => {
            let cond = convert_join_constraint(constraint)?;
            Ok((ast::JoinType::Left, cond))
        }
        sp::JoinOperator::Right(constraint) | sp::JoinOperator::RightOuter(constraint) => {
            let cond = convert_join_constraint(constraint)?;
            Ok((ast::JoinType::Right, cond))
        }
        sp::JoinOperator::FullOuter(constraint) => {
            let cond = convert_join_constraint(constraint)?;
            Ok((ast::JoinType::Full, cond))
        }
        sp::JoinOperator::CrossJoin(_) => Ok((ast::JoinType::Cross, ast::JoinCondition::None)),
        _ => Err(ParseError::UnsupportedFeature(
            "unsupported join type".to_string(),
        )),
    }
}

/// Convert a `sqlparser` [`sp::JoinConstraint`] into a arneb [`ast::JoinCondition`].
fn convert_join_constraint(
    constraint: sp::JoinConstraint,
) -> Result<ast::JoinCondition, ParseError> {
    match constraint {
        sp::JoinConstraint::On(expr) => {
            let e = convert_expr(expr)?;
            Ok(ast::JoinCondition::On(e))
        }
        sp::JoinConstraint::Using(idents) => {
            let cols = idents.into_iter().map(|id| id.to_string()).collect();
            Ok(ast::JoinCondition::Using(cols))
        }
        sp::JoinConstraint::Natural => {
            Err(ParseError::UnsupportedFeature("NATURAL JOIN".to_string()))
        }
        sp::JoinConstraint::None => Ok(ast::JoinCondition::None),
    }
}

/// Convert the ORDER BY clause of a `sqlparser` [`sp::Query`].
fn convert_order_by_clause(order_by: sp::OrderBy) -> Result<Vec<ast::OrderByExpr>, ParseError> {
    match order_by.kind {
        sp::OrderByKind::All(_) => Err(ParseError::UnsupportedFeature("ORDER BY ALL".to_string())),
        sp::OrderByKind::Expressions(exprs) => {
            exprs.into_iter().map(convert_order_by_expr).collect()
        }
    }
}

/// Convert a `sqlparser` [`sp::OrderByExpr`] into a arneb [`ast::OrderByExpr`].
fn convert_order_by_expr(ob: sp::OrderByExpr) -> Result<ast::OrderByExpr, ParseError> {
    let expr = convert_expr(ob.expr)?;
    let asc = ob.options.asc;
    let nulls_first = ob.options.nulls_first;
    Ok(ast::OrderByExpr {
        expr,
        asc,
        nulls_first,
    })
}

/// Convert a `sqlparser` [`sp::Value`] into a arneb [`ScalarValue`].
pub(crate) fn convert_value(value: sp::Value) -> Result<ScalarValue, ParseError> {
    match value {
        sp::Value::Number(s, _) => {
            if s.contains('.') {
                let f: f64 = s.parse().map_err(|_| {
                    ParseError::InvalidSyntax(format!("invalid float literal: {s}"))
                })?;
                Ok(ScalarValue::Float64(f))
            } else {
                let i: i64 = s.parse().map_err(|_| {
                    ParseError::InvalidSyntax(format!("invalid integer literal: {s}"))
                })?;
                Ok(ScalarValue::Int64(i))
            }
        }
        sp::Value::SingleQuotedString(s) => Ok(ScalarValue::Utf8(s)),
        sp::Value::DoubleQuotedString(s) => Ok(ScalarValue::Utf8(s)),
        sp::Value::Boolean(b) => Ok(ScalarValue::Boolean(b)),
        sp::Value::Null => Ok(ScalarValue::Null),
        other => Err(ParseError::UnsupportedFeature(format!(
            "literal value: {other}"
        ))),
    }
}

/// Convert a `sqlparser` [`sp::DataType`] into a arneb [`DataType`].
pub(crate) fn convert_data_type(dt: sp::DataType) -> Result<DataType, ParseError> {
    match dt {
        sp::DataType::Boolean => Ok(DataType::Boolean),
        sp::DataType::TinyInt(_) => Ok(DataType::Int8),
        sp::DataType::SmallInt(_) => Ok(DataType::Int16),
        sp::DataType::Int(_) | sp::DataType::Integer(_) => Ok(DataType::Int32),
        sp::DataType::BigInt(_) => Ok(DataType::Int64),
        sp::DataType::Float(_) | sp::DataType::Real => Ok(DataType::Float32),
        sp::DataType::Double(_) | sp::DataType::DoublePrecision => Ok(DataType::Float64),
        sp::DataType::Varchar(_) | sp::DataType::Text | sp::DataType::String(_) => {
            Ok(DataType::Utf8)
        }
        sp::DataType::Char(_) | sp::DataType::CharacterVarying(_) => Ok(DataType::Utf8),
        sp::DataType::Decimal(info) | sp::DataType::Dec(info) | sp::DataType::Numeric(info) => {
            match info {
                sp::ExactNumberInfo::PrecisionAndScale(p, s) => Ok(DataType::Decimal128 {
                    precision: p as u8,
                    scale: s as i8,
                }),
                sp::ExactNumberInfo::Precision(p) => Ok(DataType::Decimal128 {
                    precision: p as u8,
                    scale: 0,
                }),
                sp::ExactNumberInfo::None => Ok(DataType::Decimal128 {
                    precision: 38,
                    scale: 0,
                }),
            }
        }
        sp::DataType::Date => Ok(DataType::Date32),
        sp::DataType::Timestamp(_, tz_info) => {
            let timezone = match tz_info {
                sp::TimezoneInfo::WithTimeZone => Some("UTC".to_string()),
                _ => None,
            };
            Ok(DataType::Timestamp {
                unit: TimeUnit::Microsecond,
                timezone,
            })
        }
        sp::DataType::Binary(_) | sp::DataType::Varbinary(_) | sp::DataType::Blob(_) => {
            Ok(DataType::Binary)
        }
        other => Err(ParseError::UnsupportedFeature(format!(
            "data type: {other}"
        ))),
    }
}

/// Convert a `sqlparser` [`sp::BinaryOperator`] into a arneb [`ast::BinaryOp`].
fn convert_binary_op(op: sp::BinaryOperator) -> Result<ast::BinaryOp, ParseError> {
    match op {
        sp::BinaryOperator::Plus => Ok(ast::BinaryOp::Plus),
        sp::BinaryOperator::Minus => Ok(ast::BinaryOp::Minus),
        sp::BinaryOperator::Multiply => Ok(ast::BinaryOp::Multiply),
        sp::BinaryOperator::Divide => Ok(ast::BinaryOp::Divide),
        sp::BinaryOperator::Modulo => Ok(ast::BinaryOp::Modulo),
        sp::BinaryOperator::Eq => Ok(ast::BinaryOp::Eq),
        sp::BinaryOperator::NotEq => Ok(ast::BinaryOp::NotEq),
        sp::BinaryOperator::Lt => Ok(ast::BinaryOp::Lt),
        sp::BinaryOperator::LtEq => Ok(ast::BinaryOp::LtEq),
        sp::BinaryOperator::Gt => Ok(ast::BinaryOp::Gt),
        sp::BinaryOperator::GtEq => Ok(ast::BinaryOp::GtEq),
        sp::BinaryOperator::And => Ok(ast::BinaryOp::And),
        sp::BinaryOperator::Or => Ok(ast::BinaryOp::Or),
        other => Err(ParseError::UnsupportedFeature(format!(
            "binary operator: {other}"
        ))),
    }
}

/// Convert a `sqlparser` [`sp::UnaryOperator`] into a arneb [`ast::UnaryOp`].
fn convert_unary_op(op: sp::UnaryOperator) -> Result<ast::UnaryOp, ParseError> {
    match op {
        sp::UnaryOperator::Not => Ok(ast::UnaryOp::Not),
        sp::UnaryOperator::Minus => Ok(ast::UnaryOp::Minus),
        sp::UnaryOperator::Plus => Ok(ast::UnaryOp::Plus),
        other => Err(ParseError::UnsupportedFeature(format!(
            "unary operator: {other}"
        ))),
    }
}

/// Convert a `sqlparser` [`sp::ObjectName`] into a arneb [`TableReference`].
fn object_name_to_table_reference(name: &sp::ObjectName) -> Result<TableReference, ParseError> {
    // Use Ident.value to get unquoted identifier names (not .to_string() which preserves quotes)
    let parts: Vec<String> = name
        .0
        .iter()
        .map(|p| match p.as_ident() {
            Some(ident) => ident.value.clone(),
            None => p.to_string(),
        })
        .collect();
    match parts.len() {
        1 => Ok(TableReference {
            catalog: None,
            schema: None,
            table: parts.into_iter().next().unwrap(),
        }),
        2 => {
            let mut it = parts.into_iter();
            let schema = it.next().unwrap();
            let table = it.next().unwrap();
            Ok(TableReference {
                catalog: None,
                schema: Some(schema),
                table,
            })
        }
        3 => {
            let mut it = parts.into_iter();
            let catalog = it.next().unwrap();
            let schema = it.next().unwrap();
            let table = it.next().unwrap();
            Ok(TableReference {
                catalog: Some(catalog),
                schema: Some(schema),
                table,
            })
        }
        n => Err(ParseError::UnsupportedFeature(format!(
            "object name with {n} parts"
        ))),
    }
}

/// Return a human-readable name for a `sqlparser` statement variant.
fn statement_name(stmt: &sp::Statement) -> &'static str {
    match stmt {
        sp::Statement::Query(_) => "SELECT",
        sp::Statement::Insert(_) => "INSERT",
        sp::Statement::Update { .. } => "UPDATE",
        sp::Statement::Delete(_) => "DELETE",
        sp::Statement::CreateTable(_) => "CREATE TABLE",
        sp::Statement::CreateView { .. } => "CREATE VIEW",
        sp::Statement::CreateIndex(_) => "CREATE INDEX",
        sp::Statement::AlterTable { .. } => "ALTER TABLE",
        sp::Statement::Drop { .. } => "DROP",
        _ => "unsupported statement",
    }
}
